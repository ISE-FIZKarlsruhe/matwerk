from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
import requests
from requests.auth import HTTPDigestAuth
from sqlalchemy import create_engine, text

from airflow.sdk import dag, task, Variable

log = logging.getLogger(__name__)

GRAPH_PREFIX = "https://nfdi.fiz-karlsruhe.de/matwerk"

# Core class IRIs used in your queries
NFDI_DATASET = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009"
NFDI_PUBLICATION = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000190"
NFDI_EVENT = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000018"
NFDI_ORG = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000003"
NFDI_PERSON = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000004"

DATASET_TYPES = [
    "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009",  # Dataset
    "http://purls.helmholtz-metadaten.de/mwo/MWO_0001058",
    "http://purls.helmholtz-metadaten.de/mwo/MWO_0001056",
    "http://purls.helmholtz-metadaten.de/mwo/MWO_0001057",
]


# ---------------------------
# Helpers
# ---------------------------

def _get_var(name: str) -> str:
    v = Variable.get(name)
    if v is None or str(v).strip() == "":
        raise ValueError(f"Airflow Variable '{name}' is missing/empty")
    return str(v)

def sparql_json(query: str) -> dict:
    vsparql = _get_var("matwerk-virtuoso_sparql")
    vuser = _get_var("matwerk-virtuoso_user")
    vpass = _get_var("matwerk-virtuoso_pass")
    auth = HTTPDigestAuth(vuser, vpass)

    log.info("SPARQL endpoint: %s", vsparql)
    log.info("SPARQL query (first 300 chars): %s", query[:300].replace("\n", " "))

    r = requests.get(
        vsparql,
        params={"query": query, "format": "application/sparql-results+json"},
        auth=auth,
        timeout=(10, 600),
    )
    log.info("SPARQL HTTP status=%s", r.status_code)
    if r.status_code != 200:
        log.error("SPARQL error body (first 1200 chars): %s", r.text[:1200])
    r.raise_for_status()
    return r.json()

def _bindings(js: dict) -> list[dict]:
    return js.get("results", {}).get("bindings", [])

def _bval(b: dict, key: str) -> str | None:
    v = b.get(key)
    return v.get("value") if isinstance(v, dict) else None

def pg_engine():
    dsn = _get_var("kg_metrics_pg_dsn")
    return create_engine(dsn, pool_pre_ping=True)


# ---------------------------
# DAG
# ---------------------------

@dag(
    dag_id="dashboard",
    schedule="0 0 * * *",
    catchup=False,
)
def dashboard():

    @task
    def preflight():
        for n in ["matwerk-virtuoso_sparql", "matwerk-virtuoso_user", "matwerk-virtuoso_pass", "kg_metrics_pg_dsn"]:
            v = Variable.get(n)
            log.info("Variable %s present=%s", n, v is not None)

        with pg_engine().connect() as cx:
            ok = cx.execute(text("select 1")).scalar()
            log.info("Postgres OK: select 1 => %s", ok)

        js = sparql_json("SELECT (1 AS ?ok) WHERE {}")
        log.info("SPARQL OK: bindings=%s", _bindings(js))

    @task
    def ensure_tables():
        """
        Create metric tables up-front so views never fail if a metric produces 0 rows.
        """
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS public.kg_graph_stats (
              ts_utc timestamptz NOT NULL,
              graph text NOT NULL,
              triples bigint NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.kg_graph_subject_counts (
              ts_utc timestamptz NOT NULL,
              graph text NOT NULL,
              subjects bigint NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.kg_graph_class_counts (
              ts_utc timestamptz NOT NULL,
              graph text NOT NULL,
              class_iri text,
              class_label text,
              instances bigint NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.kg_graph_property_counts (
              ts_utc timestamptz NOT NULL,
              graph text NOT NULL,
              property_iri text,
              usage_count bigint NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.kg_sankey_class_property (
              ts_utc timestamptz NOT NULL,
              graph text NOT NULL,
              source_iri text,
              source_label text,
              target_iri text,
              target_label text,
              value bigint NOT NULL
            );
            """,
            # --- new
            """
            CREATE TABLE IF NOT EXISTS public.kg_entity_type_counts (
              ts_utc timestamptz NOT NULL,
              graph text NOT NULL,
              concept_iri text,
              concept_label text,
              count bigint NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.kg_dataset_type_counts (
              ts_utc timestamptz NOT NULL,
              graph text NOT NULL,
              dataset_type_iri text,
              dataset_type_label text,
              count bigint NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.kg_content_counts (
              ts_utc timestamptz NOT NULL,
              graph text NOT NULL,
              datasets bigint NOT NULL,
              publications bigint NOT NULL,
              events bigint NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.kg_datasets (
              ts_utc timestamptz NOT NULL,
              graph text NOT NULL,
              dataset_iri text,
              title text,
              creator text,
              creator_affiliation text,
              link text
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.kg_org_city_counts (
              ts_utc timestamptz NOT NULL,
              graph text NOT NULL,
              city_iri text,
              city_label text,
              org_count bigint NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.kg_top_org_by_people (
              ts_utc timestamptz NOT NULL,
              graph text NOT NULL,
              org_iri text,
              org_label text,
              people_count bigint NOT NULL
            );
            """,
        ]

        eng = pg_engine()
        with eng.begin() as cx:
            for stmt in ddl:
                cx.execute(text(stmt))
        log.info("Ensured metric tables exist")

    @task
    def list_mse_graphs(limit: int = 2000) -> list[str]:
        q = f"""
        SELECT DISTINCT ?g
        WHERE {{
          GRAPH ?g {{ ?s ?p ?o }}
          FILTER(STRSTARTS(STR(?g), "{GRAPH_PREFIX}"))
        }}
        ORDER BY ?g
        LIMIT {int(limit)}
        """
        js = sparql_json(q)
        graphs = [_bval(b, "g") for b in _bindings(js)]
        graphs = [g for g in graphs if g]
        log.info("Found %d MSE graphs (first 20): %s", len(graphs), graphs[:20])
        if not graphs:
            raise RuntimeError(f"No named graphs found with prefix {GRAPH_PREFIX}")
        return graphs

    @task
    def write_graph_triples_and_subjects(graphs: list[str]):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        rows_t, rows_s = [], []

        for i, g in enumerate(graphs, start=1):
            q = f"""
            SELECT (COUNT(*) AS ?triples) (COUNT(DISTINCT ?s) AS ?subjects)
            WHERE {{ GRAPH <{g}> {{ ?s ?p ?o }} }}
            """
            js = sparql_json(q)
            bs = _bindings(js)
            triples = int(_bval(bs[0], "triples") or "0") if bs else 0
            subjects = int(_bval(bs[0], "subjects") or "0") if bs else 0

            rows_t.append({"ts_utc": now, "graph": g, "triples": triples})
            rows_s.append({"ts_utc": now, "graph": g, "subjects": subjects})

            if i % 10 == 0:
                log.info("Processed %d/%d graphs for triples/subjects...", i, len(graphs))

        eng = pg_engine()
        pd.DataFrame(rows_t).to_sql("kg_graph_stats", eng, schema="public", if_exists="append", index=False)
        pd.DataFrame(rows_s).to_sql("kg_graph_subject_counts", eng, schema="public", if_exists="append", index=False)
        log.info("Wrote triples/subjects for %d graphs at ts_utc=%s", len(graphs), now.isoformat())

    @task
    def write_graph_class_counts(graphs: list[str], limit_per_graph: int = 200):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        rows = []

        for i, g in enumerate(graphs, start=1):
            q = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?class (SAMPLE(?label) AS ?label) (COUNT(?s) AS ?instances)
            WHERE {{
              GRAPH <{g}> {{ ?s a ?class . }}
              OPTIONAL {{
                ?class rdfs:label ?label .
                FILTER(lang(?label) = "" || langMatches(lang(?label), "en"))
              }}
            }}
            GROUP BY ?class
            ORDER BY DESC(?instances)
            LIMIT {int(limit_per_graph)}
            """
            js = sparql_json(q)
            for b in _bindings(js):
                rows.append({
                    "ts_utc": now,
                    "graph": g,
                    "class_iri": _bval(b, "class"),
                    "class_label": _bval(b, "label"),
                    "instances": int(_bval(b, "instances") or "0"),
                })

            if i % 5 == 0:
                log.info("Processed %d/%d graphs for class counts...", i, len(graphs))

        pd.DataFrame(rows).to_sql("kg_graph_class_counts", pg_engine(), schema="public", if_exists="append", index=False)
        log.info("Wrote graph-class counts rows=%d at ts_utc=%s", len(rows), now.isoformat())

    @task
    def write_graph_property_counts(graphs: list[str], limit_per_graph: int = 100):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        rows = []

        for i, g in enumerate(graphs, start=1):
            q = f"""
            SELECT ?p (COUNT(*) AS ?usageCount)
            WHERE {{ GRAPH <{g}> {{ ?s ?p ?o . }} }}
            GROUP BY ?p
            ORDER BY DESC(?usageCount)
            LIMIT {int(limit_per_graph)}
            """
            js = sparql_json(q)
            for b in _bindings(js):
                rows.append({
                    "ts_utc": now,
                    "graph": g,
                    "property_iri": _bval(b, "p"),
                    "usage_count": int(_bval(b, "usageCount") or "0"),
                })

            if i % 5 == 0:
                log.info("Processed %d/%d graphs for property counts...", i, len(graphs))

        pd.DataFrame(rows).to_sql("kg_graph_property_counts", pg_engine(), schema="public", if_exists="append", index=False)
        log.info("Wrote graph-property counts rows=%d at ts_utc=%s", len(rows), now.isoformat())

    @task
    def write_sankey_class_property_labeled(graphs: list[str], limit_per_graph: int = 300):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        rows = []

        for i, g in enumerate(graphs, start=1):
            q = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

            SELECT
              ?sClass
              (COALESCE(?sLbl, STR(?sClass)) AS ?sourceLabel)
              ?p
              (COALESCE(?pLbl, STR(?p)) AS ?targetLabel)
              ?c
            WHERE {{
              {{
                SELECT ?sClass ?sLbl ?p ?pLbl (COUNT(*) AS ?c)
                WHERE {{
                  GRAPH <{g}> {{
                    ?s a ?sClass .
                    ?s ?p ?o .
                    FILTER(?p != rdf:type)
                  }}
                  OPTIONAL {{
                    ?sClass rdfs:label ?sLbl .
                    FILTER(lang(?sLbl) = "" || langMatches(lang(?sLbl), "en"))
                  }}
                  OPTIONAL {{
                    ?p rdfs:label ?pLbl .
                    FILTER(lang(?pLbl) = "" || langMatches(lang(?pLbl), "en"))
                  }}
                }}
                GROUP BY ?sClass ?sLbl ?p ?pLbl
              }}
            }}
            ORDER BY DESC(?c)
            LIMIT {int(limit_per_graph)}
            """
            js = sparql_json(q)

            for b in _bindings(js):
                rows.append({
                    "ts_utc": now,
                    "graph": g,
                    "source_iri": _bval(b, "sClass"),
                    "source_label": _bval(b, "sourceLabel"),
                    "target_iri": _bval(b, "p"),
                    "target_label": _bval(b, "targetLabel"),
                    "value": int(_bval(b, "c") or "0"),
                })

            if i % 5 == 0:
                log.info("Processed %d/%d graphs for labeled sankey edges...", i, len(graphs))

        if rows:
            pd.DataFrame(rows).to_sql("kg_sankey_class_property", pg_engine(), schema="public", if_exists="append", index=False)
            log.info("Wrote kg_sankey_class_property rows=%d at ts_utc=%s", len(rows), now.isoformat())
        else:
            log.warning("No Sankey rows produced.")

    @task
    def write_entity_type_counts(graphs: list[str], limit_per_graph: int = 500):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        rows = []

        for i, g in enumerate(graphs, start=1):
            q = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?Concept (SAMPLE(?label) AS ?label) (COUNT(?entity) AS ?count)
            WHERE {{
              GRAPH <{g}> {{ ?entity a ?Concept . }}
              OPTIONAL {{
                ?Concept rdfs:label ?label .
                FILTER(lang(?label) = "" || langMatches(lang(?label), "en"))
              }}
            }}
            GROUP BY ?Concept
            ORDER BY DESC(?count)
            LIMIT {int(limit_per_graph)}
            """
            js = sparql_json(q)
            for b in _bindings(js):
                rows.append({
                    "ts_utc": now,
                    "graph": g,
                    "concept_iri": _bval(b, "Concept"),
                    "concept_label": _bval(b, "label"),
                    "count": int(_bval(b, "count") or "0"),
                })

            if i % 5 == 0:
                log.info("Processed %d/%d graphs for entity type counts...", i, len(graphs))

        pd.DataFrame(rows).to_sql("kg_entity_type_counts", pg_engine(), schema="public", if_exists="append", index=False)
        log.info("Wrote kg_entity_type_counts rows=%d at ts_utc=%s", len(rows), now.isoformat())

    @task
    def write_dataset_type_counts(graphs: list[str]):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        rows = []
        types_values = " ".join(f"<{t}>" for t in DATASET_TYPES)

        for i, g in enumerate(graphs, start=1):
            q = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?class (SAMPLE(?label) AS ?label) (COUNT(?dataset) AS ?count)
            WHERE {{
              GRAPH <{g}> {{
                VALUES ?class {{ {types_values} }}
                ?dataset a ?class .
              }}
              OPTIONAL {{
                ?class rdfs:label ?label .
                FILTER(lang(?label) = "" || langMatches(lang(?label), "en"))
              }}
            }}
            GROUP BY ?class
            ORDER BY DESC(?count)
            """
            js = sparql_json(q)
            for b in _bindings(js):
                rows.append({
                    "ts_utc": now,
                    "graph": g,
                    "dataset_type_iri": _bval(b, "class"),
                    "dataset_type_label": _bval(b, "label"),
                    "count": int(_bval(b, "count") or "0"),
                })

        pd.DataFrame(rows).to_sql("kg_dataset_type_counts", pg_engine(), schema="public", if_exists="append", index=False)
        log.info("Wrote kg_dataset_type_counts rows=%d at ts_utc=%s", len(rows), now.isoformat())

    @task
    def write_counts_datasets_events_publications(graphs: list[str]):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        rows = []

        for i, g in enumerate(graphs, start=1):
            q = f"""
            SELECT
              (COUNT(DISTINCT ?ds) AS ?datasets)
              (COUNT(DISTINCT ?pub) AS ?publications)
              (COUNT(DISTINCT ?ev) AS ?events)
            WHERE {{
              GRAPH <{g}> {{
                OPTIONAL {{ ?ds a <{NFDI_DATASET}> . }}
                OPTIONAL {{ ?pub a <{NFDI_PUBLICATION}> . }}
                OPTIONAL {{ ?ev a <{NFDI_EVENT}> . }}
              }}
            }}
            """
            js = sparql_json(q)
            bs = _bindings(js)
            datasets = int(_bval(bs[0], "datasets") or "0") if bs else 0
            publications = int(_bval(bs[0], "publications") or "0") if bs else 0
            events = int(_bval(bs[0], "events") or "0") if bs else 0

            rows.append({
                "ts_utc": now,
                "graph": g,
                "datasets": datasets,
                "publications": publications,
                "events": events,
            })

            if i % 10 == 0:
                log.info("Processed %d/%d graphs for ds/pub/event counts...", i, len(graphs))

        pd.DataFrame(rows).to_sql("kg_content_counts", pg_engine(), schema="public", if_exists="append", index=False)
        log.info("Wrote kg_content_counts rows=%d at ts_utc=%s", len(rows), now.isoformat())

    @task
    def write_datasets_list(graphs: list[str], limit_per_graph: int = 500):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        rows = []
        types_values = " ".join(f"<{t}>" for t in DATASET_TYPES)

        for i, g in enumerate(graphs, start=1):
            q = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT DISTINCT ?dataset ?title ?creatorLabel ?creatorAffiliationLabel ?link
            WHERE {{
              GRAPH <{g}> {{
                VALUES ?class {{ {types_values} }}
                ?dataset a ?class .

                OPTIONAL {{
                  ?dataset <http://purl.obolibrary.org/obo/IAO_0000235> ?titleNode .
                  ?titleNode a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019> .
                  ?titleNode rdfs:label ?title .
                }}

                OPTIONAL {{ ?dataset <http://purl.obolibrary.org/obo/BFO_0000178> ?creator .
                           ?creator rdfs:label ?creatorLabel . }}

                OPTIONAL {{ ?dataset <http://purl.obolibrary.org/obo/BFO_0000178> ?creatorAffiliation .
                           ?creatorAffiliation rdfs:label ?creatorAffiliationLabel . }}

                OPTIONAL {{
                  ?dataset <http://purl.obolibrary.org/obo/IAO_0000235> ?linkNode .
                  ?linkNode <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?link .
                }}
              }}
            }}
            LIMIT {int(limit_per_graph)}
            """
            js = sparql_json(q)
            for b in _bindings(js):
                rows.append({
                    "ts_utc": now,
                    "graph": g,
                    "dataset_iri": _bval(b, "dataset"),
                    "title": _bval(b, "title"),
                    "creator": _bval(b, "creatorLabel"),
                    "creator_affiliation": _bval(b, "creatorAffiliationLabel"),
                    "link": _bval(b, "link"),
                })

            if i % 5 == 0:
                log.info("Processed %d/%d graphs for datasets list...", i, len(graphs))

        pd.DataFrame(rows).to_sql("kg_datasets", pg_engine(), schema="public", if_exists="append", index=False)
        log.info("Wrote kg_datasets rows=%d at ts_utc=%s", len(rows), now.isoformat())

    @task
    def write_orgs_by_city(graphs: list[str], limit_per_graph: int = 200):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        rows = []

        for i, g in enumerate(graphs, start=1):
            q = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?c (SAMPLE(?cityLabel) AS ?cityLabel) (COUNT(DISTINCT ?org) AS ?orgCount)
            WHERE {{
              GRAPH <{g}> {{
                ?org a <{NFDI_ORG}> .
                ?org <http://purl.obolibrary.org/obo/BFO_0000171> ?c .
                ?c a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000106> .
                OPTIONAL {{ ?c rdfs:label ?cityLabel . FILTER(lang(?cityLabel) = "" || langMatches(lang(?cityLabel),"en")) }}
              }}
            }}
            GROUP BY ?c
            ORDER BY DESC(?orgCount)
            LIMIT {int(limit_per_graph)}
            """
            js = sparql_json(q)
            for b in _bindings(js):
                rows.append({
                    "ts_utc": now,
                    "graph": g,
                    "city_iri": _bval(b, "c"),
                    "city_label": _bval(b, "cityLabel"),
                    "org_count": int(_bval(b, "orgCount") or "0"),
                })

        pd.DataFrame(rows).to_sql("kg_org_city_counts", pg_engine(), schema="public", if_exists="append", index=False)
        log.info("Wrote kg_org_city_counts rows=%d at ts_utc=%s", len(rows), now.isoformat())

    @task
    def write_top_orgs_by_people_affiliation(graphs: list[str], limit_per_graph: int = 50):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        rows = []

        for i, g in enumerate(graphs, start=1):
            q = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?org (SAMPLE(?orgLabel) AS ?orgLabel) (COUNT(DISTINCT ?person) AS ?peopleCount)
            WHERE {{
              GRAPH <{g}> {{
                ?person a <{NFDI_PERSON}> .
                ?person <http://purl.obolibrary.org/obo/BFO_0000057> ?org .
                ?org a <{NFDI_ORG}> .
                OPTIONAL {{ ?org rdfs:label ?orgLabel . FILTER(lang(?orgLabel) = "" || langMatches(lang(?orgLabel),"en")) }}
              }}
            }}
            GROUP BY ?org
            ORDER BY DESC(?peopleCount)
            LIMIT {int(limit_per_graph)}
            """
            js = sparql_json(q)
            for b in _bindings(js):
                rows.append({
                    "ts_utc": now,
                    "graph": g,
                    "org_iri": _bval(b, "org"),
                    "org_label": _bval(b, "orgLabel"),
                    "people_count": int(_bval(b, "peopleCount") or "0"),
                })

        if rows:
            pd.DataFrame(rows).to_sql("kg_top_org_by_people", pg_engine(), schema="public", if_exists="append", index=False)
            log.info("Wrote kg_top_org_by_people rows=%d at ts_utc=%s", len(rows), now.isoformat())
        else:
            log.warning("No org-by-people rows produced (affiliation predicate may differ).")

    @task
    def ensure_latest_views():
        ddl = [
            """
            CREATE OR REPLACE VIEW public.kg_graph_stats_latest AS
            SELECT DISTINCT ON (graph)
              graph, ts_utc, triples
            FROM public.kg_graph_stats
            WHERE graph LIKE 'https://nfdi.fiz-karlsruhe.de/matwerk/msekg%'
            ORDER BY graph, ts_utc DESC;
            """,
            """
            CREATE OR REPLACE VIEW public.kg_graph_subject_counts_latest AS
            SELECT DISTINCT ON (graph)
              graph, ts_utc, subjects
            FROM public.kg_graph_subject_counts
            WHERE graph LIKE 'https://nfdi.fiz-karlsruhe.de/matwerk/msekg%'
            ORDER BY graph, ts_utc DESC;
            """,
            """
            CREATE OR REPLACE VIEW public.kg_graph_class_counts_latest AS
            SELECT DISTINCT ON (graph, class_iri)
              graph, class_iri, class_label, ts_utc, instances
            FROM public.kg_graph_class_counts
            WHERE graph LIKE 'https://nfdi.fiz-karlsruhe.de/matwerk/msekg%'
            ORDER BY graph, class_iri, ts_utc DESC;
            """,
            """
            CREATE OR REPLACE VIEW public.kg_graph_property_counts_latest AS
            SELECT DISTINCT ON (graph, property_iri)
              graph, property_iri, ts_utc, usage_count
            FROM public.kg_graph_property_counts
            WHERE graph LIKE 'https://nfdi.fiz-karlsruhe.de/matwerk/msekg%'
            ORDER BY graph, property_iri, ts_utc DESC;
            """,
            """
            CREATE OR REPLACE VIEW public.kg_sankey_class_property_latest AS
            SELECT DISTINCT ON (graph, source_iri, target_iri)
              graph,
              source_iri, source_label,
              target_iri, target_label,
              ts_utc,
              value
            FROM public.kg_sankey_class_property
            WHERE graph LIKE 'https://nfdi.fiz-karlsruhe.de/matwerk/msekg%'
            ORDER BY graph, source_iri, target_iri, ts_utc DESC;
            """,

            """
            CREATE OR REPLACE VIEW public.kg_entity_type_counts_latest AS
            SELECT DISTINCT ON (graph, concept_iri)
              graph, concept_iri, concept_label, ts_utc, count
            FROM public.kg_entity_type_counts
            WHERE graph LIKE 'https://nfdi.fiz-karlsruhe.de/matwerk/msekg%'
            ORDER BY graph, concept_iri, ts_utc DESC;
            """,
            """
            CREATE OR REPLACE VIEW public.kg_dataset_type_counts_latest AS
            SELECT DISTINCT ON (graph, dataset_type_iri)
              graph, dataset_type_iri, dataset_type_label, ts_utc, count
            FROM public.kg_dataset_type_counts
            WHERE graph LIKE 'https://nfdi.fiz-karlsruhe.de/matwerk/msekg%'
            ORDER BY graph, dataset_type_iri, ts_utc DESC;
            """,
            """
            CREATE OR REPLACE VIEW public.kg_content_counts_latest AS
            SELECT DISTINCT ON (graph)
              graph, ts_utc, datasets, publications, events
            FROM public.kg_content_counts
            WHERE graph LIKE 'https://nfdi.fiz-karlsruhe.de/matwerk/msekg%'
            ORDER BY graph, ts_utc DESC;
            """,
            """
            CREATE OR REPLACE VIEW public.kg_org_city_counts_latest AS
            SELECT DISTINCT ON (graph, city_iri)
              graph, city_iri, city_label, ts_utc, org_count
            FROM public.kg_org_city_counts
            WHERE graph LIKE 'https://nfdi.fiz-karlsruhe.de/matwerk/msekg%'
            ORDER BY graph, city_iri, ts_utc DESC;
            """,
            """
            CREATE OR REPLACE VIEW public.kg_top_org_by_people_latest AS
            SELECT DISTINCT ON (graph, org_iri)
              graph, org_iri, org_label, ts_utc, people_count
            FROM public.kg_top_org_by_people
            WHERE graph LIKE 'https://nfdi.fiz-karlsruhe.de/matwerk/msekg%'
            ORDER BY graph, org_iri, ts_utc DESC;
            """,
            """
            CREATE OR REPLACE VIEW public.kg_datasets_latest AS
            SELECT DISTINCT ON (graph, dataset_iri)
              graph, dataset_iri, title, creator, creator_affiliation, link, ts_utc
            FROM public.kg_datasets
            WHERE graph LIKE 'https://nfdi.fiz-karlsruhe.de/matwerk/msekg%'
            ORDER BY graph, dataset_iri, ts_utc DESC;
            """,
        ]

        eng = pg_engine()
        with eng.begin() as cx:
            for stmt in ddl:
                cx.execute(text(stmt))
        log.info("Ensured *_latest views exist")

    graphs = list_mse_graphs()
    preflight_task = preflight()
    tables_task = ensure_tables()

    preflight_task >> tables_task >> graphs

    t1 = write_graph_triples_and_subjects(graphs)
    t2 = write_graph_class_counts(graphs)
    t3 = write_graph_property_counts(graphs)
    t4 = write_sankey_class_property_labeled(graphs)

    t5 = write_entity_type_counts(graphs)
    t6 = write_dataset_type_counts(graphs)
    t7 = write_counts_datasets_events_publications(graphs)
    t8 = write_datasets_list(graphs)
    t9 = write_orgs_by_city(graphs)
    t10 = write_top_orgs_by_people_affiliation(graphs)

    [t1, t2, t3, t4, t5, t6, t7, t8, t9, t10] >> ensure_latest_views()


dashboard()
