#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import time
import tempfile
from pathlib import Path
import subprocess
from typing import Iterable, Set, List, Dict, Any

from rdflib import Graph, URIRef, RDF, Literal, Namespace, ConjunctiveGraph, BNode
from rdflib.namespace import RDFS, OWL, XSD

# ------------------ Config (env-overridable) ------------------
BASE_GRAPH_IRI = os.environ.get("BASE_GRAPH_IRI", "https://purls.helmholtz-metadaten.de/msekg/").rstrip("/") + "/"

ENDPOINT_CLASS = URIRef(os.environ.get("ENDPOINT_CLASS", "http://purls.helmholtz-metadaten.de/mwo/MWO_0001060"))
ENDPOINT_PREDICATE = URIRef(os.environ.get("ENDPOINT_PREDICATE", "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008"))  # xsd:anyURI endpoint URL
DATASET_TYPE = URIRef(os.environ.get("DATASET_TYPE", "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009"))             # Graph database (dataset)
IAO_0000235 = URIRef(os.environ.get("IAO_0000235", "http://purl.obolibrary.org/obo/IAO_0000235"))                        # denotes
HAS_GRAPH_PRED = URIRef(os.environ.get("HAS_GRAPH_PRED", "http://www.w3.org/2000/01/rdf-schema#seeAlso"))

MWO_OWL_PATH = os.environ.get("MWO_OWL_PATH", "ontology/mwo-full.owl")

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))

STATE_JSON   = os.environ.get("STATE_JSON", "data/sparql_endpoints/sparql_sources.json")
ALL_TTL      = os.environ.get("ALL_TTL", "data/all.ttl")
SUMMARY_JSON = os.environ.get("SUMMARY_JSON", "data/sparql_endpoints/sparql_sources_list.json")
STATS_TTL    = os.environ.get("STATS_TTL", "data/sparql_endpoints/dataset_stats.ttl")

# Per-graph output dir
NAMED_GRAPHS_DIR = os.environ.get("NAMED_GRAPHS_DIR", "data/sparql_endpoints/named_graphs/").rstrip("/") + "/"

# Namespace for stats/annotations
STAT = Namespace("https://purls.helmholtz-metadaten.de/msekg/stat/")

# Functionality keys → stable graph IRIs per endpoint
FUNC_CLASSES        = "classes"
FUNC_CLASS_HIER     = "classHierarchy"
FUNC_TBOX           = "tbox"

# ------------------ Helpers ------------------
def as_http_url_str(obj):
    if isinstance(obj, URIRef):
        u = str(obj)
        if u.startswith("http://") or u.startswith("https://"):
            return u
    if isinstance(obj, Literal):
        s = str(obj)
        if s.startswith("http://") or s.startswith("https://"):
            return s
    return None

def load_state(path) -> Dict[str, Any]:
    p = Path(path)
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {}

def save_state(path, data):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(path).with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(Path(path))

def ensure_parent(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def safe_overwrite_ttl(path: str, graph: Graph):
    p = Path(path)
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".ttl", encoding="utf-8") as tf:
        tmp_name = tf.name
    graph.serialize(destination=tmp_name, format="turtle")
    Path(tmp_name).replace(p)

def looks_like_html(s: str) -> bool:
    t = s.lstrip().lower()
    return t.startswith("<!doctype html") or t.startswith("<html")

def now_ts_ms() -> int:
    return int(time.time() * 1000)

def migrate_old_state_to_new(state: Dict[str, Any]) -> Dict[str, Any]:
    # New shape: {"by_endpoint": { <url>: { "classes": iri, "classHierarchy": iri, "tbox": iri } } }
    if "by_endpoint" in state and isinstance(state["by_endpoint"], dict):
        return state
    new_state = {"by_endpoint": {}}
    for k, v in list(state.items()):
        if isinstance(v, list) and k.startswith("http"):
            if v:
                new_state["by_endpoint"].setdefault(k, {})[FUNC_CLASSES] = v[-1]
    return new_state

def iri_timestamp_fragment(graph_iri: str) -> str:
    frag = graph_iri.rstrip("/").split("/")[-1]
    return "".join(ch for ch in frag if ch.isalnum() or ch in ("_", "-"))

def get_or_create_graph_iri(state: Dict[str, Any], endpoint_url: str, func_key: str) -> str:
    """
    Stable IRI per (endpoint, functionality). When minting, ensure the timestamp
    isn't already used by another functionality for this endpoint; if so, increment.
    """
    state.setdefault("by_endpoint", {})
    per_ep = state["by_endpoint"].setdefault(endpoint_url, {})

    # reuse if already present
    iri = per_ep.get(func_key)
    if iri:
        return iri

    # set of timestamps already used by this endpoint (for other functionalities)
    used_ts = set()
    for existing in per_ep.values():
        if isinstance(existing, str) and existing.startswith(BASE_GRAPH_IRI):
            used_ts.add(iri_timestamp_fragment(existing))

    # mint unique timestamp (bump if collides)
    ts = now_ts_ms()
    while str(ts) in used_ts:
        ts += 1

    iri = f"{BASE_GRAPH_IRI}{ts}"
    per_ep[func_key] = iri
    return iri


# ------------------ SPARQL helpers ------------------
PREFIXES = """
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
"""

def run_select(endpoint_url: str, query_body: str, timeout: int) -> list[dict]:
    query = PREFIXES + "\n" + query_body
    # POST raw
    try:
        res = subprocess.run(
            ["curl","-sS","-L","--compressed","--max-time",str(timeout),
             "-H","Accept: application/sparql-results+json",
             "-H","Content-Type: application/sparql-query",
             "-H","User-Agent: curl/8",
             "--data-binary", query,
             endpoint_url],
            check=False, capture_output=True, text=True
        )
        if res.returncode == 0 and res.stdout.strip() and not looks_like_html(res.stdout):
            return json.loads(res.stdout).get("results", {}).get("bindings", [])
    except Exception:
        pass
    # POST urlencoded
    try:
        res = subprocess.run(
            ["curl","-sS","-L","--compressed","--max-time",str(timeout),
             "-H","Accept: application/sparql-results+json",
             "-H","User-Agent: curl/8",
             "--data-urlencode", f"query={query}",
             endpoint_url],
            check=False, capture_output=True, text=True
        )
        if res.returncode == 0 and res.stdout.strip() and not looks_like_html(res.stdout):
            return json.loads(res.stdout).get("results", {}).get("bindings", [])
    except Exception:
        pass
    # GET urlencoded
    try:
        res = subprocess.run(
            ["curl","-sS","-L","--compressed","--max-time",str(timeout),
             "-H","Accept: application/sparql-results+json",
             "-H","User-Agent: curl/8",
             "--get","--data-urlencode", f"query={query}",
             endpoint_url],
            check=False, capture_output=True, text=True
        )
        if res.returncode == 0 and res.stdout.strip() and not looks_like_html(res.stdout):
            return json.loads(res.stdout).get("results", {}).get("bindings", [])
    except Exception:
        pass
    return []

def fetch_construct_as_turtle(ep_url: str, query_body: str, timeout: int) -> str:
    query = PREFIXES + "\n" + query_body
    # POST raw
    try:
        res = subprocess.run(
            ["curl","-sS","-L","--compressed","--max-time",str(timeout),
             "-H","Accept: text/turtle",
             "-H","Content-Type: application/sparql-query",
             "-H","User-Agent: curl/8",
             "--data-binary", query,
             ep_url],
            check=False, capture_output=True, text=True
        )
        if res.returncode == 0 and res.stdout.strip() and not looks_like_html(res.stdout):
            return res.stdout
    except Exception:
        pass
    # POST urlencoded
    try:
        res = subprocess.run(
            ["curl","-sS","-L","--compressed","--max-time",str(timeout),
             "-H","Accept: text/turtle",
             "-H","User-Agent: curl/8",
             "--data-urlencode", f"query={query}",
             ep_url],
            check=False, capture_output=True, text=True
        )
        if res.returncode == 0 and res.stdout.strip() and not looks_like_html(res.stdout):
            return res.stdout
    except Exception:
        pass
    # GET urlencoded
    try:
        res = subprocess.run(
            ["curl","-sS","-L","--compressed","--max-time",str(timeout),
             "-H","Accept: text/turtle",
             "-H","User-Agent: curl/8",
             "--get","--data-urlencode", f"query={query}",
             ep_url],
            check=False, capture_output=True, text=True
        )
        if res.returncode == 0 and res.stdout.strip() and not looks_like_html(res.stdout):
            return res.stdout
    except Exception:
        pass
    return ""

def literal_int(bindings, varname: str) -> int:
    if not bindings:
        return 0
    v = bindings[0].get(varname, {}).get("value")
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return 0

def uris_from_bindings(bindings: list[dict], varname: str) -> Set[URIRef]:
    out: Set[URIRef] = set()
    for b in bindings:
        v = b.get(varname)
        if not v: continue
        if v.get("type") == "uri":
            out.add(URIRef(v["value"]))
    return out

# ------------------ Queries ------------------
def q_construct_classes_plain() -> str:
    return """
CONSTRUCT {
  ?c a owl:Class ;
     rdfs:label ?label .
}
WHERE {
  ?c a owl:Class .
  OPTIONAL { ?c rdfs:label ?l . FILTER(LANG(?l) = "" || LANGMATCHES(LANG(?l), "en")) }
  BIND(COALESCE(?l, STRAFTER(STR(?c), "#"), STRAFTER(STR(?c), "/")) AS ?label)
}
"""

def q_construct_class_hierarchy_plain() -> str:
    return """
CONSTRUCT {
  ?c a owl:Class ;
     rdfs:label ?clabel ;
     rdfs:subClassOf ?p .
  ?p rdfs:label ?plabel .
}
WHERE {
  ?c a owl:Class .
  OPTIONAL { ?c rdfs:label ?cl . FILTER(LANG(?cl) = "" || LANGMATCHES(LANG(?cl), "en")) }
  BIND(COALESCE(?cl, STRAFTER(STR(?c), "#"), STRAFTER(STR(?c), "/")) AS ?clabel)
  OPTIONAL {
    ?c rdfs:subClassOf ?p .
    OPTIONAL { ?p rdfs:label ?pl . FILTER(LANG(?pl) = "" || LANGMATCHES(LANG(?pl), "en")) }
    BIND(COALESCE(?pl, STRAFTER(STR(?p), "#"), STRAFTER(STR(?p), "/")) AS ?plabel)
  }
}
"""

def q_construct_tbox_plain() -> str:
    # TBox approximation: pull axioms about classes and properties (no instances), including:
    # - class typing and labels
    # - subclass, equivalentClass, disjointWith
    # - property typing (object/datatype), labels
    # - domain / range
    # NOTE: We keep it compact (no bnodes written later per your rule).
    return """
CONSTRUCT {
  ?s ?p ?o .
}
WHERE {
  {
    ?s a owl:Class .
    ?s ?p ?o .
    FILTER(?p IN (rdfs:label, rdf:type, rdfs:subClassOf, owl:equivalentClass, owl:disjointWith))
  }
  UNION
  {
    ?s a owl:ObjectProperty .
    ?s ?p ?o .
    FILTER(?p IN (rdfs:label, rdf:type, rdfs:domain, rdfs:range, owl:inverseOf, owl:equivalentProperty, owl:propertyDisjointWith))
  }
  UNION
  {
    ?s a owl:DatatypeProperty .
    ?s ?p ?o .
    FILTER(?p IN (rdfs:label, rdf:type, rdfs:domain, rdfs:range, owl:equivalentProperty, owl:propertyDisjointWith))
  }
}
"""

Q_NUM_CLASSES = "SELECT (COUNT(DISTINCT ?c) AS ?n) WHERE { ?c a owl:Class . FILTER(isIRI(?c)) }"
Q_NUM_OBJ_P   = "SELECT (COUNT(DISTINCT ?p) AS ?n) WHERE { ?p a owl:ObjectProperty . }"
Q_NUM_DAT_P   = "SELECT (COUNT(DISTINCT ?p) AS ?n) WHERE { ?p a owl:DatatypeProperty . }"
Q_NUM_INST    = """
SELECT (COUNT(DISTINCT ?i) AS ?n) WHERE {
  ?i a ?t . ?t a owl:Class .
  FILTER(isIRI(?i))
}
"""

# ------------------ Discovery from all.ttl ------------------
def discover_from_all_ttl(all_ttl_path: str):
    g = Graph()
    g.parse(all_ttl_path, format="turtle")

    endpoints = []
    for ep_ind in g.subjects(RDF.type, ENDPOINT_CLASS):
        for v in g.objects(ep_ind, ENDPOINT_PREDICATE):
            url = as_http_url_str(v)
            if url:
                endpoints.append((ep_ind, url))

    seen = set(); uniq = []
    for ep_ind, url in endpoints:
        k = (str(ep_ind), url)
        if k not in seen:
            seen.add(k); uniq.append((ep_ind, url))

    ep_to_datasets = {}
    for ds in g.subjects(RDF.type, DATASET_TYPE):
        for ep_ind in g.objects(ds, IAO_0000235):
            ep_to_datasets.setdefault(ep_ind, set()).add(ds)

    out = []
    for ep_ind, url in uniq:
        out.append({"endpoint_individual": ep_ind, "endpoint_url": url, "datasets": ep_to_datasets.get(ep_ind, set())})
    return g, out

# ------------------ Load MWO terms ------------------
def load_mwo_terms(mwo_path: str):
    if not Path(mwo_path).exists():
        raise FileNotFoundError(f"MWO ontology not found at {mwo_path}")
    g = Graph()
    g.parse(mwo_path)
    mwo_classes: Set[URIRef] = set()
    mwo_objprops: Set[URIRef] = set()
    mwo_datprops: Set[URIRef] = set()
    for c in g.subjects(RDF.type, OWL.Class):
        if isinstance(c, URIRef): mwo_classes.add(c)
    for p in g.subjects(RDF.type, OWL.ObjectProperty):
        if isinstance(p, URIRef): mwo_objprops.add(p)
    for p in g.subjects(RDF.type, OWL.DatatypeProperty):
        if isinstance(p, URIRef): mwo_datprops.add(p)
    return mwo_classes, mwo_objprops, mwo_datprops

# ------------------ Reuse (sets) ------------------
def batch_values(items: Iterable[URIRef], batch_size: int = 150) -> List[List[URIRef]]:
    batch = []; out = []
    for it in items:
        batch.append(it)
        if len(batch) >= batch_size:
            out.append(batch); batch = []
    if batch: out.append(batch)
    return out

def uris_from_bindings_set(bindings: list[dict], varname: str) -> Set[URIRef]:
    return uris_from_bindings(bindings, varname)

def reused_classes(ep_url: str, class_iris: Set[URIRef]) -> Set[URIRef]:
    reused: Set[URIRef] = set()
    for chunk in batch_values(class_iris):
        values = " ".join(f"<{str(c)}>" for c in chunk)
        q = f"""
        SELECT DISTINCT ?c WHERE {{
          VALUES ?c {{ {values} }}
          {{ ?c a owl:Class . }} UNION {{ ?x a ?c . }}
        }}
        """
        reused |= uris_from_bindings_set(run_select(ep_url, q, REQUEST_TIMEOUT), "c")
    return reused

def reused_objprops(ep_url: str, prop_iris: Set[URIRef]) -> Set[URIRef]:
    reused: Set[URIRef] = set()
    for chunk in batch_values(prop_iris):
        values = " ".join(f"<{str(p)}>" for p in chunk)
        q = f"""
        SELECT DISTINCT ?p WHERE {{
          VALUES ?p {{ {values} }}
          {{ ?p a owl:ObjectProperty . }} UNION {{ ?s ?p ?o . FILTER(isIRI(?o)) }}
        }}
        """
        reused |= uris_from_bindings_set(run_select(ep_url, q, REQUEST_TIMEOUT), "p")
    return reused

def reused_dataprops(ep_url: str, prop_iris: Set[URIRef]) -> Set[URIRef]:
    reused: Set[URIRef] = set()
    for chunk in batch_values(prop_iris):
        values = " ".join(f"<{str(p)}>" for p in chunk)
        q = f"""
        SELECT DISTINCT ?p WHERE {{
          VALUES ?p {{ {values} }}
          {{ ?p a owl:DatatypeProperty . }} UNION {{ ?s ?p ?o . FILTER(isLiteral(?o)) }}
        }}
        """
        reused |= uris_from_bindings_set(run_select(ep_url, q, REQUEST_TIMEOUT), "p")
    return reused

# ------------------ Writers ------------------
def write_named_graph_files(graph_iri: str, turtle_text: str, nq_path: Path, ttl_path: Path):
    """
    Parse the Turtle, drop any triples with blank nodes, then:
      - write N-Quads with named graph IRI to nq_path
      - write Turtle (triples only) to ttl_path
    """
    if not turtle_text:
        return
    tmp_g = Graph()
    tmp_g.parse(data=turtle_text, format="turtle")

    # 1) N-Quads with GRAPH IRI
    cg = ConjunctiveGraph()
    ctx = cg.get_context(URIRef(graph_iri))
    for s, p, o in tmp_g:
        if isinstance(s, BNode) or isinstance(p, BNode) or isinstance(o, BNode):
            continue
        ctx.add((s, p, o))
    nq_path.parent.mkdir(parents=True, exist_ok=True)
    cg.serialize(destination=str(nq_path), format="nquads", encoding="utf-8")

    # 2) Plain Turtle (triples only)
    g_plain = Graph()
    for s, p, o in tmp_g:
        if isinstance(s, BNode) or isinstance(p, BNode) or isinstance(o, BNode):
            continue
        g_plain.add((s, p, o))
    ttl_path.parent.mkdir(parents=True, exist_ok=True)
    safe_overwrite_ttl(str(ttl_path), g_plain)

# ------------------ Main ------------------
def main():
    # Stable IDs state
    state = load_state(STATE_JSON)
    state = migrate_old_state_to_new(state)

    # Discover endpoints/datasets
    g_all, discovered = discover_from_all_ttl(ALL_TTL)
    if not discovered:
        print("No SPARQL endpoints found in all.ttl.")
        return

    # Load MWO terms for reuse stats
    mwo_classes, mwo_objprops, mwo_datprops = load_mwo_terms(MWO_OWL_PATH)
    print(f"Loaded MWO terms: classes={len(mwo_classes)}, objProps={len(mwo_objprops)}, dataProps={len(mwo_datprops)}")

    # Stats graph
    stats = Graph()
    stats.bind("stat", STAT); stats.bind("rdfs", RDFS); stats.bind("owl", OWL)

    summary = []

    # Ensure output directory
    Path(NAMED_GRAPHS_DIR).mkdir(parents=True, exist_ok=True)

    for rec in discovered:
        ep_ind = rec["endpoint_individual"]
        ep_url = rec["endpoint_url"]
        datasets = rec["datasets"]

        # Stable IRIs per functionality
        iri_classes = get_or_create_graph_iri(state, ep_url, FUNC_CLASSES)
        iri_hier    = get_or_create_graph_iri(state, ep_url, FUNC_CLASS_HIER)
        iri_tbox    = get_or_create_graph_iri(state, ep_url, FUNC_TBOX)

        stamp_classes = iri_timestamp_fragment(iri_classes)
        stamp_hier    = iri_timestamp_fragment(iri_hier)
        stamp_tbox    = iri_timestamp_fragment(iri_tbox)

        files_classes = (Path(f"{NAMED_GRAPHS_DIR}{stamp_classes}.nq"), Path(f"{NAMED_GRAPHS_DIR}{stamp_classes}.ttl"))
        files_hier    = (Path(f"{NAMED_GRAPHS_DIR}{stamp_hier}.nq"),    Path(f"{NAMED_GRAPHS_DIR}{stamp_hier}.ttl"))
        files_tbox    = (Path(f"{NAMED_GRAPHS_DIR}{stamp_tbox}.nq"),    Path(f"{NAMED_GRAPHS_DIR}{stamp_tbox}.ttl"))

        # --- Stats basics ---
        def one(q): return literal_int(run_select(ep_url, q, REQUEST_TIMEOUT), "n")
        num_classes = one(Q_NUM_CLASSES)
        num_objp    = one(Q_NUM_OBJ_P)
        num_datp    = one(Q_NUM_DAT_P)
        num_inst    = one(Q_NUM_INST)

        # --- Reuse sets from MWO ---
        mwo_classes_used = reused_classes(ep_url, mwo_classes) if mwo_classes else set()
        mwo_objp_used    = reused_objprops(ep_url, mwo_objprops) if mwo_objprops else set()
        mwo_datp_used    = reused_dataprops(ep_url, mwo_datprops) if mwo_datprops else set()

        # --- Build & write the three named graphs (skip bnodes) ---
        # 1) Classes (labels)
        ttl_classes = fetch_construct_as_turtle(ep_url, q_construct_classes_plain(), REQUEST_TIMEOUT)
        if ttl_classes:
            write_named_graph_files(iri_classes, ttl_classes, files_classes[0], files_classes[1])
            print(f"[{ep_url}] classes → {files_classes[0].name}, {files_classes[1].name}")
        else:
            print(f"WARNING: classes CONSTRUCT returned nothing for {ep_url}")

        # 2) Class hierarchy
        ttl_hier = fetch_construct_as_turtle(ep_url, q_construct_class_hierarchy_plain(), REQUEST_TIMEOUT)
        if ttl_hier:
            write_named_graph_files(iri_hier, ttl_hier, files_hier[0], files_hier[1])
            print(f"[{ep_url}] classHierarchy → {files_hier[0].name}, {files_hier[1].name}")
        else:
            print(f"WARNING: classHierarchy CONSTRUCT returned nothing for {ep_url}")

        # 3) TBox (class & property axioms)
        ttl_tbox = fetch_construct_as_turtle(ep_url, q_construct_tbox_plain(), REQUEST_TIMEOUT)
        if ttl_tbox:
            write_named_graph_files(iri_tbox, ttl_tbox, files_tbox[0], files_tbox[1])
            print(f"[{ep_url}] tbox → {files_tbox[0].name}, {files_tbox[1].name}")
        else:
            print(f"WARNING: tbox CONSTRUCT returned nothing for {ep_url}")

        # --- Annotate datasets with stats + reused lists + links to ALL graphs ---
        for ds in datasets:
            stats.add((ds, RDF.type, DATASET_TYPE))
            stats.add((ds, IAO_0000235, ep_ind))

            # numeric stats
            stats.add((ds, STAT.numberOfClasses, Literal(num_classes, datatype=XSD.integer)))
            stats.add((ds, STAT.numberOfObjectProperties, Literal(num_objp, datatype=XSD.integer)))
            stats.add((ds, STAT.numberOfDataProperties, Literal(num_datp, datatype=XSD.integer)))
            stats.add((ds, STAT.numberOfInstances, Literal(num_inst, datatype=XSD.integer)))

            # reuse counts
            stats.add((ds, STAT.numberOfClassesReusedFromMWO, Literal(len(mwo_classes_used), datatype=XSD.integer)))
            stats.add((ds, STAT.numberOfObjectPropertiesReusedFromMWO, Literal(len(mwo_objp_used), datatype=XSD.integer)))
            stats.add((ds, STAT.numberOfDataPropertiesReusedFromMWO, Literal(len(mwo_datp_used), datatype=XSD.integer)))

            # reuse lists
            for c in sorted(mwo_classes_used, key=str):
                stats.add((ds, STAT.classReusedFromMWO, c))
            for p in sorted(mwo_objp_used, key=str):
                stats.add((ds, STAT.objectPropertyReusedFromMWO, p))
            for p in sorted(mwo_datp_used, key=str):
                stats.add((ds, STAT.dataPropertyReusedFromMWO, p))

            # link dataset to each snapshot graph
            for iri in (iri_classes, iri_hier, iri_tbox):
                stats.add((ds, HAS_GRAPH_PRED, URIRef(iri)))
                t = (ds, HAS_GRAPH_PRED, URIRef(iri))
                if t not in g_all:
                    g_all.add(t)

        # Collect summary record
        summary.append({
            "endpoint_individual": str(ep_ind),
            "endpoint": ep_url,
            "graphs": {
                "classes": {"iri": iri_classes, "nq": str(files_classes[0]), "ttl": str(files_classes[1])},
                "classHierarchy": {"iri": iri_hier, "nq": str(files_hier[0]), "ttl": str(files_hier[1])},
                "tbox": {"iri": iri_tbox, "nq": str(files_tbox[0]), "ttl": str(files_tbox[1])},
            },
            "datasets": [str(d) for d in datasets],
            "stats": {
                "classes": num_classes,
                "objectProperties": num_objp,
                "dataProperties": num_datp,
                "instances": num_inst,
                "classesReusedFromMWO": len(mwo_classes_used),
                "objectPropertiesReusedFromMWO": len(mwo_objp_used),
                "dataPropertiesReusedFromMWO": len(mwo_datp_used),
                "classesReusedFromMWO_list": [str(x) for x in sorted(mwo_classes_used, key=str)],
                "objectPropertiesReusedFromMWO_list": [str(x) for x in sorted(mwo_objp_used, key=str)],
                "dataPropertiesReusedFromMWO_list": [str(x) for x in sorted(mwo_datp_used, key=str)],
            }
        })

    # Save stats + catalog + state + summary
    ensure_parent(STATS_TTL); safe_overwrite_ttl(STATS_TTL, stats)
    safe_overwrite_ttl(ALL_TTL, g_all)
    save_state(STATE_JSON, state)
    ensure_parent(SUMMARY_JSON)
    Path(SUMMARY_JSON).write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\nNamed graphs directory:    {NAMED_GRAPHS_DIR}")
    print(f"Stats written to:          {STATS_TTL}")
    print(f"Catalog updated:           {ALL_TTL}")
    print(f"State:                     {STATE_JSON}")
    print("Done.")

if __name__ == "__main__":
    main()
