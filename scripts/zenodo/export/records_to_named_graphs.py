#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build named-graph snapshots from Zenodo RDF (by community OR by single DOI/record).

Per dataset (DOI), produce up to three graphs (if non-empty):
  1) classes         — owl:Class with labels
  2) classHierarchy  — owl:Class with rdfs:subClassOf (+ labels)
  3) tbox            — class & property axioms (compact, bnodes omitted)

Stable IRI per (DOI-or-id, functionality):
  https://purls.helmholtz-metadaten.de/msekg/<timestamp>
(first run mints; later runs reuse it; collisions across functionalities are prevented).

Output layout (by DOI):
  data/zenodo/named_graphs/record/<doi-safe>/<timestamp>.nq
  data/zenodo/named_graphs/record/<doi-safe>/<timestamp>.ttl

We also add/merge dataset annotations into a single catalog file:
  data/zenodo/zenodo.ttl
(contains rdfs:seeAlso links to each graph, local file paths, counts, and reused-from-MWO lists)
"""

import json
import os
import re
import time
from pathlib import Path
from typing import Dict, Any, Iterable, List, Set, Tuple, Optional

import requests
from rdflib import Graph, ConjunctiveGraph, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, OWL, XSD

# ---- your package-local helpers ----
from .downloads import (
    should_fetch_rdf_like,
    download_rdf_files,
    download_rdf_and_zip_collect,
)
from .zenodo_api import (
    iter_community_records,
    extract_files,
    normalize_community,
    ZENODO_API,
    ZENODO_PAGE_SIZE,
    ZENODO_MAX_PAGES,
    ZENODO_TIMEOUT,
)
from .state import load_state, save_state, record_key

# ------------------ Config (env-overridable) ------------------
BASE_GRAPH_IRI = os.environ.get(
    "BASE_GRAPH_IRI", "https://purls.helmholtz-metadaten.de/msekg/"
).rstrip("/") + "/"

DEFAULT_OUT_DIR   = Path(os.environ.get("ZEN_OUT_DIR", "data/zenodo/named_graphs/"))
DEFAULT_STATE_FILE = Path(os.environ.get("ZEN_STATE_FILE", "data/zenodo/.graph_ids.json"))
ZCAT_TTL          = Path(os.environ.get("ZEN_CATALOG_TTL", "data/zenodo/zenodo.ttl"))

# Local MWO ontology path for “reused from MWO”
MWO_OWL_PATH = Path(os.environ.get("MWO_OWL_PATH", "ontology/mwo-full.owl"))

# Functionality keys → stable graph per dataset
FUNC_CLASSES        = "classes"
FUNC_CLASS_HIER     = "classHierarchy"
FUNC_TBOX           = "tbox"

# Namespace for stats/annotations
STAT = URIRef("https://purls.helmholtz-metadaten.de/msekg/stat/")

# ------------------ Utilities ------------------
def now_ts_ms() -> int:
    return int(time.time() * 1000)

def iri_timestamp_fragment(graph_iri: str) -> str:
    frag = graph_iri.rstrip("/").split("/")[-1]
    return "".join(ch for ch in frag if ch.isalnum() or ch in ("_", "-"))

def ensure_state_shapes(state: Dict[str, Any]) -> Dict[str, Any]:
    s = dict(state) if isinstance(state, dict) else {}
    s.setdefault("by_record", {})   # { recordKey (doi|id): { func_key: graph_iri, ... } }
    return s

def dedupe_existing_per_record_state(state: Dict[str, Any]) -> None:
    br = state.get("by_record", {})
    for rec_id, per_rec in br.items():
        seen_ts = set()
        ordered = [FUNC_CLASSES, FUNC_CLASS_HIER, FUNC_TBOX] + [
            k for k in sorted(per_rec.keys()) if k not in {FUNC_CLASSES, FUNC_CLASS_HIER, FUNC_TBOX}
        ]
        for func_key in ordered:
            iri = per_rec.get(func_key)
            if not isinstance(iri, str) or not iri.startswith(BASE_GRAPH_IRI):
                continue
            ts = iri_timestamp_fragment(iri)
            if ts in seen_ts:
                try:
                    n = int(ts)
                except ValueError:
                    n = now_ts_ms()
                while str(n) in seen_ts:
                    n += 1
                per_rec[func_key] = f"{BASE_GRAPH_IRI}{n}"
                ts = str(n)
            seen_ts.add(ts)

def get_or_create_graph_iri(state: Dict[str, Any], rec_key: str, func_key: str) -> str:
    state.setdefault("by_record", {})
    per_rec = state["by_record"].setdefault(rec_key, {})
    iri = per_rec.get(func_key)
    if iri:
        return iri

    used_ts = set()
    for existing in per_rec.values():
        if isinstance(existing, str) and existing.startswith(BASE_GRAPH_IRI):
            used_ts.add(iri_timestamp_fragment(existing))

    ts = now_ts_ms()
    while str(ts) in used_ts:
        ts += 1

    iri = f"{BASE_GRAPH_IRI}{ts}"
    per_rec[func_key] = iri
    return iri

def doi_safe_folder(doi_or_id: str) -> str:
    # Use DOI preferred; if not DOI, use id string.
    # Replace slashes and illegal filename chars.
    s = doi_or_id.strip()
    s = s.replace("https://doi.org/", "")
    s = s.replace("http://doi.org/", "")
    s = s.replace("doi:", "")
    # keep alnum, dot, dash, underscore; turn others into _
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)

def write_named_graph_files(graph_iri: str, turtle_text: str, out_dir: Path) -> Tuple[Optional[Path], Optional[Path], int]:
    """
    Parse the Turtle, drop any triples with blank nodes, then:
      - write N-Quads with named graph IRI to <out_dir>/<timestamp>.nq
      - write Turtle (triples only) to <out_dir>/<timestamp>.ttl
    Returns (nq_path, ttl_path, triple_count). If triple_count==0 → returns (None,None,0).
    """
    g_in = Graph()
    g_in.parse(data=turtle_text, format="turtle")

    # Filter out any triple that involves a blank node
    triples = [(s, p, o) for (s, p, o) in g_in if not (isinstance(s, BNode) or isinstance(p, BNode) or isinstance(o, BNode))]
    if not triples:
        return None, None, 0

    ts = iri_timestamp_fragment(graph_iri)
    out_dir.mkdir(parents=True, exist_ok=True)

    # N-Quads
    cg = ConjunctiveGraph()
    ctx = cg.get_context(URIRef(graph_iri))
    for s, p, o in triples:
        ctx.add((s, p, o))
    nq_path = out_dir / f"{ts}.nq"
    cg.serialize(destination=str(nq_path), format="nquads", encoding="utf-8")

    # Turtle
    g_out = Graph()
    for s, p, o in triples:
        g_out.add((s, p, o))
    ttl_path = out_dir / f"{ts}.ttl"
    g_out.serialize(destination=str(ttl_path), format="turtle")

    return nq_path, ttl_path, len(triples)

# ------------------ Builders (from a local graph) ------------------
PREFIXES = """
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
"""

def construct_classes_plain_local(g: Graph) -> str:
    q = PREFIXES + """
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
    tmp = Graph()
    tmp += g.query(q).graph
    return tmp.serialize(format="turtle")

def construct_class_hierarchy_plain_local(g: Graph) -> str:
    q = PREFIXES + """
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
    tmp = Graph()
    tmp += g.query(q).graph
    return tmp.serialize(format="turtle")

def construct_tbox_plain_local(g: Graph) -> str:
    q = PREFIXES + """
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
    tmp = Graph()
    tmp += g.query(q).graph
    return tmp.serialize(format="turtle")

# ------------------ Counts & MWO reuse on local graph ------------------
def local_counts(g: Graph) -> Dict[str, int]:
    classes = len({c for c in g.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)})
    objp = len({p for p in g.subjects(RDF.type, OWL.ObjectProperty) if isinstance(p, URIRef)})
    datap = len({p for p in g.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(p, URIRef)})
    instances = len({s for s, _, _ in g.triples((None, RDF.type, None)) if isinstance(s, URIRef)})
    return dict(classes=classes, objectProperties=objp, dataProperties=datap, instances=instances)

def load_mwo_terms(mwo_path: Path) -> Tuple[Set[URIRef], Set[URIRef], Set[URIRef]]:
    if not mwo_path.exists():
        return set(), set(), set()
    m = Graph()
    m.parse(mwo_path)
    mwo_classes: Set[URIRef] = {c for c in m.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)}
    mwo_objp: Set[URIRef]    = {p for p in m.subjects(RDF.type, OWL.ObjectProperty) if isinstance(p, URIRef)}
    mwo_datp: Set[URIRef]    = {p for p in m.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(p, URIRef)}
    return mwo_classes, mwo_objp, mwo_datp

def reused_from_mwo_local(g: Graph, mwo_classes: Set[URIRef], mwo_objp: Set[URIRef], mwo_datp: Set[URIRef]) -> Tuple[Set[URIRef], Set[URIRef], Set[URIRef]]:
    used_cls: Set[URIRef] = set()
    for c in mwo_classes:
        if (c, RDF.type, OWL.Class) in g or next(g.triples((None, RDF.type, c)), None):
            used_cls.add(c)
    used_op: Set[URIRef] = set()
    for p in mwo_objp:
        if (p, RDF.type, OWL.ObjectProperty) in g:
            used_op.add(p); continue
        for _ in g.triples((None, p, None)):
            used_op.add(p); break
    used_dp: Set[URIRef] = set()
    for p in mwo_datp:
        if (p, RDF.type, OWL.DatatypeProperty) in g:
            used_dp.add(p); continue
        for s,o in ((s,o) for s,_,o in g.triples((None, p, None))):
            if not isinstance(o, URIRef):
                used_dp.add(p); break
    return used_cls, used_op, used_dp

# ------------------ Fetch single record by DOI or record URL ------------------
def fetch_record_by_doi(doi: str, token: Optional[str]) -> Optional[dict]:
    """
    Resolve a DOI to a Zenodo record via the Search API.
    """
    doi_norm = doi.strip().replace("https://doi.org/", "").replace("http://doi.org/", "").replace("doi:", "")
    q = f'doi:"{doi_norm}"'
    params = {"q": q, "size": 1}
    headers = {"Accept": "application/json"}
    if token:
        params["access_token"] = token
    try:
        r = requests.get(ZENODO_API, params=params, headers=headers, timeout=ZENODO_TIMEOUT)
        if r.status_code == 200:
            js = r.json()
            hits = js.get("hits", {}).get("hits", [])
            if hits:
                return hits[0]
    except Exception:
        pass
    return None

def fetch_record_by_url(url: str, token: Optional[str]) -> Optional[dict]:
    """
    Support https://zenodo.org/record/<id> or https://zenodo.org/records/<id>
    """
    m = re.search(r"/record[s]?/(\d+)", url)
    if not m:
        return None
    rec_id = m.group(1)
    params = {}
    if token:
        params["access_token"] = token
    headers = {"Accept": "application/json"}
    try:
        r = requests.get(f"{ZENODO_API}/{rec_id}", params=params, headers=headers, timeout=ZENODO_TIMEOUT)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

# ------------------ Core (one record) ------------------
def process_one_record(rec: dict, out_dir_root: Path, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a summary dict for this record (or {} if nothing was written).
    """
    # Prefer DOI as key + folder, else numeric id
    md = rec.get("metadata") or {}
    doi = md.get("doi") or rec.get("doi")  # sometimes in different places
    key = (doi or str(rec.get("id"))).strip()
    folder = out_dir_root / doi_safe_folder(key)

    files = extract_files(rec)
    rdfish = [f for f in files if should_fetch_rdf_like(f.get("key"), f.get("link"))]
    if not rdfish:
        return {}

    # download RDF (direct + RDF found inside ZIPs)
    download_root = folder / "tmp"
    dl_direct = download_rdf_files(rdfish, download_root, os.environ.get("ZENODO_TOKEN"))
    dl_zip = download_rdf_and_zip_collect(rdfish, download_root, os.environ.get("ZENODO_TOKEN"))

    # merge into one local graph per record
    g_rec = Graph()
    # parse direct
    for local_path, content_type, fmeta in dl_direct:
        try:
            g_rec.parse(local_path)
        except Exception:
            try:
                g_rec.parse(local_path, format="turtle")
            except Exception:
                pass
    # parse from zip
    for local_path, content_type, fmeta, inner_name in dl_zip:
        try:
            g_rec.parse(local_path)
        except Exception:
            try:
                g_rec.parse(local_path, format="turtle")
            except Exception:
                pass

    if len(g_rec) == 0:
        return {}

    # stable IRIs (prevent collisions by bumping ts if needed)
    iri_classes = get_or_create_graph_iri(state, key, FUNC_CLASSES)
    iri_hier    = get_or_create_graph_iri(state, key, FUNC_CLASS_HIER)
    iri_tbox    = get_or_create_graph_iri(state, key, FUNC_TBOX)

    # build snapshots from local graph
    ttl_classes = construct_classes_plain_local(g_rec)
    ttl_hier    = construct_class_hierarchy_plain_local(g_rec)
    ttl_tbox    = construct_tbox_plain_local(g_rec)

    # write files (but only if non-empty after bnode-filter)
    written = []
    nq_classes, ttl_classes_path, n1 = write_named_graph_files(iri_classes, ttl_classes, folder)
    if n1 > 0:
        written.append(("classes", iri_classes, nq_classes, ttl_classes_path))
    nq_hier, ttl_hier_path, n2 = write_named_graph_files(iri_hier, ttl_hier, folder)
    if n2 > 0:
        written.append(("classHierarchy", iri_hier, nq_hier, ttl_hier_path))
    nq_tbox, ttl_tbox_path, n3 = write_named_graph_files(iri_tbox, ttl_tbox, folder)
    if n3 > 0:
        written.append(("tbox", iri_tbox, nq_tbox, ttl_tbox_path))

    if not written:
        return {}  # nothing to save

    # counts + reuse lists
    cnt = local_counts(g_rec)
    mwo_classes, mwo_objp, mwo_datp = load_mwo_terms(MWO_OWL_PATH)
    used_cls, used_op, used_dp = reused_from_mwo_local(g_rec, mwo_classes, mwo_objp, mwo_datp)

    return {
        "key": key,
        "doi": doi,
        "record_id": rec.get("id"),
        "graphs": [
            {"func": func, "iri": iri, "nq": str(nq), "ttl": str(ttl)}
            for (func, iri, nq, ttl) in written
        ],
        "counts": cnt,
        "reuse": {
            "classes": [str(x) for x in sorted(used_cls, key=str)],
            "objectProperties": [str(x) for x in sorted(used_op, key=str)],
            "dataProperties": [str(x) for x in sorted(used_dp, key=str)],
        },
    }

# ------------------ Catalog writer (zenodo.ttl) ------------------
def merge_into_catalog(catalog_path: Path, summaries: List[Dict[str, Any]]) -> None:
    """
    Adds/updates dataset nodes (by DOI if available, else by record id):
      - rdfs:seeAlso → each named graph IRI
      - stat:graphFileNQ / stat:graphFileTTL → local file path literals
      - counts + reuse lists
    """
    g = Graph()
    if catalog_path.exists():
        try:
            g.parse(catalog_path)
        except Exception:
            # try explicit turtle
            try: g.parse(catalog_path, format="turtle")
            except Exception: pass

    def u(prop: str) -> URIRef:
        return URIRef(str(STAT) + prop)

    for s in summaries:
        if not s:
            continue
        subj = URIRef("https://doi.org/" + s["doi"]) if s.get("doi") else URIRef(f"https://zenodo.org/record/{s['record_id']}")

        # links to snapshot graphs + local file paths
        for item in s["graphs"]:
            iri = URIRef(item["iri"])
            g.add((subj, RDFS.seeAlso, iri))
            g.add((subj, u("graphFileNQ"), Literal(item["nq"], datatype=XSD.string)))
            g.add((subj, u("graphFileTTL"), Literal(item["ttl"], datatype=XSD.string)))

        # numeric counts
        cnt = s["counts"]
        g.add((subj, u("numberOfClasses"), Literal(cnt["classes"], datatype=XSD.integer)))
        g.add((subj, u("numberOfObjectProperties"), Literal(cnt["objectProperties"], datatype=XSD.integer)))
        g.add((subj, u("numberOfDataProperties"), Literal(cnt["dataProperties"], datatype=XSD.integer)))
        g.add((subj, u("numberOfInstances"), Literal(cnt["instances"], datatype=XSD.integer)))

        # reuse lists + counts
        rl = s["reuse"]
        g.add((subj, u("numberOfClassesReusedFromMWO"), Literal(len(rl["classes"]), datatype=XSD.integer)))
        g.add((subj, u("numberOfObjectPropertiesReusedFromMWO"), Literal(len(rl["objectProperties"]), datatype=XSD.integer)))
        g.add((subj, u("numberOfDataPropertiesReusedFromMWO"), Literal(len(rl["dataProperties"]), datatype=XSD.integer)))
        for x in rl["classes"]:
            g.add((subj, u("classReusedFromMWO"), URIRef(x)))
        for x in rl["objectProperties"]:
            g.add((subj, u("objectPropertyReusedFromMWO"), URIRef(x)))
        for x in rl["dataProperties"]:
            g.add((subj, u("dataPropertyReusedFromMWO"), URIRef(x)))

    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    g.serialize(destination=str(catalog_path), format="turtle")

# ------------------ Entry points ------------------
def run_for_community(
    community: str,
    out_dir_root: Path,
    state_file: Path,
    token: Optional[str],
) -> None:
    state = ensure_state_shapes(load_state(state_file))
    dedupe_existing_per_record_state(state)

    summaries: List[Dict[str, Any]] = []
    for rec in iter_community_records(normalize_community(community), token):
        summary = process_one_record(rec, out_dir_root, state)
        if summary:
            summaries.append(summary)

    if summaries:
        merge_into_catalog(ZCAT_TTL, summaries)
    save_state(state_file, state)

def run_for_single_doi(
    doi: str,
    out_dir_root: Path,
    state_file: Path,
    token: Optional[str],
) -> None:
    rec = fetch_record_by_doi(doi, token)
    if not rec:
        print(f"Could not resolve DOI: {doi}")
        return
    state = ensure_state_shapes(load_state(state_file))
    dedupe_existing_per_record_state(state)
    summary = process_one_record(rec, out_dir_root, state)
    if summary:
        merge_into_catalog(ZCAT_TTL, [summary])
        save_state(state_file, state)
        print(f"Processed DOI {doi}")
    else:
        print(f"No non-empty RDF snapshots for DOI {doi}")

def run_for_record_url(
    record_url: str,
    out_dir_root: Path,
    state_file: Path,
    token: Optional[str],
) -> None:
    rec = fetch_record_by_url(record_url, token)
    if not rec:
        print(f"Could not fetch record: {record_url}")
        return
    state = ensure_state_shapes(load_state(state_file))
    dedupe_existing_per_record_state(state)
    summary = process_one_record(rec, out_dir_root, state)
    if summary:
        merge_into_catalog(ZCAT_TTL, [summary])
        save_state(state_file, state)
        print(f"Processed record {record_url}")
    else:
        print(f"No non-empty RDF snapshots for record {record_url}")

# ------------- CLI -------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Build named-graph snapshots from Zenodo RDF.")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--community", help="Zenodo community slug or URL (e.g., 'nfdi-matwerk')")
    g.add_argument("--doi", help="A single DOI (e.g., 10.5281/zenodo.123456)")
    g.add_argument("--record-url", help="A single record URL (https://zenodo.org/record/<id> or /records/<id>)")
    parser.add_argument("--token", default=os.environ.get("ZENODO_TOKEN"), help="Zenodo API token (env ZENODO_TOKEN)")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Root directory for named graphs (by DOI)")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE), help="State JSON to persist graph IRIs")
    args = parser.parse_args()

    out_dir_root = Path(args.out_dir)
    out_dir_root.mkdir(parents=True, exist_ok=True)
    state_file = Path(args.state_file)

    if args.community:
        run_for_community(args.community, out_dir_root, state_file, args.token)
    elif args.doi:
        run_for_single_doi(args.doi, out_dir_root, state_file, args.token)
    elif args.record_url:
        run_for_record_url(args.record_url, out_dir_root, state_file, args.token)
