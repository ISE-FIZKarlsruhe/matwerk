#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from typing import Set, Dict, Any, List, Tuple, Optional
import argparse
import requests
import re
import hashlib
from urllib.parse import urlsplit, urlunsplit, quote

from .zenodo_api import ZENODO_API, ZENODO_TIMEOUT

from rdflib import Graph, ConjunctiveGraph, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, OWL, XSD

from .downloads import download_rdf_files, download_rdf_and_zip_collect
from .iri_utils import normalize_graph_uris, safe_slug
from .rdf_builders import build_record_in_default_graph
from .state import record_key
from .zenodo_api import (
    normalize_community,
    iter_community_records,
    extract_files,
    get_records_by_ids,
    get_metadata,
)
from .mint import get_or_mint_instance, get_or_mint_file_graph, get_or_mint_file_identifier
from .rdf_import import import_rdf_into_named_graph

# ------------------ Snapshot config ------------------
BASE_GRAPH_IRI = os.environ.get(
    "BASE_GRAPH_IRI", "https://nfdi.fiz-karlsruhe.de/matwerk/msekg/"
).rstrip("/") + "/"

FUNC_CLASSES = "classes"
FUNC_CLASS_HIER = "classHierarchy"
FUNC_TBOX = "tbox"

MWO_OWL_PATH = Path(os.environ.get("MWO_OWL_PATH", "ontology/mwo-full.owl"))

# ------------------ Utils ------------------
def _doi_safe(doi_or_id: str) -> str:
    s = doi_or_id.strip().replace("https://doi.org/", "").replace("http://doi.org/", "").replace("doi:", "")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)

def _stable_snapshot_iri(rec_key: str, func_key: str) -> str:
    """
    Deterministic snapshot graph IRI per (rec_key, func_key), no state file required.
    Human-readable + stable.
    """
    return f"{BASE_GRAPH_IRI}snapshot/{func_key}/{_doi_safe(rec_key)}"

def _stable_file_stem(s: str) -> str:
    """
    Safe file stem derived from IRI (avoid slashes / unsafe chars).
    """
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]

def to_doi_iri(s: str) -> str:
    s = urlsplit(s)
    path = quote(s.path, safe="/:@!$&'()*+,;=~-._")
    query = quote(s.query, safe="=&:@!$'()*+,;=~-._")
    frag = quote(s.fragment, safe=":@!$&'()*+,;=~-._")
    return urlunsplit((s.scheme, s.netloc, path, query, frag))

# ------------------ Builders (from a local graph) ------------------
PREFIXES = """
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
"""

def _construct_classes_local(g: Graph) -> Graph:
    q = PREFIXES + """
    CONSTRUCT {
      ?c a owl:Class ; rdfs:label ?label .
    } WHERE {
      ?c a owl:Class .
      OPTIONAL { ?c rdfs:label ?l . FILTER(LANG(?l) = "" || LANGMATCHES(LANG(?l),"en")) }
      BIND(COALESCE(?l, STRAFTER(STR(?c),"#"), STRAFTER(STR(?c),"/")) AS ?label)
    }"""
    res = Graph()
    res += g.query(q).graph
    return res

def _construct_hierarchy_local(g: Graph) -> Graph:
    q = PREFIXES + """
    CONSTRUCT {
      ?c a owl:Class ; rdfs:label ?clabel ; rdfs:subClassOf ?p .
      ?p rdfs:label ?plabel .
    } WHERE {
      ?c a owl:Class .
      OPTIONAL { ?c rdfs:label ?cl . FILTER(LANG(?cl) = "" || LANGMATCHES(LANG(?cl),"en")) }
      BIND(COALESCE(?cl, STRAFTER(STR(?c),"#"), STRAFTER(STR(?c),"/")) AS ?clabel)
      OPTIONAL {
        ?c rdfs:subClassOf ?p .
        OPTIONAL { ?p rdfs:label ?pl . FILTER(LANG(?pl) = "" || LANGMATCHES(LANG(?pl),"en")) }
        BIND(COALESCE(?pl, STRAFTER(STR(?p),"#"), STRAFTER(STR(?p),"/")) AS ?plabel)
      }
    }"""
    res = Graph()
    res += g.query(q).graph
    return res

def _construct_tbox_local(g: Graph) -> Graph:
    q = PREFIXES + """
    CONSTRUCT { ?s ?p ?o . } WHERE {
      { ?s a owl:Class . ?s ?p ?o .
        FILTER(?p IN (rdfs:label, rdf:type, rdfs:subClassOf, owl:equivalentClass, owl:disjointWith)) }
      UNION
      { ?s a owl:ObjectProperty . ?s ?p ?o .
        FILTER(?p IN (rdfs:label, rdf:type, rdfs:domain, rdfs:range, owl:inverseOf, owl:equivalentProperty, owl:propertyDisjointWith)) }
      UNION
      { ?s a owl:DatatypeProperty . ?s ?p ?o .
        FILTER(?p IN (rdfs:label, rdf:type, rdfs:domain, rdfs:range, owl:equivalentProperty, owl:propertyDisjointWith)) }
    }"""
    res = Graph()
    res += g.query(q).graph
    return res

def _strip_bnodes(g: Graph) -> List[Tuple]:
    return [(s, p, o) for (s, p, o) in g if not (isinstance(s, BNode) or isinstance(p, BNode) or isinstance(o, BNode))]

# ------------------ Stats + reuse (local) ------------------
def _local_counts(g: Graph) -> Dict[str, int]:
    classes = len({c for c in g.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)})
    objp = len({p for p in g.subjects(RDF.type, OWL.ObjectProperty) if isinstance(p, URIRef)})
    datap = len({p for p in g.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(p, URIRef)})
    inst = len({s for s, _, _ in g.triples((None, RDF.type, None)) if isinstance(s, URIRef)})
    return dict(classes=classes, objectProperties=objp, dataProperties=datap, instances=inst)

def _load_mwo_terms(path: Path) -> Tuple[Set[URIRef], Set[URIRef], Set[URIRef]]:
    if not path.exists():
        return set(), set(), set()
    m = Graph()
    m.parse(path)
    mwo_classes = {c for c in m.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)}
    mwo_objp = {p for p in m.subjects(RDF.type, OWL.ObjectProperty) if isinstance(p, URIRef)}
    mwo_datp = {p for p in m.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(p, URIRef)}
    return mwo_classes, mwo_objp, mwo_datp

def _reused_from_mwo_local(g: Graph, mwo_classes: Set[URIRef], mwo_objp: Set[URIRef], mwo_datp: Set[URIRef]):
    used_cls = set()
    for c in mwo_classes:
        if (c, RDF.type, OWL.Class) in g or next(g.triples((None, RDF.type, c)), None):
            used_cls.add(c)
    used_op = set()
    for p in mwo_objp:
        if (p, RDF.type, OWL.ObjectProperty) in g:
            used_op.add(p)
            continue
        for _ in g.triples((None, p, None)):
            used_op.add(p)
            break
    used_dp = set()
    for p in mwo_datp:
        if (p, RDF.type, OWL.DatatypeProperty) in g:
            used_dp.add(p)
            continue
        for _, o in ((s, o) for s, _, o in g.triples((None, p, None))):
            if not isinstance(o, URIRef):
                used_dp.add(p)
                break
    return used_cls, used_op, used_dp

def _make_stats_comment(cnt: Dict[str, int], used_cls: Set[URIRef], used_op: Set[URIRef], used_dp: Set[URIRef]) -> str:
    """
    Single annotation string; no new predicates.
    Keep it bounded to avoid gigantic literals.
    """
    MAX_LIST = 50

    def _fmt_list(title: str, iris: Set[URIRef]) -> str:
        iris_s = sorted((str(x) for x in iris), key=str)
        if not iris_s:
            return f"{title}: 0"
        head = iris_s[:MAX_LIST]
        more = len(iris_s) - len(head)
        lines = "\n  - ".join([""] + head)
        suffix = f"\n  ... (+{more} more)" if more > 0 else ""
        return f"{title}: {len(iris_s)}{lines}{suffix}"

    return (
        "Local snapshot statistics (computed from imported RDF graphs):\n"
        f"- classes: {cnt['classes']}\n"
        f"- objectProperties: {cnt['objectProperties']}\n"
        f"- dataProperties: {cnt['dataProperties']}\n"
        f"- instances: {cnt['instances']}\n"
        "\n"
        "Reused from MWO (heuristic based on usage in local graph):\n"
        f"- classes: {len(used_cls)}\n"
        f"- objectProperties: {len(used_op)}\n"
        f"- dataProperties: {len(used_dp)}\n"
        "\n"
        + _fmt_list("MWO classes reused", used_cls) + "\n\n"
        + _fmt_list("MWO objectProperties reused", used_op) + "\n\n"
        + _fmt_list("MWO dataProperties reused", used_dp)
    )

# ------------------ Writers ------------------
def _write_named_graph_files(graph_iri: str, g_src: Graph, out_dir: Path, overwrite: bool = False) -> Tuple[Optional[Path], Optional[Path], int]:
    """
    Write <stem>.nq (with GRAPH IRI) and <stem>.ttl (triples only), skipping blank nodes.
    'stem' is a stable hash of graph_iri so filenames remain safe even if graph_iri has slashes.
    """
    triples = _strip_bnodes(g_src)
    stem = _stable_file_stem(graph_iri)
    out_dir.mkdir(parents=True, exist_ok=True)
    nq_path = out_dir / f"{stem}.nq"
    ttl_path = out_dir / f"{stem}.ttl"

    if not triples:
        return None, None, 0

    if (not overwrite) and nq_path.exists() and ttl_path.exists():
        return nq_path, ttl_path, 1

    ds = ConjunctiveGraph()
    ctx = ds.get_context(URIRef(graph_iri))
    for t in triples:
        ctx.add(t)
    ds.serialize(destination=str(nq_path), format="nquads", encoding="utf-8")

    out_g = Graph()
    for t in triples:
        out_g.add(t)
    out_g.serialize(destination=str(ttl_path), format="turtle")

    return nq_path, ttl_path, len(triples)

# ------------------ Single-record fetch helpers ------------------
def _fetch_record_by_doi(doi: str, token: str | None):
    doi_norm = doi.strip().replace("https://doi.org/", "").replace("http://doi.org/", "").replace("doi:", "")
    params = {"q": f'doi:"{doi_norm}"', "size": 1, "all_versions": 1}
    if token:
        params["access_token"] = token
    r = requests.get(ZENODO_API, params=params, timeout=ZENODO_TIMEOUT, headers={"Accept": "application/json"})
    if r.ok:
        hits = r.json().get("hits", {}).get("hits", [])
        return hits[0] if hits else None
    return None

def _fetch_record_by_url(url: str, token: str | None):
    m = re.search(r"/record[s]?/(\d+)", url)
    if not m:
        return None
    rec_id = m.group(1)
    params = {"all_versions": 1}
    if token:
        params["access_token"] = token
    r = requests.get(f"{ZENODO_API}/{rec_id}", params=params, timeout=ZENODO_TIMEOUT, headers={"Accept": "application/json"})
    return r.json() if r.ok else None

# ------------------ CLI ------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--community", default="https://zenodo.org/communities/nfdi-matwerk/", help="Zenodo community slug or URL")
    parser.add_argument("--record-ids", default=None, help="Comma-separated Zenodo record IDs (bypass community fetch)")
    parser.add_argument("--out", default="data/zenodo/zenodo.ttl", help="Path to write RDF output (TriG if named graphs present)")
    parser.add_argument("--format", default="turtle", choices=["turtle", "xml", "json-ld", "nt", "n3", "trig"])
    parser.add_argument("--download-dir", default="data/zenodo/download", help="Directory to download files")
    parser.add_argument("--max", type=int, default=-1, help="Limit number of records processed")

    # Snapshots (deterministic IRIs; no state file)
    parser.add_argument("--make-snapshots", action="store_true", help="Also build classes/classHierarchy/tbox named graphs per record")
    parser.add_argument("--snapshots-dir", default="data/zenodo/named_graphs/record/", help="Root directory for per-record snapshots")
    parser.add_argument("--snapshots-overwrite", action="store_true", help="Overwrite existing snapshot files (default: keep existing)")
    parser.add_argument("--mwo-owl", default=str(MWO_OWL_PATH), help="Path to mwo-full.owl for reuse stats")

    # Optional single-dataset modes
    parser.add_argument("--doi", default=None, help="Process a single DOI (e.g., 10.5281/zenodo.13797439)")
    parser.add_argument("--record-url", default=None, help="Process a single record URL (https://zenodo.org/record/<id>)")

    args = parser.parse_args()

    token = os.environ.get("ZENODO_TOKEN")
    comm_slug = normalize_community(args.community)

    # No JSON state: identifiers must be stable via mint.py (deterministic)
    state: Dict[str, Any] = {}
    session_nodes: Set[str] = set()

    ds = ConjunctiveGraph()
    g = ds.default_context
    ds.bind("nfdi", "https://nfdi.fiz-karlsruhe.de/ontology/")
    ds.bind("obo", "http://purl.obolibrary.org/obo/")

    count = 0
    download_root = Path(args.download_dir).expanduser().resolve() if args.download_dir else None
    if download_root:
        download_root.mkdir(parents=True, exist_ok=True)

    # Choose records iterator
    if args.record_ids:
        ids = [x.strip() for x in args.record_ids.split(",") if x.strip()]
        records_iter = get_records_by_ids(ids, token)
        print(f"Fetching {len(ids)} record(s) by ID: {', '.join(ids)}")
    elif args.doi:
        rec = _fetch_record_by_doi(args.doi, token)
        if not rec:
            print(f"[WARN] Could not resolve DOI: {args.doi}")
            records_iter = []
        else:
            records_iter = [rec]
            print(f"Processing DOI {args.doi}")
    elif args.record_url:
        rec = _fetch_record_by_url(args.record_url, token)
        if not rec:
            print(f"[WARN] Could not fetch record: {args.record_url}")
            records_iter = []
        else:
            records_iter = [rec]
            print(f"Processing record {args.record_url}")
    else:
        records_iter = iter_community_records(comm_slug, token)

    # Load MWO terms once (for reuse stats)
    mwo_classes, mwo_objp, mwo_datp = _load_mwo_terms(Path(args.mwo_owl))

    snapshots_root = Path(args.snapshots_dir)

    for rec in records_iter:
        count += 1
        key = record_key(rec)

        instance_iri = get_or_mint_instance(state, key, session_nodes)
        print(f"[{count}] Record {rec.get('id')} -> instance {instance_iri}")

        # Default graph enrichment for the record (no dcterms/prov here)
        build_record_in_default_graph(g, rec, instance_iri, state, key, session_nodes)

        # Handle files: enrich ALL, and import RDF from direct files and ZIPs
        files = extract_files(rec)
        downloaded_map: Dict[str, List[Tuple[Path, str, Optional[str]]]] = {}
        imported_graphs: List[URIRef] = []

        if download_root:
            rid = rec.get("id")
            dest = download_root / str(rid)

            dl_direct = download_rdf_files(files, dest, token)
            for local_path, content_type, fmeta in dl_direct:
                fkey = fmeta.get("key") or fmeta.get("filename") or local_path.name
                downloaded_map.setdefault(fkey, []).append((local_path, content_type, None))

            dl_zip = download_rdf_and_zip_collect(files, dest, token)
            for local_path, content_type, fmeta, inner_name in dl_zip:
                fkey = fmeta.get("key") or fmeta.get("filename") or local_path.name
                downloaded_map.setdefault(fkey, []).append((local_path, content_type, inner_name))

        for fmeta in files:
            file_key = fmeta.get("key") or fmeta.get("filename") or "file"
            file_link = fmeta.get("link")
            file_uri = URIRef(file_link) if file_link and file_link.startswith("http") else URIRef(f"urn:file:{safe_slug(file_key)}")

            fid = URIRef(get_or_mint_file_identifier(state, key, file_key, session_nodes))

            g.add((fid, URIRef("https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006"), URIRef(to_doi_iri(str(file_uri)))))
            g.add((instance_iri, URIRef("http://purl.obolibrary.org/obo/BFO_0000051"), fid))
            g.add((fid, RDFS.label, Literal(file_key)))

            meta = get_metadata(rec)
            title = meta.get("title")
            doi = meta.get("doi") or rec.get("doi")
            if title:
                g.add((fid, URIRef("http://purl.obolibrary.org/obo/OBI_0002135"), Literal(title)))
            if doi:
                g.add((fid, URIRef("https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006"), URIRef(to_doi_iri(str(doi)))))
                doi_url = (rec.get("links", {}) or {}).get("doi") or (f"https://doi.org/{doi}" if not str(doi).startswith("http") else str(doi))
                g.add((fid, URIRef("http://purl.org/dc/terms/isPartOf"), URIRef(doi_url)))
                g.add((fid, RDFS.seeAlso, URIRef(doi_url)))

            for local_path, content_type, inner_name in downloaded_map.get(file_key, []):
                combined_key = f"{file_key}!{inner_name}" if inner_name else file_key
                graph_iri = get_or_mint_file_graph(state, key, combined_key, session_nodes)

                ok = import_rdf_into_named_graph(ds, local_path, graph_iri, content_type)
                if ok:
                    g.add((fid, RDFS.seeAlso, graph_iri))
                    imported_graphs.append(URIRef(graph_iri))
                else:
                    print(f"  (skipped linking {local_path.name}; parse failed)")

        # ---------- Snapshots (deterministic IRIs; no state) ----------
        if args.make_snapshots:
            g_rec = Graph()
            for ctx_iri in imported_graphs:
                for t in ds.get_context(ctx_iri):
                    g_rec.add(t)

            if len(g_rec) > 0:
                rec_key = record_key(rec)
                folder = snapshots_root / _doi_safe(rec_key)
                folder.mkdir(parents=True, exist_ok=True)

                iri_classes = _stable_snapshot_iri(rec_key, FUNC_CLASSES)
                iri_hier = _stable_snapshot_iri(rec_key, FUNC_CLASS_HIER)
                iri_tbox = _stable_snapshot_iri(rec_key, FUNC_TBOX)

                snap_classes = _construct_classes_local(g_rec)
                snap_hier = _construct_hierarchy_local(g_rec)
                snap_tbox = _construct_tbox_local(g_rec)

                files_written = []
                nq1, ttl1, n1 = _write_named_graph_files(iri_classes, snap_classes, folder, overwrite=args.snapshots_overwrite)
                if n1 > 0:
                    files_written.append((iri_classes, nq1, ttl1))
                nq2, ttl2, n2 = _write_named_graph_files(iri_hier, snap_hier, folder, overwrite=args.snapshots_overwrite)
                if n2 > 0:
                    files_written.append((iri_hier, nq2, ttl2))
                nq3, ttl3, n3 = _write_named_graph_files(iri_tbox, snap_tbox, folder, overwrite=args.snapshots_overwrite)
                if n3 > 0:
                    files_written.append((iri_tbox, nq3, ttl3))

                # Stats + reuse as a simple annotation (rdfs:comment), no new predicates
                cnt = _local_counts(g_rec)
                used_cls, used_op, used_dp = _reused_from_mwo_local(g_rec, mwo_classes, mwo_objp, mwo_datp)
                g.add((instance_iri, RDFS.comment, Literal(_make_stats_comment(cnt, used_cls, used_op, used_dp))))

                # Link snapshots (annotation-friendly)
                for iri, _, _ in files_written:
                    g.add((instance_iri, RDFS.seeAlso, URIRef(iri)))

        if 0 < args.max <= count:
            break

    # If named graphs exist, use TriG
    has_named = any(1 for ctx in ds.contexts() if ctx.identifier != g.identifier)
    if args.format.lower() != "trig" and has_named:
        print("Note: Dataset contains named graphs. Switching to TriG.")
        args.format = "trig"

    normalize_graph_uris(ds)

    print(f"Serializing {count} records to {args.out} as {args.format}…")
    if args.format.lower() == "trig":
        ds.serialize(destination=args.out, format="trig")
    else:
        g.serialize(destination=args.out, format=args.format)

    print("Done.")


if __name__ == "__main__":
    main()
