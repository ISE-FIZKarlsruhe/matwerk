#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from typing import Set, Dict, Any, List, Tuple, Optional
import argparse
import time
import datetime
import uuid
import requests
import re
from urllib.parse import urlsplit, urlunsplit, quote
from .zenodo_api import ZENODO_API, ZENODO_TIMEOUT

from rdflib import Graph, ConjunctiveGraph, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, OWL, XSD, DCTERMS
from rdflib import Namespace

from .downloads import download_rdf_files, download_rdf_and_zip_collect
from .iri_utils import normalize_graph_uris, safe_slug
from .rdf_builders import build_record_in_default_graph
from .state import load_state, save_state, record_key
from .zenodo_api import best_external_url, normalize_community, iter_community_records, extract_files, get_records_by_ids
from .mint import get_or_mint_instance, get_or_mint_file_graph, get_or_mint_file_identifier
from .rdf_import import import_rdf_into_named_graph
from .zenodo_api import get_metadata

# ------------------ Snapshot config ------------------
BASE_GRAPH_IRI = os.environ.get(
    "BASE_GRAPH_IRI", "https://purls.helmholtz-metadaten.de/msekg/"
).rstrip("/") + "/"

FUNC_CLASSES        = "classes"
FUNC_CLASS_HIER     = "classHierarchy"
FUNC_TBOX           = "tbox"

STAT = URIRef("https://purls.helmholtz-metadaten.de/msekg/stat/")
MWO_OWL_PATH = Path(os.environ.get("MWO_OWL_PATH", "ontology/mwo-full.owl"))
PROV = Namespace("http://www.w3.org/ns/prov#")

# ------------------ Utils for stable per-record graph IRIs ------------------
def _now_ms() -> int:
    return int(time.time() * 1000)

def _iri_ts(iri: str) -> str:
    return iri.rstrip("/").split("/")[-1]

def _normalize_doi(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    s = s.replace("https://doi.org/", "").replace("http://doi.org/", "").replace("doi:", "")
    return s

def _dataset_key_from_record(rec: dict) -> str:
    """Stable key per dataset: canonical DOI (no prefix) if present, else zenodo numeric id."""
    md = rec.get("metadata") or {}
    doi = _normalize_doi(md.get("doi") or rec.get("doi"))
    return doi or str(rec.get("id"))

def _ensure_snapshot_state_shapes(state: Dict[str, Any]) -> Dict[str, Any]:
    s = dict(state) if isinstance(state, dict) else {}
    #s.setdefault("by_record", {})   # { rec_key (doi|id): { func_key: graph_iri, ... } }
    return s

def _dedupe_existing_per_record_state(state: Dict[str, Any]) -> None:
    if not isinstance(state, dict):
        return
    br = state.setdefault("by_record", {})
    # Coerce non-dict entries to empty dicts; do not change values.
    for rec_key, per_rec in list(br.items()):
        if not isinstance(per_rec, dict):
            br[rec_key] = {}


def _get_or_create_snapshot_iri(state: Dict[str, Any], rec_key: str, func_key: str) -> str:
    """
    Stable IRI per (rec_key, func_key). If present in state, reuse it (base-agnostic).
    Only mint a new timestamp if this pair has never been seen before.
    Also guarantees different timestamps across the three functions on first mint.
    """
    state.setdefault("by_record", {})
    per_rec = state["by_record"].setdefault(rec_key, {})

    # Reuse if already present (DO NOT require any specific base)
    iri = per_rec.get(func_key)

    if isinstance(iri, str) and iri:
        return iri

    # First time for this functionality -> mint a unique timestamp not used by siblings
    used_ts = set()
    for existing in per_rec.values():
        if isinstance(existing, str) and existing:
            used_ts.add(_iri_ts(existing))

    ts = _now_ms()
    while str(ts) in used_ts:
        ts += 1

    iri = f"{BASE_GRAPH_IRI}{ts}"
    per_rec[func_key] = iri
    return iri

def _doi_safe(doi_or_id: str) -> str:
    s = doi_or_id.strip().replace("https://doi.org/","").replace("http://doi.org/","").replace("doi:","")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)

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
    return [(s,p,o) for (s,p,o) in g if not (isinstance(s,BNode) or isinstance(p,BNode) or isinstance(o,BNode))]

# ------------------ Stats + reuse (local) ------------------
def _local_counts(g: Graph) -> Dict[str, int]:
    classes = len({c for c in g.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)})
    objp    = len({p for p in g.subjects(RDF.type, OWL.ObjectProperty) if isinstance(p, URIRef)})
    datap   = len({p for p in g.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(p, URIRef)})
    inst    = len({s for s,_,_ in g.triples((None, RDF.type, None)) if isinstance(s, URIRef)})
    return dict(classes=classes, objectProperties=objp, dataProperties=datap, instances=inst)

def _load_mwo_terms(path: Path) -> Tuple[Set[URIRef], Set[URIRef], Set[URIRef]]:
    if not path.exists():
        return set(), set(), set()
    m = Graph(); m.parse(path)
    mwo_classes = {c for c in m.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)}
    mwo_objp    = {p for p in m.subjects(RDF.type, OWL.ObjectProperty) if isinstance(p, URIRef)}
    mwo_datp    = {p for p in m.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(p, URIRef)}
    return mwo_classes, mwo_objp, mwo_datp

def _reused_from_mwo_local(g: Graph, mwo_classes: Set[URIRef], mwo_objp: Set[URIRef], mwo_datp: Set[URIRef]):
    used_cls = set()
    for c in mwo_classes:
        if (c, RDF.type, OWL.Class) in g or next(g.triples((None, RDF.type, c)), None):
            used_cls.add(c)
    used_op = set()
    for p in mwo_objp:
        if (p, RDF.type, OWL.ObjectProperty) in g:
            used_op.add(p); continue
        for _ in g.triples((None, p, None)):
            used_op.add(p); break
    used_dp = set()
    for p in mwo_datp:
        if (p, RDF.type, OWL.DatatypeProperty) in g:
            used_dp.add(p); continue
        for s,o in ((s,o) for s,_,o in g.triples((None, p, None))):
            if not isinstance(o, URIRef):
                used_dp.add(p); break
    return used_cls, used_op, used_dp

# ------------------ Writers ------------------
def _write_named_graph_files(graph_iri: str, g_src: Graph, out_dir: Path, overwrite: bool = False) -> Tuple[Optional[Path], Optional[Path], int]:
    """
    Write <timestamp>.nq (with GRAPH IRI) and <timestamp>.ttl (triples only), skipping blank nodes.
    Return (nq_path, ttl_path, ntriples). If no non-bnode triples → (None, None, 0)
    If overwrite=False and both files already exist, nothing is written and we return their paths with count>0.
    """
    triples = _strip_bnodes(g_src)
    ts = _iri_ts(graph_iri)
    out_dir.mkdir(parents=True, exist_ok=True)
    nq_path = out_dir / f"{ts}.nq"
    ttl_path = out_dir / f"{ts}.ttl"

    if not triples:
        return None, None, 0

    if (not overwrite) and nq_path.exists() and ttl_path.exists():
        # Already materialized for this IRI; keep existing files
        return nq_path, ttl_path, 1

    # N-Quads with named graph IRI
    ds = ConjunctiveGraph()
    ctx = ds.get_context(URIRef(graph_iri))
    for t in triples:
        ctx.add(t)
    ds.serialize(destination=str(nq_path), format="nquads", encoding="utf-8")

    # Turtle (triples only)
    out_g = Graph()
    for t in triples:
        out_g.add(t)
    out_g.serialize(destination=str(ttl_path), format="turtle")

    return nq_path, ttl_path, len(triples)

# ------------------ Single-record fetch helpers ------------------
def _fetch_record_by_doi(doi: str, token: str | None):
    doi_norm = doi.strip().replace("https://doi.org/","").replace("http://doi.org/","").replace("doi:","")
    params = {"q": f'doi:"{doi_norm}"', "size": 1, "all_versions": 1}
    if token: params["access_token"] = token
    r = requests.get(ZENODO_API, params=params, timeout=ZENODO_TIMEOUT, headers={"Accept":"application/json"})
    if r.ok:
        hits = r.json().get("hits", {}).get("hits", [])
        return hits[0] if hits else None
    return None

def _fetch_record_by_url(url: str, token: str | None):
    m = re.search(r"/record[s]?/(\d+)", url)
    if not m: return None
    rec_id = m.group(1)
    params = {"all_versions": 1}
    if token: params["access_token"] = token
    r = requests.get(f"{ZENODO_API}/{rec_id}", params=params, timeout=ZENODO_TIMEOUT, headers={"Accept":"application/json"})
    return r.json() if r.ok else None

def to_doi_iri(s: str) -> str:
    
    s = urlsplit(s)
    path = quote(s.path, safe="/:@!$&'()*+,;=~-._")
    query = quote(s.query, safe="=&:@!$'()*+,;=~-._")
    frag  = quote(s.fragment, safe=":@!$&'()*+,;=~-._")
    return urlunsplit((s.scheme, s.netloc, path, query, frag))

# ------------------ CLI ------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--community", default="https://zenodo.org/communities/nfdi-matwerk/", help="Zenodo community slug or URL")
    parser.add_argument("--record-ids", default=None, help="Comma-separated Zenodo record IDs (bypass community fetch)")
    parser.add_argument("--out", default="data/zenodo/zenodo.ttl", help="Path to write RDF output (TriG if named graphs present)")
    parser.add_argument("--format", default="turtle", choices=["turtle", "xml", "json-ld", "nt", "n3", "trig"])
    parser.add_argument("--download-dir", default="data/zenodo/download", help="Directory to download files")
    parser.add_argument("--max", type=int, default=-1, help="Limit number of records processed")
    parser.add_argument("--state-file", default="data/zenodo/.graph_ids.json", help="JSON file to persist IRIs")

    # NEW: one-run snapshots
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

    state_path = Path(args.state_file)
    state = _ensure_snapshot_state_shapes(load_state(state_path))
    _dedupe_existing_per_record_state(state)

    ds = ConjunctiveGraph()
    g = ds.default_context
    ds.bind("nfdi", "https://nfdi.fiz-karlsruhe.de/ontology/")
    ds.bind("obo", "http://purl.obolibrary.org/obo/")
    ds.bind("dcterms", "http://purl.org/dc/terms/")
    ds.bind("stat", str(STAT))
    
    ds.bind("prov", str(PROV))

    run_id = os.environ.get("RUN_ID") or uuid.uuid4().hex
    run_iri = URIRef(f"https://purls.helmholtz-metadaten.de/msekg/{run_id}")
    run_time = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    g.add((run_iri, RDF.type, PROV.Activity))
    g.add((run_iri, PROV.startedAtTime, Literal(run_time, datatype=XSD.dateTime)))

    git_sha = os.environ.get("GIT_SHA")
    if git_sha:
        g.add((run_iri, DCTERMS.identifier, Literal(git_sha)))


    count = 0
    download_root = Path(args.download_dir).expanduser().resolve() if args.download_dir else None
    if download_root:
        download_root.mkdir(parents=True, exist_ok=True)

    session_nodes: Set[str] = set()

    # Choose records iterator
    records_iter = None
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

        # Default graph enrichment for the record
        build_record_in_default_graph(g, rec, instance_iri, state, key, session_nodes)
        
        # --- Provenance: source + created/updated (Zenodo) + run ---
        src_url = best_external_url(rec) or (f"https://zenodo.org/records/{rec.get('id')}" if rec.get("id") else None)
        if src_url:
            g.add((instance_iri, DCTERMS.source, URIRef(src_url)))
            g.add((instance_iri, PROV.wasDerivedFrom, URIRef(src_url)))

        if str(instance_iri) in session_nodes:
            #g.add((instance_iri, PROV.wasGeneratedBy, run_iri))
            g.add((instance_iri, PROV.generatedAtTime, Literal(run_time, datatype=XSD.dateTime)))


        # Zenodo timestamps:
        # IMPORTANT: get_metadata(rec) returns rec["metadata"] only
        meta = get_metadata(rec)

        created_at = rec.get("created") or meta.get("publication_date")
        updated_at = rec.get("updated") or rec.get("modified") or rec.get("updated_at")  # payload variants

        if created_at:
            # If created_at is YYYY-MM-DD, this is technically xsd:date; keep as dateTime only if it includes time.
            dtype = XSD.dateTime if "T" in str(created_at) else XSD.date
            g.add((instance_iri, DCTERMS.created, Literal(created_at, datatype=dtype)))

        if updated_at:
            dtype = XSD.dateTime if "T" in str(updated_at) else XSD.date
            g.add((instance_iri, DCTERMS.modified, Literal(updated_at, datatype=dtype)))


        # Handle files: enrich ALL, and import RDF from direct files and ZIPs
        files = extract_files(rec)
        downloaded_map = {}  # file_key -> list of (local_path, content_type, inner_name)
        imported_graphs: List[URIRef] = []

        if download_root:
            rid = rec.get("id")
            dest = download_root / str(rid)
            # Direct RDF
            dl_direct = download_rdf_files(files, dest, token)
            for local_path, content_type, fmeta in dl_direct:
                fkey = fmeta.get("key") or fmeta.get("filename") or local_path.name
                downloaded_map.setdefault(fkey, []).append((local_path, content_type, None))
            # ZIP + inner RDF
            dl_zip = download_rdf_and_zip_collect(files, dest, token)
            for local_path, content_type, fmeta, inner_name in dl_zip:
                fkey = fmeta.get("key") or fmeta.get("filename") or local_path.name
                downloaded_map.setdefault(fkey, []).append((local_path, content_type, inner_name))

        # Enrich all files + import into named graphs
        for fmeta in files:
            file_key = fmeta.get("key") or fmeta.get("filename") or "file"
            file_link = fmeta.get("link")
            file_uri = URIRef(file_link) if file_link and file_link.startswith("http") else URIRef(f"urn:file:{safe_slug(file_key)}")

            fid = URIRef(get_or_mint_file_identifier(state, key, file_key, session_nodes))

            # Add run provenance only if this fid was minted in THIS run
            if str(fid) in session_nodes:
                #g.add((fid, PROV.wasGeneratedBy, run_iri))
                g.add((fid, PROV.generatedAtTime, Literal(run_time, datatype=XSD.dateTime)))

            g.add((fid, URIRef("https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006"), URIRef(to_doi_iri(file_uri))))
            g.add((instance_iri, URIRef("http://purl.obolibrary.org/obo/BFO_0000051"), fid))
            g.add((fid, RDFS.label, Literal(file_key)))

            meta = get_metadata(rec)
            title = meta.get("title")
            doi = meta.get("doi") or rec.get("doi")
            if title:
                g.add((fid, URIRef("http://purl.obolibrary.org/obo/OBI_0002135"), Literal(title)))
            if doi:
                g.add((fid, URIRef("https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006"), URIRef(to_doi_iri(doi))))
                doi_url = (rec.get("links", {}) or {}).get("doi") or (f"https://doi.org/{doi}" if not str(doi).startswith("http") else str(doi))
                g.add((fid, URIRef("http://purl.org/dc/terms/isPartOf"), URIRef(doi_url)))
                g.add((fid, RDFS.seeAlso, URIRef(doi_url)))

            # link/import if we downloaded RDF for this file
            for local_path, content_type, inner_name in downloaded_map.get(file_key, []):
                combined_key = f"{file_key}!{inner_name}" if inner_name else file_key
                graph_iri = get_or_mint_file_graph(state, key, combined_key, session_nodes)

                # Add run provenance only if this graph IRI was minted in THIS run
                if str(graph_iri) in session_nodes:
                    #g.add((URIRef(graph_iri), PROV.wasGeneratedBy, run_iri))
                    g.add((URIRef(graph_iri), PROV.generatedAtTime, Literal(run_time, datatype=XSD.dateTime)))

                ok = import_rdf_into_named_graph(ds, local_path, graph_iri, content_type)
                if ok:
                    g.add((fid, RDFS.seeAlso, graph_iri))
                    imported_graphs.append(URIRef(graph_iri))
                else:
                    print(f"  (skipped linking {local_path.name}; parse failed)")

        # ---------- Snapshots in the same run (no re-download) ----------
        if args.make_snapshots:
            # Merge only this record's imported graphs into a temporary local graph
            g_rec = Graph()
            for ctx_iri in imported_graphs:
                for t in ds.get_context(ctx_iri):
                    g_rec.add(t)

            if len(g_rec) > 0:
                rec_key = _dataset_key_from_record(rec)            # canonical DOI or id
                folder = snapshots_root / _doi_safe(rec_key)
                folder.mkdir(parents=True, exist_ok=True)

                # Mint/reuse stable snapshot IRIs (per record + functionality)
                iri_classes = _get_or_create_snapshot_iri(state, rec_key, FUNC_CLASSES)
                iri_hier    = _get_or_create_snapshot_iri(state, rec_key, FUNC_CLASS_HIER)
                iri_tbox    = _get_or_create_snapshot_iri(state, rec_key, FUNC_TBOX)

                # Build snapshots from local graph
                snap_classes = _construct_classes_local(g_rec)
                snap_hier    = _construct_hierarchy_local(g_rec)
                snap_tbox    = _construct_tbox_local(g_rec)

                # Write only if non-empty after removing bnodes; skip rewrite unless --snapshots-overwrite
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

                # Stats + reuse (local)
                cnt = _local_counts(g_rec)
                used_cls, used_op, used_dp = _reused_from_mwo_local(g_rec, mwo_classes, mwo_objp, mwo_datp)

                # Annotate the dataset instance in default graph
                g.add((instance_iri, STAT + "numberOfClasses",            Literal(cnt["classes"], datatype=XSD.integer)))
                g.add((instance_iri, STAT + "numberOfObjectProperties",   Literal(cnt["objectProperties"], datatype=XSD.integer)))
                g.add((instance_iri, STAT + "numberOfDataProperties",     Literal(cnt["dataProperties"], datatype=XSD.integer)))
                g.add((instance_iri, STAT + "numberOfInstances",          Literal(cnt["instances"], datatype=XSD.integer)))

                g.add((instance_iri, STAT + "numberOfClassesReusedFromMWO",        Literal(len(used_cls), datatype=XSD.integer)))
                g.add((instance_iri, STAT + "numberOfObjectPropertiesReusedFromMWO", Literal(len(used_op), datatype=XSD.integer)))
                g.add((instance_iri, STAT + "numberOfDataPropertiesReusedFromMWO",   Literal(len(used_dp), datatype=XSD.integer)))

                for x in sorted(used_cls, key=str):
                    g.add((instance_iri, STAT + "classReusedFromMWO", x))
                for x in sorted(used_op, key=str):
                    g.add((instance_iri, STAT + "objectPropertyReusedFromMWO", x))
                for x in sorted(used_dp, key=str):
                    g.add((instance_iri, STAT + "dataPropertyReusedFromMWO", x))

                for iri, nq_path, ttl_path in files_written:
                    g.add((instance_iri, RDFS.seeAlso, URIRef(iri)))
                    #if nq_path:
                    #    g.add((instance_iri, STAT + "graphFileNQ", Literal(str(nq_path), datatype=XSD.string)))
                    #if ttl_path:
                    #    g.add((instance_iri, STAT + "graphFileTTL", Literal(str(ttl_path), datatype=XSD.string)))

        if 0 < args.max <= count:
            break

    save_state(state_path, state)

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

