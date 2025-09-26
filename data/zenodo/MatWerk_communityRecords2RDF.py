#!/usr/bin/env python3
"""
Export all records from a Zenodo community to RDF using MWO ontology
and (optionally) download all files from each record), with STABLE named-graph IRIs.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, quote

import requests
import hashlib

# Import from rdflib submodules to avoid local shadowing issues
from rdflib.graph import ConjunctiveGraph, Graph
from rdflib.term import Literal, URIRef
from rdflib.namespace import RDF, RDFS

ZENODO_API = "https://zenodo.org/api/records"
DEFAULT_PAGE_SIZE = 100

MSEKG_BASE = "https://purls.helmholtz-metadaten.de/msekg/"

# ------------------------
# IRI/URI helpers
# ------------------------
_UNSAFE_HTTP_CHARS = re.compile(r"[^\w\-\.\~:/\?\#\[\]@!\$&'\(\)\*\+,;=%]|\s")

def iri_to_uri(u: str) -> str:
    """Percent-encode an IRI so rdflib accepts it as a URI (RFC 3986)."""
    p = urlparse(u)
    path = quote(p.path, safe="/%:@!$&'*+,;=")
    query = quote(p.query, safe="=&%:@!$'(),*+;$/?")
    fragment = quote(p.fragment, safe="")
    return urlunparse((p.scheme, p.netloc, path, p.params, query, fragment))

def _needs_encoding(u: str) -> bool:
    return (u.startswith("http://") or u.startswith("https://")) and bool(_UNSAFE_HTTP_CHARS.search(u))

def normalize_graph_uris(g: ConjunctiveGraph) -> None:
    """Scan all triples (in all contexts) and percent-encode bad HTTP IRIs."""
    to_add: List[Tuple[Graph, Tuple]] = []
    to_remove: List[Tuple[Graph, Tuple]] = []
    for ctx in g.contexts():
        for s, p, o in ctx.triples((None, None, None)):
            s2 = URIRef(iri_to_uri(str(s))) if isinstance(s, URIRef) and _needs_encoding(str(s)) else s
            p2 = URIRef(iri_to_uri(str(p))) if isinstance(p, URIRef) and _needs_encoding(str(p)) else p
            if isinstance(o, URIRef) and _needs_encoding(str(o)):
                o2 = URIRef(iri_to_uri(str(o)))
            else:
                o2 = o
            if (s2, p2, o2) != (s, p, o):
                to_remove.append((ctx, (s, p, o)))
                to_add.append((ctx, (s2, p2, o2)))
    for ctx, t in to_remove:
        ctx.remove(t)
    for ctx, t in to_add:
        ctx.add(t)

def safe_slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"^doi:\s*", "", s)
    s = s.replace("/", "_")
    s = re.sub(r"[^a-z0-9._-]+", "", s)
    return s or "id"

def safe_uri_segment(s: str) -> str:
    return quote(s, safe="._-")

# ------------------------
# State (stable graph IDs)
# ------------------------
def load_state(path: Path) -> Dict[str, str]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def save_state(path: Path, state: Dict[str, str]) -> None:
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

def record_key(rec: dict) -> str:
    """Stable key to look up graph IRI in state."""
    # Prefer conceptrecid; fall back to recid
    return str(rec.get("conceptrecid") or rec.get("id"))

def get_doi(rec: dict) -> Optional[str]:
    meta = rec.get("metadata") or {}
    return meta.get("doi") or rec.get("doi")

def best_external_url(rec: dict) -> Optional[str]:
    links = rec.get("links", {}) or {}
    return links.get("doi") or links.get("record_html") or links.get("self") or (f"https://zenodo.org/records/{rec.get('id')}" if rec.get("id") else None)

def deterministic_graph_iri(rec: dict, scheme: str = "auto") -> URIRef:
    """
    Build a deterministic graph IRI (no timestamps).
    Schemes: auto | doi | concept | recid | hash
    """
    doi = get_doi(rec)
    concept = rec.get("conceptrecid")
    recid = rec.get("id")

    if scheme == "doi":
        if doi: return URIRef(MSEKG_BASE + "g/doi/" + safe_uri_segment(safe_slug(doi)))
    elif scheme == "concept":
        if concept: return URIRef(MSEKG_BASE + "g/zenodo-concept/" + safe_uri_segment(str(concept)))
    elif scheme == "recid":
        if recid: return URIRef(MSEKG_BASE + "g/zenodo/" + safe_uri_segment(str(recid)))
    elif scheme == "hash":
        ext = best_external_url(rec) or f"zenodo:{recid or concept or ''}"
        h = hashlib.sha256(ext.encode("utf-8")).hexdigest()[:16]
        return URIRef(MSEKG_BASE + "g/h/" + h)

    # todo: create unique instance for each, add more metadata
    if doi:
        return URIRef(MSEKG_BASE + "g/doi/" + safe_uri_segment(safe_slug(doi)))
    if concept:
        return URIRef(MSEKG_BASE + "g/zenodo-concept/" + safe_uri_segment(str(concept)))
    if recid:
        return URIRef(MSEKG_BASE + "g/zenodo/" + safe_uri_segment(str(recid)))
    # last resort: stable hash of best external URL
    ext = best_external_url(rec) or "zenodo:unknown"
    h = hashlib.sha256(ext.encode("utf-8")).hexdigest()[:16]
    return URIRef(MSEKG_BASE + "g/h/" + h)

# ------------------------
# Zenodo API
# ------------------------
def zenodo_request(url: str, params: Optional[dict] = None, token: Optional[str] = None) -> dict:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    r = requests.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

def normalize_community(community: str) -> str:
    """Accept either a slug (nfdi-matwerk) or a full Zenodo community URL and return the slug."""
    c = community.strip()
    if c.startswith("http://") or c.startswith("https://"):
        path = urlparse(c).path.rstrip("/")
        parts = [p for p in path.split("/") if p]
        if parts and parts[-1] != "communities":
            return parts[-1]
    return c

def iter_community_records(community: str, token: Optional[str]) -> Iterable[dict]:
    page = 1
    total = None
    while True:
        params = {
            "communities": community,
            "size": DEFAULT_PAGE_SIZE,
            "page": page,
            "sort": "mostrecent",
            "all_versions": 1,
        }
        data = zenodo_request(ZENODO_API, params=params, token=token)

        hits = data.get("hits", {}).get("hits", [])
        if total is None:
            raw_total = data.get("hits", {}).get("total")
            total = raw_total.get("value") if isinstance(raw_total, dict) else raw_total
            print(f"Found ~{total} records in community '{community}'.")

        if not hits:
            break

        for rec in hits:
            yield rec

        page += 1
        time.sleep(0.3)

# ------------------------
# RDF helpers
# ------------------------
def safe_uri(s: str) -> URIRef:
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^\w\-\.]+", "", s)
    return URIRef(s)

def record_best_external_uri(rec: dict) -> Optional[URIRef]:
    links = rec.get("links", {}) or {}
    doi_url = links.get("doi")
    if doi_url:
        return URIRef(iri_to_uri(doi_url))
    rec_html = links.get("record_html") or links.get("self")
    if rec_html:
        return URIRef(iri_to_uri(rec_html))
    recid = rec.get("id")
    if recid:
        return URIRef(f"https://zenodo.org/records/{recid}")
    return None

def get_metadata(rec: dict) -> dict:
    return rec.get("metadata", rec)

def extract_files(rec: dict) -> List[Dict[str, str]]:
    files: List[Dict[str, str]] = []
    if isinstance(rec.get("files"), list):
        for f in rec["files"]:
            if not isinstance(f, dict):
                continue
            link = f.get("links", {}).get("self") or f.get("links", {}).get("download") or f.get("links", {}).get("content")
            files.append({
                "key": f.get("key") or f.get("filename"),
                "checksum": f.get("checksum"),
                "link": link,
            })
    elif isinstance(rec.get("files"), dict):
        entries = rec["files"].get("entries")
        if isinstance(entries, dict):
            entries = entries.values()
        if isinstance(entries, list):
            for f in entries:
                if not isinstance(f, dict):
                    continue
                links = f.get("links", {}) or {}
                link = links.get("content") or links.get("self")
                files.append({
                    "key": f.get("key") or f.get("id") or f.get("filename"),
                    "checksum": f.get("checksum"),
                    "link": link,
                })
    return files

def build_mwo_graph_for_record(ctx: Graph, rec: dict, dataset_uri: URIRef) -> None:
    meta = get_metadata(rec)
    subj = dataset_uri
    ctx.add((subj, RDF.type, URIRef("https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009")))

    title = meta.get("title")
    if title:
        ctx.add((subj, URIRef("http://purl.obolibrary.org/obo/IAO_0000235"), Literal(title)))


    doi = meta.get("doi") or rec.get("doi")
    if doi:
        ctx.add((subj, URIRef("https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006"), Literal(doi)))


    files = extract_files(rec)
    for f in files:
        file_id = f.get("link") or f.get("key") or "file"
        if file_id and isinstance(file_id, str) and file_id.startswith("http"):
            file_uri = URIRef(iri_to_uri(file_id))
        else:
            file_uri = URIRef(f"urn:file:{safe_slug(str(file_id))}")
        ctx.add((file_uri, RDF.type, URIRef("https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009")))
        ctx.add((file_uri, URIRef("http://purl.obolibrary.org/obo/BFO_0000051"), subj))

# ------------------------
# Downloads
# ------------------------
def have_zenodo_get() -> bool:
    return shutil.which("zenodo_get") is not None

def download_with_zenodo_get(identifier: str, dest: Path, token: Optional[str]) -> None:
    cmd = ["zenodo_get", "--dest", str(dest), identifier]
    env = os.environ.copy()
    if token:
        env["ZENODO_TOKEN"] = token
    subprocess.run(cmd, check=False, env=env)

def download_files_direct(files: List[Dict[str, str]], dest: Path, token: Optional[str]) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    for f in files:
        url = f.get("link")
        name = f.get("key") or (Path(url).name if url else None)
        if not url or not name:
            continue
        outpath = dest / name
        try:
            with requests.get(url, headers=headers, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(outpath, "wb") as fp:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            fp.write(chunk)
        except Exception as e:
            print(f"  ! Failed to download {url}: {e}")

# ------------------------
# Main
# ------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--community", default="https://zenodo.org/communities/nfdi-matwerk/", help="Zenodo community slug or URL")
    parser.add_argument("--out", default="zenodo.ttl", help="Path to write RDF output")
    parser.add_argument("--format", default="turtle", choices=["turtle", "xml", "json-ld", "nt", "n3", "trig"])
    parser.add_argument("--download-dir", default=None, help="Directory to download files")
    parser.add_argument("--max", type=int, default=-1, help="Limit number of records processed")
    parser.add_argument("--state-file", default=".graph_ids.json", help="JSON file to persist graph IRI mapping")
    parser.add_argument("--graph-id-scheme", default="auto", choices=["auto", "doi", "concept", "recid", "hash"],
                        help="How to generate deterministic graph IRIs")
    args = parser.parse_args()

    token = os.environ.get("ZENODO_TOKEN")
    comm_slug = normalize_community(args.community)

    state_path = Path(args.state_file)
    state = load_state(state_path)

    g = ConjunctiveGraph()
    # Bind prefixes for readability
    g.bind("mwo", "http://purls.helmholtz-metadaten.de/mwo/")
    g.bind("obo", "http://purl.obolibrary.org/obo/")
    g.bind("nfdi", "https://nfdi.fiz-karlsruhe.de/ontology/")

    count = 0
    download_root = Path(args.download_dir).expanduser().resolve() if args.download_dir else None
    if download_root:
        download_root.mkdir(parents=True, exist_ok=True)
        use_zenodo_get = have_zenodo_get()
        print("Using zenodo_get for downloads…" if use_zenodo_get else "zenodo_get not found; attempting direct downloads…")
    else:
        use_zenodo_get = False

    for rec in iter_community_records(comm_slug, token):
        count += 1
        key = record_key(rec)

        # Stable graph IRI: use state if present, else generate + persist
        if key in state:
            graph_iri_str = state[key]
        else:
            graph_iri_str = str(deterministic_graph_iri(rec, scheme=args.graph_id_scheme))
            state[key] = graph_iri_str

        graph_iri = URIRef(graph_iri_str)
        ctx = g.get_context(graph_iri)
        print(f"[{count}] Record {rec.get('id')} -> named graph {graph_iri}")

        build_mwo_graph_for_record(ctx, rec, graph_iri)

        if download_root:
            rid = rec.get("id")
            dest = download_root / str(rid)
            dest.mkdir(parents=True, exist_ok=True)
            if use_zenodo_get:
                identifier = (rec.get("links", {}) or {}).get("doi") or (rec.get("links", {}) or {}).get("record_html") or f"https://zenodo.org/records/{rid}"
                download_with_zenodo_get(identifier, dest, token)
            else:
                files = extract_files(rec)
                if files:
                    download_files_direct(files, dest, token)
                else:
                    print("  (no file links; skipping)")

        if 0 < args.max <= count:
            break

    # Persist mapping so graph IRIs stay stable next runs
    save_state(state_path, state)

    print(f"Serializing {count} records to {args.out} as {args.format}…")
    if args.format.lower() != "trig":
        print("Note: Named graphs require TriG/N-Quads. Switching to TriG.")
        args.format = "trig"

    # Safety pass: ensure all HTTP IRIs are RFC-3986 encoded before writing
    normalize_graph_uris(g)

    g.serialize(destination=args.out, format=args.format)
    print("Done.")

if __name__ == "__main__":
    main()
