#!/usr/bin/env python3
"""
Simplified fast fetcher:
- Discovers SPARQL endpoints from data/all.ttl (strict ENDPOINT_PREDICATE).
- Fetches the whole default graph of each endpoint via paged CONSTRUCT.
- Streams results directly to a single N-Quads file, one named graph per endpoint.
- Updates data/all.ttl by adding rdfs:seeAlso (or HAS_GRAPH_PRED) triples pointing to each new graph IRI.
- Maintains a JSON history endpoint -> [graph_iri, ...]

Speed-ups vs original:
- Requests application/n-triples with gzip to reduce transfer and parsing cost.
- Writes NT -> NQ on the fly without creating big rdflib graphs.
- Larger default page size, no artificial sleeps, fewer round trips.
"""

import json
import os
import time
import tempfile
from pathlib import Path
import subprocess

from rdflib import Graph, URIRef, RDF
from rdflib.term import URIRef as RDFURIRef, Literal as RDFLiteral
from SPARQLWrapper import SPARQLWrapper, GET, POST

# ------------------ Config (env-overridable) ------------------
BASE_GRAPH_IRI = os.environ.get("BASE_GRAPH_IRI", "https://purls.helmholtz-metadaten.de/msekg/g/").rstrip("/") + "/"

ENDPOINT_CLASS = URIRef(os.environ.get(
    "ENDPOINT_CLASS",
    "http://purls.helmholtz-metadaten.de/mwo/MWO_0001060"
))

ENDPOINT_PREDICATE = URIRef(os.environ.get(
    "ENDPOINT_PREDICATE",
    "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008"
))

HAS_GRAPH_PRED = URIRef(os.environ.get(
    "HAS_GRAPH_PRED",
    "http://www.w3.org/2000/01/rdf-schema#seeAlso"
))

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "1000"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))

STATE_JSON = os.environ.get("STATE_JSON", "data/sparql_endpoints/sparql_sources.json")   # endpoint -> [graph_iri,...]
OUT_NQUADS = os.environ.get("OUT_NQUADS", "data/sparql_endpoints/endpoints.nq")          # merged quads across endpoints
ALL_TTL = os.environ.get("ALL_TTL", "data/all.ttl")

# ------------------ Helpers ------------------
def as_http_url_str(obj):
    if isinstance(obj, RDFURIRef):
        u = str(obj)
        if u.startswith("http://") or u.startswith("https://"):
            return u
    if isinstance(obj, RDFLiteral):
        s = str(obj)
        if s.startswith("http://") or s.startswith("https://"):
            return s
    return None

def load_state(path):
    p = Path(path)
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for k, v in list(data.items()):
                    if isinstance(v, str):
                        data[k] = [v]
                    elif not isinstance(v, list):
                        data[k] = []
                return data
        except Exception:
            pass
    return {}

def save_state(path, data):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(path).with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(Path(path))

def next_unique_graph_iri(used_iris: set, history: dict) -> str:
    now_ms = int(time.time() * 1000)
    hist_set = set()
    for lst in history.values():
        if isinstance(lst, list):
            hist_set.update(lst)
        elif isinstance(lst, str):
            hist_set.add(lst)
    ts = now_ms
    while True:
        candidate = f"{BASE_GRAPH_IRI}{ts}"
        if (candidate not in used_iris) and (candidate not in hist_set):
            return candidate
        ts += 1

def collect_endpoints_with_subjects(all_ttl_path):
    """
    Return list[(subject_iri, endpoint_url_str)]. Only strict ENDPOINT_PREDICATE values that are http(s) URLs.
    """
    g = Graph()
    g.parse(all_ttl_path, format="turtle")
    pairs = []
    for s in g.subjects(RDF.type, ENDPOINT_CLASS):
        for v in g.objects(s, ENDPOINT_PREDICATE):
            u = as_http_url_str(v)
            if u:
                pairs.append((s, u))

    # de-duplicate
    seen = set()
    uniq = []
    for s, u in pairs:
        key = (str(s), u)
        if key not in seen:
            seen.add(key)
            uniq.append((s, u))
    return uniq

# ------------------ Fetching ------------------
def make_construct_query(offset: int, limit: int) -> str:
    # Simple, portable pagination. For maximum speed on huge datasets, migrate to keyset pagination.
    return f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ ?s ?p ?o }} OFFSET {offset} LIMIT {limit}"

def fetch_nt_page(endpoint_url: str, query: str, timeout: int) -> bytes:
    """
    Try SPARQLWrapper (GET then POST) requesting N-Triples + gzip.
    Fallback to curl if needed. Returns raw bytes (NT) or b'' on empty.
    """
    sw = SPARQLWrapper(endpoint_url)
    sw.setTimeout(timeout)
    sw.addCustomHttpHeader("Accept", "application/n-triples")
    sw.addCustomHttpHeader("Accept-Encoding", "gzip")
    for method in (GET, POST):
        try:
            sw.setMethod(method)
            sw.setQuery(query)
            # read() gives us raw, avoiding extra parsing work
            resp = sw.query().response
            return resp.read()
        except Exception:
            continue

    # curl fallback
    try:
        res = subprocess.run(
            [
                "curl", "-sS", "-L", "--compressed",
                "--max-time", str(timeout),
                "-H", "Accept: application/n-triples",
                "-H", "Content-Type: application/sparql-query",
                "--data-binary", query,
                endpoint_url,
            ],
            check=False,
            capture_output=True,
        )
        if res.returncode != 0:
            return b""
        return res.stdout or b""
    except FileNotFoundError:
        return b""

def stream_construct_nt(endpoint_url: str, page_size: int, timeout: int):
    """
    Yield N-Triples text for each page until empty.
    """
    offset = 0
    while True:
        q = make_construct_query(offset, page_size)
        raw = fetch_nt_page(endpoint_url, q, timeout=timeout)
        if not raw:
            break
        text = raw.decode("utf-8", errors="replace").strip()
        if not text:
            break
        # Guard against HTML error pages
        if text.lower().startswith("<!doctype html") or text.lower().startswith("<html"):
            if offset == 0:
                raise RuntimeError(f"Endpoint {endpoint_url} returned HTML (not RDF).")
            break
        yield text
        offset += page_size

# ------------------ Writing ------------------
def ensure_parent(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def append_nt_as_nq(nt_text: str, graph_iri: str, out_path: str):
    """
    Convert NT lines to NQ by inserting the graph IRI before the final period.
    Skips comments/blank lines.
    """
    ensure_parent(out_path)
    with open(out_path, "a", encoding="utf-8") as f:
        for line in nt_text.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            # Heuristic: valid N-Triples statement ends with ' .'
            if s.endswith(" ."):
                # Insert graph IRI before final ' .'
                f.write(f"{s[:-2]} <{graph_iri}> .\n")

def safe_overwrite_ttl(path: str, graph: Graph):
    p = Path(path)
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".ttl", encoding="utf-8") as tf:
        tmp_name = tf.name
    graph.serialize(destination=tmp_name, format="turtle")
    Path(tmp_name).replace(p)

# ------------------ Main ------------------
def main():
    pairs = collect_endpoints_with_subjects(ALL_TTL)
    if not pairs:
        print("No endpoints found in all.ttl.")
        return

    print("Found pairs: ", pairs)

    history = load_state(STATE_JSON)
    used_this_run = set()
    summary = []

    # Prepare output file (truncate for a fresh run)
    ensure_parent(OUT_NQUADS)
    Path(OUT_NQUADS).write_text("", encoding="utf-8")

    MAX_TRIPLES = 2000  # stop after this many per endpoint

    for subj_iri, ep in pairs:
        ep = ep.strip()
        print(f"Fetching from endpoint: {ep}")
        graph_iri = next_unique_graph_iri(used_this_run, history)
        used_this_run.add(graph_iri)

        lst = history.get(ep, [])
        if isinstance(lst, str):
            lst = [lst]
        lst.append(graph_iri)
        history[ep] = lst

        total_written = 0
        try:
            for nt_chunk in stream_construct_nt(ep, PAGE_SIZE, REQUEST_TIMEOUT):
                append_nt_as_nq(nt_chunk, graph_iri, OUT_NQUADS)
                triples_in_chunk = sum(
                    1 for L in nt_chunk.splitlines()
                    if L.strip().endswith(" .") and not L.lstrip().startswith("#")
                )
                total_written += triples_in_chunk

                if total_written >= MAX_TRIPLES:
                    print(f"Reached test limit ({MAX_TRIPLES} triples), stopping early for {ep}")
                    break
        except Exception as e:
            print(f"WARNING: Failed to fetch from {ep}: {e}")

        summary.append({
            "subject": str(subj_iri),
            "endpoint": ep,
            "graph_iri": graph_iri,
            "triples_added_estimate": total_written
        })
        print(f"[{ep}] -> {graph_iri} (~{total_written} triples)")


    # Save state and summary
    save_state(STATE_JSON, history)
    Path("/data/sparql_sources_list.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    # Update all.ttl with graph pointers
    try:
        g_all = Graph()
        g_all.parse(ALL_TTL, format="turtle")

        # endpoint -> subjects
        ep_to_subjects = {}
        for s, ep in pairs:
            ep_to_subjects.setdefault(ep, set()).add(s)

        added = 0
        for item in summary:
            ep = item["endpoint"]
            graph_iri = item["graph_iri"]
            for s in ep_to_subjects.get(ep, set()):
                t = (s, HAS_GRAPH_PRED, URIRef(graph_iri))
                if t not in g_all:
                    g_all.add(t)
                    added += 1

        safe_overwrite_ttl(ALL_TTL, g_all)
        print(f"Updated {ALL_TTL} with {added} seeAlso link(s).")
    except Exception as e:
        print(f"WARNING: Failed to update {ALL_TTL}: {e}")

    print(f"Wrote quads: {OUT_NQUADS}")
    print(f"Mapping JSON (history): {STATE_JSON}")
    print("Done.")

if __name__ == "__main__":
    main()
