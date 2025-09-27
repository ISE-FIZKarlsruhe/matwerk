#!/usr/bin/env python3
"""
Fetch triples from SPARQL endpoints declared in data/all.ttl, store each into a
named graph (N-Quads), and overwrite data/all.ttl to add pointers from the
endpoint individuals to the created graph IRIs.

Environment overrides:
  BASE_GRAPH_IRI       default "https://purls.helmholtz-metadaten.de/msekg/g/"
  MWO_ENDPOINT_CLASS   default "http://purls.helmholtz-metadaten.de/mwo/MWO_0001060"
  ENDPOINT_PREDICATE   default "https://purls.helmholtz-metadaten.de/nfdicore/NFDI_0001008"
  HAS_GRAPH_PRED       default "http://www.w3.org/2000/01/rdf-schema#seeAlso"
  PAGE_SIZE            default 5000
  REQUEST_TIMEOUT      default 120 (seconds)
  STATE_JSON           default "/data/sparql_sources.json"
  OUT_NQUADS           default "/data/endpoints.nq"

Behavior:
  - Only values of ENDPOINT_PREDICATE are treated as endpoints (no heuristic fallback).
  - Graph IRIs are epoch milliseconds (no hash). If a collision would occur, the
    timestamp is incremented (+1 ms) until a unique IRI is found considering both
    the current run and history stored in STATE_JSON.
  - STATE_JSON keeps history: { endpoint: [graph_iri_1, graph_iri_2, ...] }.
"""

import json
import os
import sys
import time
import tempfile
import subprocess
from pathlib import Path

from rdflib import Graph, ConjunctiveGraph, URIRef, RDF
from rdflib.term import URIRef as RDFURIRef, Literal as RDFLiteral
from SPARQLWrapper import SPARQLWrapper, TURTLE, POST, GET

# --- Config (env-overridable) ---
BASE_GRAPH_IRI = os.environ.get(
    "BASE_GRAPH_IRI",
    "https://purls.helmholtz-metadaten.de/msekg/g/"
).rstrip("/") + "/"

MWO_ENDPOINT_CLASS = URIRef(os.environ.get(
    "MWO_ENDPOINT_CLASS",
    "http://purls.helmholtz-metadaten.de/mwo/MWO_0001060"
))

ENDPOINT_PREDICATE = URIRef(os.environ.get(
    "ENDPOINT_PREDICATE",
    "https://purls.helmholtz-metadaten.de/nfdicore/NFDI_0001008"
))

HAS_GRAPH_PRED = URIRef(os.environ.get(
    "HAS_GRAPH_PRED",
    "http://www.w3.org/2000/01/rdf-schema#seeAlso"
))

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "5000"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "120"))

STATE_JSON = os.environ.get("STATE_JSON", "/data/sparql_sources.json")  # endpoint -> [graph_iri_1, graph_iri_2,...]
OUT_NQUADS = os.environ.get("OUT_NQUADS", "/data/endpoints.nq")         # merged quads across endpoints

# --- Helpers ---
def is_http_url_node(obj) -> bool:
    if isinstance(obj, RDFURIRef):
        u = str(obj)
        return u.startswith("http://") or u.startswith("https://")
    if isinstance(obj, RDFLiteral):
        s = str(obj)
        return s.startswith("http://") or s.startswith("https://")
    return False

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
            # normalize to dict[str, list[str]]
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
    """
    Generate unique BASE_GRAPH_IRI + epoch_ms, bumping by +1ms until not present in used_iris
    and not present anywhere in 'history' values.
    """
    now_ms = int(time.time() * 1000)
    # Build a set of all historical IRIs for fast lookup
    hist_set = set()
    for lst in history.values():
        hist_set.update(lst if isinstance(lst, list) else [lst])
    ts = now_ms
    while True:
        candidate = f"{BASE_GRAPH_IRI}{ts}"
        if (candidate not in used_iris) and (candidate not in hist_set):
            return candidate
        ts += 1  # bump 1ms and try again

def collect_endpoints_with_subjects(all_ttl_path):
    """
    Return list of (subject_iri, endpoint_url_str).
    Only accepts objects of ENDPOINT_PREDICATE that are http(s) URLs.
    """
    g = Graph()
    g.parse(all_ttl_path, format="turtle")
    pairs = []

    for s in g.subjects(RDF.type, MWO_ENDPOINT_CLASS):
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

def curl_construct(endpoint_url: str, limit: int, offset: int, timeout: int) -> bytes:
    """
    Use curl as a fallback: POST application/x-www-form-urlencoded with Accept: text/turtle.
    Follows redirects (-L). Returns raw bytes (turtle) or b'' if empty/failure.
    """
    query = f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ ?s ?p ?o }} OFFSET {offset} LIMIT {limit}"
    try:
        res = subprocess.run(
            [
                "curl",
                "-sS",
                "-L",                        # follow redirects
                "--max-time", str(timeout),
                "-H", "accept: text/turtle",
                "--data-urlencode", f"query={query}",
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

def stream_construct(endpoint_url, limit=5000, timeout=120):
    """Yield Turtle chunks of paged CONSTRUCT results."""
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setReturnFormat(TURTLE)
    offset = 0
    while True:
        query = f"""
        CONSTRUCT {{ ?s ?p ?o }}
        WHERE {{ ?s ?p ?o }}
        OFFSET {offset}
        LIMIT {limit}
        """
        data = None
        last_err = None

        # Try SPARQLWrapper GET and POST
        for method in (GET, POST):
            try:
                sparql.setMethod(method)
                sparql.setQuery(query)
                sparql.setTimeout(timeout)
                data = sparql.query().convert()  # bytes (Turtle)
                break
            except Exception as e:
                last_err = e
                data = None
                continue

        # Curl fallback
        if not data:
            data = curl_construct(endpoint_url, limit=limit, offset=offset, timeout=timeout)
            if not data:
                if offset == 0 and last_err:
                    raise RuntimeError(f"Failed initial fetch from {endpoint_url}: {last_err}")
                break

        chunk = data.decode("utf-8", errors="replace").strip()
        if not chunk:
            break

        # Quick guard: if server returned HTML, stop (not a SPARQL endpoint)
        if chunk.lower().startswith("<!doctype html") or chunk.lower().startswith("<html"):
            if offset == 0:
                raise RuntimeError(f"Endpoint {endpoint_url} returned HTML (not RDF).")
            break

        yield chunk
        offset += limit
        time.sleep(0.1)

def write_all_quads(cg: ConjunctiveGraph, out_path: str):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(out_path).with_suffix(".tmp")
    cg.serialize(destination=str(tmp), format="nquads")
    tmp.replace(Path(out_path))

def overwrite_ttl(path: str, graph: Graph):
    """Safely overwrite a Turtle file."""
    p = Path(path)
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".ttl", encoding="utf-8") as tf:
        tmp_name = tf.name
    graph.serialize(destination=tmp_name, format="turtle")
    Path(tmp_name).replace(p)

# --- Main flow ---
def main():
    if len(sys.argv) < 2:
        print("Usage: fetch_endpoints.py <path-to-all.ttl> [out-nquads]", file=sys.stderr)
        sys.exit(2)

    all_ttl = sys.argv[1]
    out_nq = sys.argv[2] if len(sys.argv) > 2 else OUT_NQUADS

    # 1) Discover endpoints and their subject IRIs (STRICT: only ENDPOINT_PREDICATE)
    pairs = collect_endpoints_with_subjects(all_ttl)  # [(subject_iri, endpoint_url)]
    if not pairs:
        print("No endpoints found in all.ttl.")
        return

    # 2) Load history: endpoint -> [graph_iri, ...]
    history = load_state(STATE_JSON)

    # 3) Fetch each endpoint into its own named graph
    cg = ConjunctiveGraph()
    summary = []
    used_this_run = set()  # to avoid duplicate IRIs within a run

    for subj_iri, ep in pairs:
        ep = ep.strip()

        # Always create a NEW graph IRI per run (no reuse), and ensure uniqueness globally
        graph_iri = next_unique_graph_iri(used_this_run, history)
        used_this_run.add(graph_iri)

        # record in history
        lst = history.get(ep, [])
        if isinstance(lst, str):
            lst = [lst]
        lst.append(graph_iri)
        history[ep] = lst

        ctx = cg.get_context(URIRef(graph_iri))
        before = len(ctx)

        try:
            for chunk in stream_construct(ep, limit=PAGE_SIZE, timeout=REQUEST_TIMEOUT):
                tmp_g = Graph()
                tmp_g.parse(data=chunk, format="turtle")
                for t in tmp_g:
                    ctx.add(t)
        except Exception as e:
            print(f"WARNING: Failed to fetch from {ep}: {e}", file=sys.stderr)

        added = len(ctx) - before
        summary.append({"subject": str(subj_iri), "endpoint": ep, "graph_iri": graph_iri, "triples_added": added})
        print(f"[{ep}] -> {graph_iri} (+{added} triples)")
        time.sleep(0.25)  # be polite

    # 4) Write merged N-Quads and state
    write_all_quads(cg, out_nq)
    save_state(STATE_JSON, history)
    Path("/data/sparql_sources_list.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # 5) Overwrite all.ttl adding seeAlso pointers (or env predicate)
    g_all = Graph()
    g_all.parse(all_ttl, format="turtle")

    # Build quick lookup: endpoint_url -> subject(s)
    ep_to_subjects = {}
    for s, ep in pairs:
        ep_to_subjects.setdefault(ep, set()).add(s)

    added_links = 0
    for item in summary:
        ep = item["endpoint"]
        graph_iri = item["graph_iri"]
        subjects = ep_to_subjects.get(ep, set())
        for s in subjects:
            triple = (s, HAS_GRAPH_PRED, URIRef(graph_iri))
            if triple not in g_all:
                g_all.add(triple)
                added_links += 1

    #overwrite_ttl(all_ttl, g_all)
    print(f"Updated {all_ttl} with {added_links} graph pointer triple(s).")
    print(f"Wrote quads: {out_nq}")
    print(f"Mapping JSON (history): {STATE_JSON}")
    print("Done.")

if __name__ == "__main__":
    main()
