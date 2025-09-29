#!/usr/bin/env python3
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
MAX_TRIPLES = int(os.environ.get("MAX_TRIPLES", "10000"))   # cap per endpoint for tests; 0=unlimited
ONLY_FIRST_ENDPOINT = os.environ.get("ONLY_FIRST_ENDPOINT", "0") == "1"

STATE_JSON = os.environ.get("STATE_JSON", "data/sparql_endpoints/sparql_sources.json")   # endpoint -> [graph_iri,...]
OUT_NQUADS = os.environ.get("OUT_NQUADS", "data/sparql_endpoints/endpoints.nq")          # merged quads across endpoints
ALL_TTL    = os.environ.get("ALL_TTL", "data/all.ttl")
SUMMARY_JSON = os.environ.get("SUMMARY_JSON", "data/sparql_endpoints/sparql_sources_list.json")

# If we're capping triples, don't ask the server for more than we need
if MAX_TRIPLES > 0:
    PAGE_SIZE = min(PAGE_SIZE, MAX_TRIPLES)

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
    return f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ ?s ?p ?o }} OFFSET {offset} LIMIT {limit}"

def fetch_nt_page(endpoint_url: str, query: str, timeout: int) -> bytes:
    sw = SPARQLWrapper(endpoint_url)
    sw.setTimeout(timeout)
    sw.addCustomHttpHeader("Accept", "application/n-triples")
    sw.addCustomHttpHeader("Accept-Encoding", "gzip")
    for method in (GET, POST):
        try:
            sw.setMethod(method)
            sw.setQuery(query)
            return sw.query().response.read()
        except Exception:
            continue
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

def stream_construct_nt(endpoint_url: str, page_size: int, timeout: int, max_total: int | None):
    """
    Yield N-Triples pages; stop when endpoint is empty or max_total reached.
    """
    offset = 0
    remaining = max_total if (max_total and max_total > 0) else None
    while True:
        this_limit = min(page_size, remaining) if remaining else page_size
        if this_limit <= 0:
            break
        q = make_construct_query(offset, this_limit)
        raw = fetch_nt_page(endpoint_url, q, timeout=timeout)
        if not raw:
            break
        text = raw.decode("utf-8", errors="replace").strip()
        if not text:
            break
        if text.lower().startswith("<!doctype html") or text.lower().startswith("<html"):
            if offset == 0:
                raise RuntimeError(f"Endpoint {endpoint_url} returned HTML (not RDF).")
            break
        yield text
        triples_in_chunk = sum(1 for L in text.splitlines() if L.strip().endswith(" .") and not L.lstrip().startswith("#"))
        offset += this_limit
        if remaining is not None:
            remaining -= triples_in_chunk
            if remaining <= 0:
                break

# ------------------ Writing ------------------
def ensure_parent(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def append_nt_as_nq(nt_text: str, graph_iri: str, out_handle):
    """
    Convert NT lines to NQ by inserting graph IRI; write to the given handle.
    """
    for line in nt_text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.endswith(" ."):
            out_handle.write(f"{s[:-2]} <{graph_iri}> .\n")

def rewrite_nquads_excluding_graphs(src_path: str, dst_path: str, excluded_graphs: set[str]):
    """
    Copy all quads from src to dst, skipping lines whose graph IRI is in excluded_graphs.
    Assumes line-oriented N-Quads: ... <graph> .
    """
    ensure_parent(dst_path)
    if not Path(src_path).exists():
        Path(dst_path).write_text("", encoding="utf-8")
        return
    with open(src_path, "r", encoding="utf-8", errors="replace") as src, \
         open(dst_path, "w", encoding="utf-8") as dst:
        for line in src:
            s = line.rstrip("\n")
            if not s or s.lstrip().startswith("#"):
                continue
            # Fast check: graph IRI is the last <...> before final " ."
            # We'll just see if any excluded graph substring appears; stricter parse if needed.
            skip = False
            for g in excluded_graphs:
                if s.endswith(f"<{g}> ."):
                    skip = True
                    break
            if not skip:
                dst.write(s + "\n")

def safe_overwrite(path: str, tmp_path: str):
    Path(tmp_path).replace(Path(path))

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
    if ONLY_FIRST_ENDPOINT:
        pairs = pairs[:1]

    print("Found pairs: ", pairs)

    history = load_state(STATE_JSON)
    used_this_run = set()
    summary = []

    # Determine the graph IRI to use per endpoint (reuse if exists)
    endpoint_to_graph = {}
    graphs_to_refresh = set()

    for _, ep in pairs:
        ep = ep.strip()
        lst = history.get(ep, [])
        if isinstance(lst, str):
            lst = [lst]
        if lst:
            graph_iri = lst[-1]  # reuse last known graph for this endpoint
        else:
            graph_iri = next_unique_graph_iri(used_this_run, history)
            lst.append(graph_iri)
            history[ep] = lst
        used_this_run.add(graph_iri)
        endpoint_to_graph[ep] = graph_iri
        graphs_to_refresh.add(graph_iri)

    # Rebuild OUT_NQUADS: copy all old quads except those for graphs we're refreshing
    tmp_nq = str(Path(OUT_NQUADS).with_suffix(".tmp"))
    rewrite_nquads_excluding_graphs(OUT_NQUADS, tmp_nq, graphs_to_refresh)

    # Append fresh quads for each refreshed graph
    with open(tmp_nq, "a", encoding="utf-8") as outfh:
        for subj_iri, ep in pairs:
            ep = ep.strip()
            graph_iri = endpoint_to_graph[ep]
            print(f"Fetching from endpoint: {ep}  -> graph {graph_iri}")

            total_written = 0
            try:
                for nt_chunk in stream_construct_nt(ep, PAGE_SIZE, REQUEST_TIMEOUT,
                                                    max_total=MAX_TRIPLES if MAX_TRIPLES > 0 else None):
                    append_nt_as_nq(nt_chunk, graph_iri, outfh)
                    total_written += sum(1 for L in nt_chunk.splitlines()
                                         if L.strip().endswith(" .") and not L.lstrip().startswith("#"))
            except Exception as e:
                print(f"WARNING: Failed to fetch from {ep}: {e}")

            summary.append({
                "subject": str(subj_iri),
                "endpoint": ep,
                "graph_iri": graph_iri,
                "triples_added_estimate": total_written
            })
            print(f"[{ep}] -> {graph_iri} (~{total_written} triples)")

    # Atomically replace the N-Quads file
    ensure_parent(OUT_NQUADS)
    safe_overwrite(OUT_NQUADS, tmp_nq)

    # Save state and summary
    save_state(STATE_JSON, history)
    summary_path = Path(SUMMARY_JSON)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    # Update all.ttl with graph pointers (add only if missing)
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
