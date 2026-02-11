#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import tempfile
import hashlib
import contextlib
import warnings
from pathlib import Path
import subprocess
from typing import Iterable, Set, List, Dict, Any

from rdflib import Graph, URIRef, RDF, Literal, Namespace, BNode
from rdflib.namespace import RDFS, OWL, SKOS, XSD

# rdflib Dataset fallback (older versions)
try:
    from rdflib.dataset import Dataset as RDFDataset  # rdflib >= 6.x

    def new_dataset():
        return RDFDataset()

except Exception:
    from rdflib import ConjunctiveGraph as RDFDataset  # rdflib <= 5.x

    warnings.filterwarnings("ignore", category=DeprecationWarning, module="rdflib")

    def new_dataset():
        return RDFDataset()


# ------------------ Config (env-overridable; may be overridden by CLI) ------------------
BASE_GRAPH_IRI = os.environ.get("BASE_GRAPH_IRI", "https://purls.helmholtz-metadaten.de/msekg/").rstrip("/") + "/"

ENDPOINT_CLASS = URIRef(os.environ.get("ENDPOINT_CLASS", "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001095"))
ENDPOINT_PREDICATE = URIRef(
    os.environ.get("ENDPOINT_PREDICATE", "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008")
)  # xsd:anyURI endpoint URL
DATASET_TYPE = URIRef(
    os.environ.get("DATASET_TYPE", "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009")
)  # Graph database (dataset)
IAO_0000235 = URIRef(os.environ.get("IAO_0000235", "http://purl.obolibrary.org/obo/IAO_0000235"))  # denotes
HAS_GRAPH_PRED = URIRef(os.environ.get("HAS_GRAPH_PRED", "http://purl.obolibrary.org/obo/BFO_0000051"))

MWO_OWL_PATH = os.environ.get("MWO_OWL_PATH", "ontology/mwo-full.owl")

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))

STATE_JSON = os.environ.get("STATE_JSON", "data/sparql_endpoints/sparql_sources.json")
ALL_TTL = os.environ.get("ALL_TTL", "data/all_NotReasoned.ttl")
SUMMARY_JSON = os.environ.get("SUMMARY_JSON", "data/sparql_endpoints/sparql_sources_list.json")
STATS_TTL = os.environ.get("STATS_TTL", "data/sparql_endpoints/dataset_stats.ttl")

# Per-graph output dir
NAMED_GRAPHS_DIR = os.environ.get("NAMED_GRAPHS_DIR", "data/sparql_endpoints/named_graphs/").rstrip("/") + "/"

# Namespaces
VOID = Namespace("http://rdfs.org/ns/void#")

# Functionality keys → deterministic graph IRIs per endpoint
FUNC_CLASSES = "classes"
FUNC_CLASS_HIER = "classHierarchy"
FUNC_TBOX = "tbox"


# ------------------ CLI ------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="Harvest SPARQL endpoints from an input Turtle catalog and write named-graph snapshots + annotation stats."
    )
    p.add_argument("--all-ttl", required=True, help="Input Turtle catalog (e.g. spreadsheets_asserted.ttl)")
    p.add_argument("--mwo-owl", required=True, help="Path to MWO ontology file (mwo-full.owl)")
    p.add_argument("--state-json", required=True, help="State JSON output path (optional; mapping/debug only)")
    p.add_argument("--summary-json", required=True, help="Summary JSON output path")
    p.add_argument("--stats-ttl", required=True, help="Stats TTL output path (annotation-only graph)")
    p.add_argument("--named-graphs-dir", required=True, help="Directory for named graph outputs (.nq/.ttl)")

    p.add_argument(
        "--base-graph-iri",
        default=BASE_GRAPH_IRI,
        help="Base IRI for named graphs (default from env/defaults).",
    )
    p.add_argument(
        "--request-timeout",
        type=int,
        default=REQUEST_TIMEOUT,
        help="SPARQL HTTP request timeout in seconds (default from env/defaults).",
    )
    p.add_argument("--print-config", action="store_true", help="Print resolved config and exit (debug)")
    return p.parse_args()


def apply_cli_overrides(args):
    """
    Override module-level config with CLI args.
    This keeps the rest of the code unchanged (it continues to use globals).
    """
    global ALL_TTL, MWO_OWL_PATH, STATE_JSON, SUMMARY_JSON, STATS_TTL, NAMED_GRAPHS_DIR, BASE_GRAPH_IRI, REQUEST_TIMEOUT

    ALL_TTL = args.all_ttl
    MWO_OWL_PATH = args.mwo_owl
    STATE_JSON = args.state_json
    SUMMARY_JSON = args.summary_json
    STATS_TTL = args.stats_ttl
    NAMED_GRAPHS_DIR = args.named_graphs_dir.rstrip("/") + "/"
    BASE_GRAPH_IRI = args.base_graph_iri.rstrip("/") + "/"
    REQUEST_TIMEOUT = args.request_timeout


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
    """
    Serialize `graph` to Turtle at `path` atomically.
    Writes a temp file in the same directory to avoid cross-device rename errors.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=str(p.parent), prefix=p.name + ".", suffix=".tmp")
    os.close(fd)
    try:
        graph.serialize(destination=tmp_path, format="turtle")
        os.replace(tmp_path, p)
    except Exception:
        with contextlib.suppress(Exception):
            os.remove(tmp_path)
        raise


def looks_like_html(s: str) -> bool:
    t = s.lstrip().lower()
    return t.startswith("<!doctype html") or t.startswith("<html")


def _stable_endpoint_key(endpoint_url: str) -> str:
    """
    Canonicalize endpoint string so trivial differences don't change graph IRIs.
    """
    u = endpoint_url.strip()
    # normalize trailing slash
    u = u[:-1] if u.endswith("/") else u
    return u


def deterministic_graph_iri(endpoint_url: str, func_key: str) -> str:
    """
    Deterministic named-graph IRI from (endpoint_url, func_key).
    No timestamps, no state dependence.
    """
    ep = _stable_endpoint_key(endpoint_url)
    payload = f"{func_key}|{ep}".encode("utf-8")
    h = hashlib.sha256(payload).hexdigest()[:24]  # 96 bits
    return f"{BASE_GRAPH_IRI}snapshot/sparql/{func_key}/{h}"


def file_stem_for_graph_iri(graph_iri: str) -> str:
    """
    Deterministic filename stem from the graph IRI.
    """
    return hashlib.sha1(graph_iri.encode("utf-8")).hexdigest()[:16]


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
            [
                "curl",
                "-sS",
                "-L",
                "--compressed",
                "--fail-with-body",
                "--max-time",
                str(timeout),
                "-H",
                "Accept: application/sparql-results+json",
                "-H",
                "Content-Type: application/sparql-query",
                "-H",
                "User-Agent: curl/8",
                "--data-binary",
                query,
                endpoint_url,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if res.returncode == 0 and res.stdout.strip() and not looks_like_html(res.stdout):
            return json.loads(res.stdout).get("results", {}).get("bindings", [])
    except Exception:
        pass
    # POST urlencoded
    try:
        res = subprocess.run(
            [
                "curl",
                "-sS",
                "-L",
                "--compressed",
                "--fail-with-body",
                "--max-time",
                str(timeout),
                "-H",
                "Accept: application/sparql-results+json",
                "-H",
                "User-Agent: curl/8",
                "--data-urlencode",
                f"query={query}",
                endpoint_url,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if res.returncode == 0 and res.stdout.strip() and not looks_like_html(res.stdout):
            return json.loads(res.stdout).get("results", {}).get("bindings", [])
    except Exception:
        pass
    # GET urlencoded
    try:
        res = subprocess.run(
            [
                "curl",
                "-sS",
                "-L",
                "--compressed",
                "--fail-with-body",
                "--max-time",
                str(timeout),
                "-H",
                "Accept: application/sparql-results+json",
                "-H",
                "User-Agent: curl/8",
                "--get",
                "--data-urlencode",
                f"query={query}",
                endpoint_url,
            ],
            check=False,
            capture_output=True,
            text=True,
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
            [
                "curl",
                "-sS",
                "-L",
                "--compressed",
                "--fail-with-body",
                "--max-time",
                str(timeout),
                "-H",
                "Accept: text/turtle",
                "-H",
                "Content-Type: application/sparql-query",
                "-H",
                "User-Agent: curl/8",
                "--data-binary",
                query,
                ep_url,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if res.returncode == 0 and res.stdout.strip() and not looks_like_html(res.stdout):
            return res.stdout
    except Exception:
        pass
    # POST urlencoded
    try:
        res = subprocess.run(
            [
                "curl",
                "-sS",
                "-L",
                "--compressed",
                "--fail-with-body",
                "--max-time",
                str(timeout),
                "-H",
                "Accept: text/turtle",
                "-H",
                "User-Agent: curl/8",
                "--data-urlencode",
                f"query={query}",
                ep_url,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if res.returncode == 0 and res.stdout.strip() and not looks_like_html(res.stdout):
            return res.stdout
    except Exception:
        pass
    # GET urlencoded
    try:
        res = subprocess.run(
            [
                "curl",
                "-sS",
                "-L",
                "--compressed",
                "--fail-with-body",
                "--max-time",
                str(timeout),
                "-H",
                "Accept: text/turtle",
                "-H",
                "User-Agent: curl/8",
                "--get",
                "--data-urlencode",
                f"query={query}",
                ep_url,
            ],
            check=False,
            capture_output=True,
            text=True,
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
        if not v:
            continue
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
    return """
CONSTRUCT { ?s ?p ?o . }
WHERE {
  { ?s a owl:Class . ?s ?p ?o . }
  UNION
  { ?s a owl:ObjectProperty . ?s ?p ?o . }
  UNION
  { ?s a owl:DatatypeProperty . ?s ?p ?o . }
}
"""


# Counts
Q_NUM_CLASSES = "SELECT (COUNT(DISTINCT ?c) AS ?n) WHERE { ?c a owl:Class . FILTER(isIRI(?c)) }"
Q_NUM_OBJ_P = "SELECT (COUNT(DISTINCT ?p) AS ?n) WHERE { ?p a owl:ObjectProperty . }"
Q_NUM_DAT_P = "SELECT (COUNT(DISTINCT ?p) AS ?n) WHERE { ?p a owl:DatatypeProperty . }"
Q_NUM_INST = """
SELECT (COUNT(DISTINCT ?i) AS ?n) WHERE {
  ?i a ?t . ?t a owl:Class .
  FILTER(isIRI(?i))
}
"""

# Per-class instance counts (partition materialization)
Q_INSTANCES_PER_CLASS = """
SELECT ?c (SAMPLE(?lbl) AS ?label) (COUNT(DISTINCT ?i) AS ?n)
WHERE {
  ?i a ?c .
  ?c a owl:Class .
  FILTER(isIRI(?i))
  OPTIONAL { ?c rdfs:label ?l . FILTER(LANG(?l) = "" || LANGMATCHES(LANG(?l), "en")) }
  BIND(COALESCE(?l, STRAFTER(STR(?c), "#"), STRAFTER(STR(?c), "/")) AS ?lbl)
}
GROUP BY ?c
"""

# Used terms → for void:vocabulary
Q_TERMS_USED = """
SELECT DISTINCT ?term
WHERE {
  {
    ?i a ?term .
    FILTER(isIRI(?i) && isIRI(?term))
  }
  UNION
  {
    ?s ?term ?o .
    FILTER(isIRI(?term))
  }
  FILTER(STRSTARTS(STR(?term), "http://") || STRSTARTS(STR(?term), "https://"))
}
"""


# ------------------ Discovery from all_NotReasoned.ttl ------------------
def discover_from_all_ttl(all_ttl_path: str):
    g = Graph()
    g.parse(all_ttl_path, format="turtle")

    endpoints = []
    for ep_ind in g.subjects(RDF.type, ENDPOINT_CLASS):
        for v in g.objects(ep_ind, ENDPOINT_PREDICATE):
            url = as_http_url_str(v)
            if url:
                endpoints.append((ep_ind, url))

    seen = set()
    uniq = []
    for ep_ind, url in endpoints:
        k = (str(ep_ind), url)
        if k not in seen:
            seen.add(k)
            uniq.append((ep_ind, url))

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
        if isinstance(c, URIRef):
            mwo_classes.add(c)
    for p in g.subjects(RDF.type, OWL.ObjectProperty):
        if isinstance(p, URIRef):
            mwo_objprops.add(p)
    for p in g.subjects(RDF.type, OWL.DatatypeProperty):
        if isinstance(p, URIRef):
            mwo_datprops.add(p)
    return mwo_classes, mwo_objprops, mwo_datprops


# ------------------ Reuse (sets) ------------------
def batch_values(items: Iterable[URIRef], batch_size: int = 150) -> List[List[URIRef]]:
    batch = []
    out = []
    for it in items:
        batch.append(it)
        if len(batch) >= batch_size:
            out.append(batch)
            batch = []
    if batch:
        out.append(batch)
    return out


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
        reused |= uris_from_bindings(run_select(ep_url, q, REQUEST_TIMEOUT), "c")
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
        reused |= uris_from_bindings(run_select(ep_url, q, REQUEST_TIMEOUT), "p")
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
        reused |= uris_from_bindings(run_select(ep_url, q, REQUEST_TIMEOUT), "p")
    return reused


# ------------------ Writers ------------------
def write_named_graph_files(graph_iri: str, turtle_text: str, nq_path: Path, ttl_path: Path):
    """
    Parse the Turtle, drop any triples with blank nodes, then:
      - write N-Quads with named graph IRI to nq_path
      - write Turtle (triples only) to ttl_path
    Works with rdflib>=6 (Dataset) and rdflib<=5 (ConjunctiveGraph).
    """
    if not turtle_text:
        return

    tmp_g = Graph()
    try:
        tmp_g.parse(data=turtle_text, format="turtle")
    except Exception as e:
        snippet = turtle_text[:200].replace("\n", "\\n")
        print(f"WARNING: failed to parse Turtle for graph {graph_iri}: {e}\n  Payload starts with: {snippet}")
        return

    nq_path.parent.mkdir(parents=True, exist_ok=True)
    ttl_path.parent.mkdir(parents=True, exist_ok=True)

    # 1) N-Quads with named graph IRI (skip bnodes)
    ds = new_dataset()
    try:
        ctx = ds.graph(URIRef(graph_iri))  # rdflib >= 6
    except AttributeError:
        ctx = ds.get_context(URIRef(graph_iri))  # rdflib <= 5

    for s, p, o in tmp_g:
        if isinstance(s, BNode) or isinstance(p, BNode) or isinstance(o, BNode):
            continue
        ctx.add((s, p, o))

    fd_nq, tmp_nq = tempfile.mkstemp(dir=str(nq_path.parent), prefix=nq_path.name + ".", suffix=".tmp")
    os.close(fd_nq)
    try:
        ds.serialize(destination=tmp_nq, format="nquads")
        os.replace(tmp_nq, nq_path)
    except Exception:
        with contextlib.suppress(Exception):
            os.remove(tmp_nq)
        raise

    # 2) Plain Turtle (triples only, skip bnodes)
    g_plain = Graph()
    for s, p, o in tmp_g:
        if isinstance(s, BNode) or isinstance(p, BNode) or isinstance(o, BNode):
            continue
        g_plain.add((s, p, o))

    safe_overwrite_ttl(str(ttl_path), g_plain)


# ------------------ VoID helpers ------------------
def partition_iri_for(ds: URIRef, cls: URIRef) -> URIRef:
    """Skolemize a stable, bnode-free IRI for a VoID classPartition node."""
    h = hashlib.sha1((str(ds) + " " + str(cls)).encode("utf-8")).hexdigest()[:16]
    return URIRef(f"{BASE_GRAPH_IRI}{h}")


def namespace_iri(u: str) -> str:
    """Best-effort namespace (hash or slash)."""
    if "#" in u:
        return u.split("#", 1)[0] + "#"
    i = u.rfind("/")
    return u[: i + 1] if i >= 0 else u


def make_stats_comment(
    ep_url: str,
    num_classes: int,
    num_objp: int,
    num_datp: int,
    num_inst: int,
    vocab_ns: Set[str],
    mwo_classes_used: Set[URIRef],
    mwo_objp_used: Set[URIRef],
    mwo_datp_used: Set[URIRef],
) -> str:
    """
    Single annotation string; no new predicates.
    Keep it bounded to avoid gigantic literals.
    """
    MAX_LIST = 50

    def fmt_list(title: str, iris: Set[URIRef]) -> str:
        iris_s = sorted((str(x) for x in iris), key=str)
        if not iris_s:
            return f"{title}: 0"
        head = iris_s[:MAX_LIST]
        more = len(iris_s) - len(head)
        lines = "\n  - ".join([""] + head)
        suffix = f"\n  ... (+{more} more)" if more > 0 else ""
        return f"{title}: {len(iris_s)}{lines}{suffix}"

    vocab_s = sorted(vocab_ns)[:MAX_LIST]
    vocab_more = len(vocab_ns) - len(vocab_s)
    vocab_lines = "\n  - ".join([""] + vocab_s) if vocab_s else ""
    vocab_suffix = f"\n  ... (+{vocab_more} more)" if vocab_more > 0 else ""

    return (
        f"SPARQL endpoint: {ep_url}\n"
        "Counts:\n"
        f"- classes: {num_classes}\n"
        f"- objectProperties: {num_objp}\n"
        f"- dataProperties: {num_datp}\n"
        f"- propertiesTotal: {num_objp + num_datp}\n"
        f"- instances: {num_inst}\n"
        "\n"
        "Vocabularies (namespaces observed):\n"
        + (f"- total: {len(vocab_ns)}{vocab_lines}{vocab_suffix}\n" if vocab_ns else "- total: 0\n")
        + "\n"
        "Reused from MWO (heuristic):\n"
        f"- classes: {len(mwo_classes_used)}\n"
        f"- objectProperties: {len(mwo_objp_used)}\n"
        f"- dataProperties: {len(mwo_datp_used)}\n"
        "\n"
        + fmt_list("MWO classes reused", mwo_classes_used)
        + "\n\n"
        + fmt_list("MWO objectProperties reused", mwo_objp_used)
        + "\n\n"
        + fmt_list("MWO dataProperties reused", mwo_datp_used)
    )


# ------------------ Main ------------------
def main():
    # state is now OPTIONAL mapping/debug only; determinism does NOT depend on it
    state = load_state(STATE_JSON)
    state.setdefault("by_endpoint", {})

    g_all, discovered = discover_from_all_ttl(ALL_TTL)
    if not discovered:
        print("No SPARQL endpoints found in all_NotReasoned.ttl.")
        return

    mwo_classes, mwo_objprops, mwo_datprops = load_mwo_terms(MWO_OWL_PATH)
    print(f"Loaded MWO terms: classes={len(mwo_classes)}, objProps={len(mwo_objprops)}, dataProps={len(mwo_datprops)}")

    # "stats" graph uses existing vocabularies + annotation properties only
    stats = Graph()
    stats.bind("rdfs", RDFS)
    stats.bind("owl", OWL)
    stats.bind("void", VOID)
    stats.bind("rdf", RDF)
    stats.bind("skos", SKOS)
    stats.bind("obo", Namespace("http://purl.obolibrary.org/obo/"))
    stats.bind("mwo", Namespace("http://purls.helmholtz-metadaten.de/mwo/"))

    summary = []
    Path(NAMED_GRAPHS_DIR).mkdir(parents=True, exist_ok=True)

    for rec in discovered:
        ep_ind = rec["endpoint_individual"]
        ep_url = rec["endpoint_url"]
        datasets = rec["datasets"]

        # -------- Deterministic graph IRIs (no timestamps) --------
        iri_classes = deterministic_graph_iri(ep_url, FUNC_CLASSES)
        iri_hier = deterministic_graph_iri(ep_url, FUNC_CLASS_HIER)
        iri_tbox = deterministic_graph_iri(ep_url, FUNC_TBOX)

        # deterministic filenames
        stem_classes = file_stem_for_graph_iri(iri_classes)
        stem_hier = file_stem_for_graph_iri(iri_hier)
        stem_tbox = file_stem_for_graph_iri(iri_tbox)

        files_classes = (Path(f"{NAMED_GRAPHS_DIR}{stem_classes}.nq"), Path(f"{NAMED_GRAPHS_DIR}{stem_classes}.ttl"))
        files_hier = (Path(f"{NAMED_GRAPHS_DIR}{stem_hier}.nq"), Path(f"{NAMED_GRAPHS_DIR}{stem_hier}.ttl"))
        files_tbox = (Path(f"{NAMED_GRAPHS_DIR}{stem_tbox}.nq"), Path(f"{NAMED_GRAPHS_DIR}{stem_tbox}.ttl"))

        # optional: store mapping for debugging
        state["by_endpoint"].setdefault(ep_url, {})
        state["by_endpoint"][ep_url][FUNC_CLASSES] = iri_classes
        state["by_endpoint"][ep_url][FUNC_CLASS_HIER] = iri_hier
        state["by_endpoint"][ep_url][FUNC_TBOX] = iri_tbox

        def one(q):
            return literal_int(run_select(ep_url, q, REQUEST_TIMEOUT), "n")

        num_classes = one(Q_NUM_CLASSES)
        num_objp = one(Q_NUM_OBJ_P)
        num_datp = one(Q_NUM_DAT_P)
        num_inst = one(Q_NUM_INST)
        num_props = num_objp + num_datp

        inst_bindings = run_select(ep_url, Q_INSTANCES_PER_CLASS, REQUEST_TIMEOUT)

        mwo_classes_used = reused_classes(ep_url, mwo_classes) if mwo_classes else set()
        mwo_objp_used = reused_objprops(ep_url, mwo_objprops) if mwo_objprops else set()
        mwo_datp_used = reused_dataprops(ep_url, mwo_datprops) if mwo_datprops else set()

        ttl_classes = fetch_construct_as_turtle(ep_url, q_construct_classes_plain(), REQUEST_TIMEOUT)
        if ttl_classes:
            write_named_graph_files(iri_classes, ttl_classes, files_classes[0], files_classes[1])
            print(f"[{ep_url}] classes → {files_classes[0].name}, {files_classes[1].name}")
        else:
            print(f"WARNING: classes CONSTRUCT returned nothing for {ep_url}")

        ttl_hier = fetch_construct_as_turtle(ep_url, q_construct_class_hierarchy_plain(), REQUEST_TIMEOUT)
        if ttl_hier:
            write_named_graph_files(iri_hier, ttl_hier, files_hier[0], files_hier[1])
            print(f"[{ep_url}] classHierarchy → {files_hier[0].name}, {files_hier[1].name}")
        else:
            print(f"WARNING: classHierarchy CONSTRUCT returned nothing for {ep_url}")

        ttl_tbox = fetch_construct_as_turtle(ep_url, q_construct_tbox_plain(), REQUEST_TIMEOUT)
        if ttl_tbox:
            write_named_graph_files(iri_tbox, ttl_tbox, files_tbox[0], files_tbox[1])
            print(f"[{ep_url}] tbox → {files_tbox[0].name}, {files_tbox[1].name}")
        else:
            print(f"WARNING: tbox CONSTRUCT returned nothing for {ep_url}")

        term_rows = run_select(ep_url, Q_TERMS_USED, REQUEST_TIMEOUT)
        vocab_ns = set()
        for b in term_rows:
            t = b.get("term", {}).get("value")
            if t:
                vocab_ns.add(namespace_iri(t))

        comment = make_stats_comment(
            ep_url=ep_url,
            num_classes=num_classes,
            num_objp=num_objp,
            num_datp=num_datp,
            num_inst=num_inst,
            vocab_ns=vocab_ns,
            mwo_classes_used=mwo_classes_used,
            mwo_objp_used=mwo_objp_used,
            mwo_datp_used=mwo_datp_used,
        )

        for ds in datasets:
            stats.add((ds, RDF.type, DATASET_TYPE))
            stats.add((ds, IAO_0000235, ep_ind))
            stats.add((ds, VOID.sparqlEndpoint, URIRef(ep_url)))

            # Keep VoID core stats (existing vocabulary)
            stats.add((ds, VOID.classes, Literal(num_classes, datatype=XSD.integer)))
            stats.add((ds, VOID.properties, Literal(num_props, datatype=XSD.integer)))
            stats.add((ds, VOID.entities, Literal(num_inst, datatype=XSD.integer)))

            # Stats as annotation only
            stats.add((ds, RDFS.comment, Literal(comment)))

            # Link the three named graphs
            for iri in (iri_classes, iri_hier, iri_tbox):
                stats.add((ds, HAS_GRAPH_PRED, URIRef(iri)))
                t = (ds, HAS_GRAPH_PRED, URIRef(iri))
                if t not in g_all:
                    g_all.add(t)

            # Keep VoID classPartition
            for b in inst_bindings:
                c = b.get("c", {}).get("value")
                n = b.get("n", {}).get("value")
                lbl = b.get("label", {}).get("value")
                if not c or not n:
                    continue
                c_iri = URIRef(c)
                try:
                    n_lit = Literal(int(float(n)), datatype=XSD.integer)
                except Exception:
                    continue

                part = partition_iri_for(ds, c_iri)
                stats.add((ds, VOID.classPartition, part))
                stats.add((part, VOID["class"], c_iri))
                stats.add((part, VOID.entities, n_lit))
                if lbl:
                    stats.add((part, RDFS.label, Literal(lbl)))

            for ns in sorted(vocab_ns):
                stats.add((ds, VOID.vocabulary, URIRef(ns)))

        summary.append(
            {
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
                    "propertiesTotal": num_props,
                    "instances": num_inst,
                    "numberOfClassesReusedFromMWO": len(mwo_classes_used),
                    "classesReusedFromMWO": [str(x) for x in sorted(mwo_classes_used, key=str)],
                    "numberOfObjectPropertiesReusedFromMWO": len(mwo_objp_used),
                    "objectPropertiesReusedFromMWO": [str(x) for x in sorted(mwo_objp_used, key=str)],
                    "numberOfDataPropertiesReusedFromMWO": len(mwo_datp_used),
                    "dataPropertiesReusedFromMWO": [str(x) for x in sorted(mwo_datp_used, key=str)],
                    "classCounts": [
                        {
                            "class": b["c"]["value"],
                            "label": b.get("label", {}).get("value", ""),
                            "instances": int(float(b["n"]["value"])),
                        }
                        for b in inst_bindings
                        if "c" in b and "n" in b
                    ],
                    "vocabularies": sorted(vocab_ns),
                },
            }
        )

    # merge stats into unified catalog
    for triple in stats:
        g_all.add(triple)

    ensure_parent(ALL_TTL)
    safe_overwrite_ttl(ALL_TTL, g_all)

    safe_overwrite_ttl(STATS_TTL, stats)

    # mapping/debug only
    save_state(STATE_JSON, state)
    ensure_parent(SUMMARY_JSON)
    Path(SUMMARY_JSON).write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\nUnified catalog + stats written to: {ALL_TTL}")
    print(f"(Stats copy also at: {STATS_TTL})")
    print(f"Named graphs directory: {NAMED_GRAPHS_DIR}")
    print(f"State: {STATE_JSON} (mapping/debug only; determinism does not depend on it)")
    print("Done.")


if __name__ == "__main__":
    args = parse_args()
    apply_cli_overrides(args)

    if args.print_config:
        print(
            json.dumps(
                {
                    "ALL_TTL": ALL_TTL,
                    "MWO_OWL_PATH": MWO_OWL_PATH,
                    "STATE_JSON": STATE_JSON,
                    "SUMMARY_JSON": SUMMARY_JSON,
                    "STATS_TTL": STATS_TTL,
                    "NAMED_GRAPHS_DIR": NAMED_GRAPHS_DIR,
                    "BASE_GRAPH_IRI": BASE_GRAPH_IRI,
                    "REQUEST_TIMEOUT": REQUEST_TIMEOUT,
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(0)

    main()
