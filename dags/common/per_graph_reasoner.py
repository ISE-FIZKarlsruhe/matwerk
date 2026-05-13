# common/per_graph_reasoner.py
"""
Per-named-graph reasoning for Zenodo TriG inputs.

Why this exists
---------------
ROBOT (OWLAPI) flattens TriG into a single ontology, so a single bad imported
file (e.g. an old reasoner-output TTL round-tripped through Zenodo) makes the
whole Openllet run inconsistent and the entire DAG fails. By splitting the
TriG into one file per named graph, reasoning each separately, and recombining,
we isolate inconsistent records and keep the rest of the pipeline running.

Outputs (all written into out_dir):
  - {artifact}_inferences.ttl      flat TTL, union of inferences from consistent graphs
                                   (kept for compatibility with the existing
                                    validation_checks DAG)
  - {artifact}_published.trig      TriG, per-graph data + inferences + metadata,
                                   intended for per-graph publishing to Virtuoso
  - validation_manifest.json       structured per-graph status
  - validation_report.txt          concatenated reasoner stdout/stderr
  - per_graph/<safe>/...           intermediate files (kept for debugging)
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

from rdflib import Dataset, Graph, URIRef
from rdflib.namespace import RDFS

from .graph_metadata import (
    GRAPH_PROVENANCE_PREFIXES,
    GraphProvenanceFacts,
    ValidationFacts,
    build_graph_provenance_ttl,
    utc_now_iso_seconds,
)


# -------- helpers ---------------------------------------------------------

_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_stem(s: str, max_len: int = 40) -> str:
    base = _SAFE_RE.sub("_", s).strip("_")[:max_len]
    digest = hashlib.sha1(s.encode("utf-8")).hexdigest()[:10]
    return f"{base}_{digest}" if base else digest


def _is_trig(path: str) -> bool:
    """
    Cheap detection: TriG declares named graphs as "<...> { ... }".
    We do not parse — just scan the first ~64 KiB for a graph header.
    """
    try:
        with open(path, "rb") as f:
            head = f.read(65536)
    except OSError:
        return False
    text = head.decode("utf-8", errors="replace")
    return bool(re.search(r"^\s*<[^>]+>\s*\{", text, flags=re.MULTILINE))


def _run(cmd: List[str], *, timeout: Optional[int] = None) -> Tuple[int, str, str]:
    """Run a subprocess; return (rc, stdout, stderr) — never raises on non-zero."""
    print(f"[CMD] {' '.join(cmd)}")
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _shell(cmd: str, *, timeout: Optional[int] = None) -> Tuple[int, str, str]:
    """Same as _run but takes a shell string (needed for redirection in extract)."""
    print(f"[SHELL] {cmd}")
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        shell=True,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _excerpt(text: str, max_chars: int = 600) -> str:
    text = (text or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n…(truncated)"


# -------- main config -----------------------------------------------------


@dataclass
class ReasonConfig:
    in_path: str                # input TriG (zenodo.ttl) or flat TTL
    artifact: str               # "zenodo", "spreadsheets", ...
    extension_owl_path: str     # nfdicore-extension.owl
    out_dir: str                # run dir (intermediate + final outputs go here)
    robot_cmd: str              # e.g. "robot" or full path
    openllet_cmd: str           # e.g. "java -jar /opt/airflow/openlletnew.jar"
    pre_filter_terms: List[str] # IRIs to remove before reasoning (subset of the
                                # current preFilterCmdTemplate term list)
    extract_signatures: str = (
        "PropertyAssertion SubPropertyOf InverseProperties SubClassOf ClassAssertion"
    )


# -------- reasoning of one isolated graph ---------------------------------


def _pre_filter(cfg: ReasonConfig, in_ttl: str, out_ttl: str) -> Tuple[int, str, str]:
    cmd = [*cfg.robot_cmd.split(), "remove", "--input", in_ttl]
    if cfg.pre_filter_terms:
        cmd += ["--term", cfg.pre_filter_terms[0], "--axioms", "SubPropertyChainOf"]
        for t in cfg.pre_filter_terms[1:]:
            cmd += ["remove", "--term", t]
    cmd += ["--output", out_ttl]
    return _run(cmd)


def _merge_expand(cfg: ReasonConfig, in_ttl: str, out_ttl: str) -> Tuple[int, str, str]:
    cmd = [
        *cfg.robot_cmd.split(),
        "merge",
        "--input", in_ttl,
        "--input", cfg.extension_owl_path,
        "expand",
        "--annotate-expansion-axioms", "true",
        "--output", out_ttl,
    ]
    return _run(cmd)


def _openllet_consistency(cfg: ReasonConfig, in_ttl: str) -> Tuple[int, str, str]:
    cmd = f"{cfg.openllet_cmd} consistency '{in_ttl}'"
    return _shell(cmd)


def _openllet_extract(cfg: ReasonConfig, in_ttl: str, out_owl: str) -> Tuple[int, str, str]:
    cmd = (
        f"{cfg.openllet_cmd} extract -s \"{cfg.extract_signatures}\" "
        f"'{in_ttl}' > '{out_owl}'"
    )
    return _shell(cmd)


def _robot_convert(cfg: ReasonConfig, in_owl: str, out_ttl: str) -> Tuple[int, str, str]:
    cmd = [*cfg.robot_cmd.split(), "convert", "--input", in_owl, "--output", out_ttl]
    return _run(cmd)


# -------- per-graph result modeling ---------------------------------------


@dataclass
class GraphResult:
    graph_iri: str
    safe: str
    asserted_path: str
    consistent: bool
    log: str
    inferences_ttl_path: Optional[str]   # set only when consistent
    triples_in: int = 0
    triples_inferred: int = 0
    error: Optional[str] = None


def _split_trig(in_trig: str, work_dir: str) -> Tuple[List[Tuple[str, str]], Graph]:
    """
    Split the TriG into per-graph TTL files. Returns:
      - list of (graph_iri, ttl_path)
      - the default graph as an rdflib.Graph (for re-export later)
    """
    ds = Dataset()
    ds.parse(in_trig, format="trig")

    per_graph_dir = os.path.join(work_dir, "per_graph")
    os.makedirs(per_graph_dir, exist_ok=True)

    out: List[Tuple[str, str]] = []
    default = Graph()

    for ctx in ds.contexts():
        # rdflib's default graph identifier is a URN-like string; treat it specially.
        is_default = (
            ctx.identifier == ds.default_context.identifier
            or str(ctx.identifier).startswith("urn:x-rdflib:default")
        )
        if is_default:
            for t in ctx:
                default.add(t)
            continue

        graph_iri = str(ctx.identifier)
        safe = _safe_stem(graph_iri)
        sub_dir = os.path.join(per_graph_dir, safe)
        os.makedirs(sub_dir, exist_ok=True)
        ttl_path = os.path.join(sub_dir, "asserted.ttl")
        flat = Graph()
        for t in ctx:
            flat.add(t)
        flat.serialize(destination=ttl_path, format="turtle")
        out.append((graph_iri, ttl_path))

    return out, default


def _reason_one_graph(
    cfg: ReasonConfig,
    graph_iri: str,
    asserted_ttl: str,
) -> GraphResult:
    safe = _safe_stem(graph_iri)
    sub_dir = os.path.dirname(asserted_ttl)
    filtered = os.path.join(sub_dir, "filtered.ttl")
    expanded = os.path.join(sub_dir, "expanded.ttl")
    inferences_owl = os.path.join(sub_dir, "inferences.owl")
    inferences_ttl = os.path.join(sub_dir, "inferences.ttl")

    log_parts: List[str] = [f"## graph {graph_iri}"]
    log_parts.append(f"asserted_ttl: {asserted_ttl}")

    triples_in = 0
    try:
        gtmp = Graph()
        gtmp.parse(asserted_ttl, format="turtle")
        triples_in = len(gtmp)
        del gtmp
    except Exception as e:
        log_parts.append(f"[WARN] could not count triples: {e}")

    # 1) pre-filter (best-effort; if it fails we skip rather than abort the run)
    rc, so, se = _pre_filter(cfg, asserted_ttl, filtered)
    log_parts.append(f"### pre_filter rc={rc}")
    if so: log_parts.append(so)
    if se: log_parts.append(se)
    if rc != 0:
        return GraphResult(
            graph_iri=graph_iri, safe=safe, asserted_path=asserted_ttl,
            consistent=False, log="\n".join(log_parts),
            inferences_ttl_path=None, triples_in=triples_in,
            error="pre_filter failed",
        )

    # 2) merge with nfdicore-extension + expand
    rc, so, se = _merge_expand(cfg, filtered, expanded)
    log_parts.append(f"### merge_expand rc={rc}")
    if so: log_parts.append(so)
    if se: log_parts.append(se)
    if rc != 0:
        return GraphResult(
            graph_iri=graph_iri, safe=safe, asserted_path=asserted_ttl,
            consistent=False, log="\n".join(log_parts),
            inferences_ttl_path=None, triples_in=triples_in,
            error="merge_expand failed",
        )

    # 3) consistency check (cheap; openllet prints "Consistent: Yes/No" on stdout
    #    and returns rc != 0 + InconsistentOntologyException when inconsistent)
    rc, so, se = _openllet_consistency(cfg, expanded)
    log_parts.append(f"### openllet consistency rc={rc}")
    if so: log_parts.append(so)
    if se: log_parts.append(se)
    out_text = (so or "") + "\n" + (se or "")
    is_inconsistent = (
        "Consistent: No" in out_text
        or "InconsistentOntologyException" in out_text
        or "Cannot do reasoning with inconsistent" in out_text
    )
    consistent = (rc == 0) and not is_inconsistent
    if not consistent:
        return GraphResult(
            graph_iri=graph_iri, safe=safe, asserted_path=asserted_ttl,
            consistent=False, log="\n".join(log_parts),
            inferences_ttl_path=None, triples_in=triples_in,
            error="inconsistent",
        )

    # 4) extract inferences
    rc, so, se = _openllet_extract(cfg, expanded, inferences_owl)
    log_parts.append(f"### openllet extract rc={rc}")
    if so: log_parts.append(so)
    if se: log_parts.append(se)
    if rc != 0 or not os.path.exists(inferences_owl) or os.path.getsize(inferences_owl) == 0:
        return GraphResult(
            graph_iri=graph_iri, safe=safe, asserted_path=asserted_ttl,
            consistent=True, log="\n".join(log_parts),
            inferences_ttl_path=None, triples_in=triples_in,
            error="extract produced no output",
        )

    # 5) convert OWL/XML -> TTL
    rc, so, se = _robot_convert(cfg, inferences_owl, inferences_ttl)
    log_parts.append(f"### robot convert rc={rc}")
    if so: log_parts.append(so)
    if se: log_parts.append(se)
    if rc != 0 or not os.path.exists(inferences_ttl) or os.path.getsize(inferences_ttl) == 0:
        return GraphResult(
            graph_iri=graph_iri, safe=safe, asserted_path=asserted_ttl,
            consistent=True, log="\n".join(log_parts),
            inferences_ttl_path=None, triples_in=triples_in,
            error="convert produced no output",
        )

    triples_inferred = 0
    try:
        gtmp = Graph()
        gtmp.parse(inferences_ttl, format="turtle")
        triples_inferred = len(gtmp)
        del gtmp
    except Exception as e:
        log_parts.append(f"[WARN] could not count inferred triples: {e}")

    return GraphResult(
        graph_iri=graph_iri, safe=safe, asserted_path=asserted_ttl,
        consistent=True, log="\n".join(log_parts),
        inferences_ttl_path=inferences_ttl, triples_in=triples_in,
        triples_inferred=triples_inferred,
    )


# -------- recombination ---------------------------------------------------


def _build_validation_metadata_graph(
    *,
    graph_iri: str,
    record_iri: Optional[str],
    file_key: str,
    consistent: bool,
    log_excerpt: str,
    full_log: Optional[str],
    checked_at: str,
) -> Graph:
    """
    Build a small TTL fragment carrying BFO/MWO provenance + validation status
    for `<graph_iri>`. Returns a parsed rdflib.Graph (so the caller can drop
    its triples into a named context of the output Dataset).

    record_iri is optional — if missing, we mint a placeholder by stripping
    the trailing `/graph/<file_key>` if present.
    """
    facts = GraphProvenanceFacts(
        graph_iri=graph_iri,
        record_iri=record_iri or graph_iri.split("/graph/")[0],
        file_key=file_key,
        download_url=None,
        record_url=None,
        content_type=None,
        harvested_at=checked_at,
        validation=ValidationFacts(
            consistent=consistent,
            reasoner="openllet",
            log_excerpt=log_excerpt,
            full_log=full_log,
            checked_at=checked_at,
        ),
    )
    ttl = GRAPH_PROVENANCE_PREFIXES + build_graph_provenance_ttl(facts)
    g = Graph()
    g.parse(data=ttl, format="turtle")
    return g


def _flatten_inferences(results: Iterable[GraphResult], out_ttl: str) -> int:
    """Concatenate all consistent-graph inferences into one flat TTL."""
    union = Graph()
    for r in results:
        if r.consistent and r.inferences_ttl_path and os.path.exists(r.inferences_ttl_path):
            try:
                union.parse(r.inferences_ttl_path, format="turtle")
            except Exception as e:
                print(f"[WARN] could not merge inferences from {r.graph_iri}: {e}")
    union.serialize(destination=out_ttl, format="turtle")
    return len(union)


def _build_published_trig(
    *,
    default_graph: Graph,
    results: List[GraphResult],
    asserted_per_graph: List[Tuple[str, str]],  # same as _split_trig output
    out_path: str,
    record_iri_lookup: Optional[dict] = None,   # graph_iri -> record_iri (from default graph)
) -> None:
    """
    Build the per-graph TriG that the publisher uploads to Virtuoso.
    For each named graph G in the input:
      <G>             : original asserted triples (always present)
      <G/inferences>  : inferred triples (present only when consistent)
      <G/metadata>    : BFO/MWO provenance + validation triples
    Default graph: original default + an aggregated index pointer.
    """
    ds = Dataset()
    # default graph
    for t in default_graph:
        ds.add(t)

    asserted_by_iri = {iri: path for iri, path in asserted_per_graph}
    checked_at = utc_now_iso_seconds()

    for r in results:
        # 1. asserted triples
        ctx_data = ds.graph(URIRef(r.graph_iri))
        try:
            ctx_data.parse(asserted_by_iri[r.graph_iri], format="turtle")
        except Exception as e:
            print(f"[WARN] could not load asserted for {r.graph_iri}: {e}")

        # 2. inferences (only if produced)
        has_inferences = (
            r.consistent
            and r.inferences_ttl_path
            and os.path.exists(r.inferences_ttl_path)
        )
        if has_inferences:
            ctx_inf = ds.graph(URIRef(r.graph_iri + "/inferences"))
            try:
                ctx_inf.parse(r.inferences_ttl_path, format="turtle")
            except Exception as e:
                print(f"[WARN] could not load inferences for {r.graph_iri}: {e}")

        # 3. metadata
        record_iri = (record_iri_lookup or {}).get(r.graph_iri)
        # File key = trailing path segment after /graph/ if present, else the safe stem.
        m = re.search(r"/graph/(.+)$", r.graph_iri)
        file_key = m.group(1) if m else r.safe
        meta_g = _build_validation_metadata_graph(
            graph_iri=r.graph_iri,
            record_iri=record_iri,
            file_key=file_key,
            consistent=r.consistent,
            log_excerpt=_excerpt(r.error or "consistent"),
            full_log=r.log,
            checked_at=checked_at,
        )
        ctx_meta = ds.graph(URIRef(r.graph_iri + "/metadata"))
        for t in meta_g:
            ctx_meta.add(t)
        # Schema-driven link from the asserted graph IRI to its inferences graph IRI,
        # so SPARQL clients can discover inference graphs without IRI string matching.
        if has_inferences:
            ctx_meta.add((
                URIRef(r.graph_iri),
                RDFS.seeAlso,
                URIRef(r.graph_iri + "/inferences"),
            ))

        # 4. annotate the graph IRI in the default graph with its consistency status
        if not r.consistent:
            ds.add((URIRef(r.graph_iri), RDFS.comment, _ttl_literal_safe("INCONSISTENT")))

    ds.serialize(destination=out_path, format="trig")


def _ttl_literal_safe(s: str):
    from rdflib import Literal
    return Literal(s)


def _build_record_iri_lookup(default_graph: Graph) -> dict:
    """
    Look up (graph_iri -> record_iri) using the harvester's BFO_0000050
    provenance triples emitted by build_graph_provenance_ttl.
    """
    out: dict = {}
    BFO_0000050 = URIRef("http://purl.obolibrary.org/obo/BFO_0000050")
    for s, p, o in default_graph.triples((None, BFO_0000050, None)):
        out[str(s)] = str(o)
    return out


# -------- top-level entry point ------------------------------------------


def reason_artifact(cfg: ReasonConfig) -> dict:
    """
    Drive the per-graph (TriG) or single-shot (flat TTL) reasoning flow.
    Returns a manifest dict; also writes the output files described at the top.
    """
    os.makedirs(cfg.out_dir, exist_ok=True)
    flat_inferences_ttl = os.path.join(cfg.out_dir, f"{cfg.artifact}_inferences.ttl")
    published_trig = os.path.join(cfg.out_dir, f"{cfg.artifact}_published.trig")
    manifest_path = os.path.join(cfg.out_dir, "validation_manifest.json")
    report_path = os.path.join(cfg.out_dir, "validation_report.txt")

    if not os.path.exists(cfg.in_path) or os.path.getsize(cfg.in_path) == 0:
        raise FileNotFoundError(f"input missing/empty: {cfg.in_path}")

    is_trig = _is_trig(cfg.in_path)

    if not is_trig:
        # Single-shot flat path. Run the same chain once with the input as one "graph".
        sub_dir = os.path.join(cfg.out_dir, "per_graph", "flat")
        os.makedirs(sub_dir, exist_ok=True)
        flat_in = os.path.join(sub_dir, "asserted.ttl")
        if os.path.abspath(cfg.in_path) != os.path.abspath(flat_in):
            shutil.copyfile(cfg.in_path, flat_in)
        result = _reason_one_graph(cfg, graph_iri=f"flat://{cfg.artifact}", asserted_ttl=flat_in)

        # Flat output: just copy the inferences if produced.
        if result.consistent and result.inferences_ttl_path:
            shutil.copyfile(result.inferences_ttl_path, flat_inferences_ttl)
        else:
            # Honour the existing convention that mark_reason_success expects a non-empty file.
            with open(flat_inferences_ttl, "w", encoding="utf-8") as f:
                f.write("# No inferences produced (flat path; see validation_report.txt).\n")

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(result.log + "\n")
        manifest = {
            "mode": "flat",
            "artifact": cfg.artifact,
            "graphs": [_result_to_dict(result)],
        }
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
        return manifest

    # ---- TriG path ----
    work_dir = cfg.out_dir
    print(f"[INFO] Splitting TriG: {cfg.in_path}")
    asserted_per_graph, default_graph = _split_trig(cfg.in_path, work_dir)
    print(f"[INFO] {len(asserted_per_graph)} named graph(s) to reason")

    record_iri_lookup = _build_record_iri_lookup(default_graph)

    results: List[GraphResult] = []
    log_parts: List[str] = []
    for graph_iri, asserted_ttl in asserted_per_graph:
        print(f"[INFO] Reasoning graph: {graph_iri}")
        r = _reason_one_graph(cfg, graph_iri, asserted_ttl)
        print(f"[INFO]  -> consistent={r.consistent} error={r.error} "
              f"in={r.triples_in} inferred={r.triples_inferred}")
        results.append(r)
        log_parts.append(r.log)

    # Flat inferences (consistent graphs only) for compatibility with validation_checks
    n_inf = _flatten_inferences(results, flat_inferences_ttl)
    print(f"[INFO] Wrote {flat_inferences_ttl} ({n_inf} triples)")

    # Per-graph TriG for publisher
    _build_published_trig(
        default_graph=default_graph,
        results=results,
        asserted_per_graph=asserted_per_graph,
        out_path=published_trig,
        record_iri_lookup=record_iri_lookup,
    )
    print(f"[INFO] Wrote {published_trig}")

    # Reports
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(log_parts) + "\n")
    manifest = {
        "mode": "trig",
        "artifact": cfg.artifact,
        "graphs": [_result_to_dict(r) for r in results],
        "counts": {
            "graphs_total": len(results),
            "graphs_consistent": sum(1 for r in results if r.consistent),
            "graphs_inconsistent": sum(1 for r in results if not r.consistent),
            "inferred_triples_total": n_inf,
        },
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    return manifest


def _result_to_dict(r: GraphResult) -> dict:
    return {
        "graph_iri": r.graph_iri,
        "consistent": r.consistent,
        "error": r.error,
        "triples_in": r.triples_in,
        "triples_inferred": r.triples_inferred,
        "asserted_path": r.asserted_path,
        "inferences_ttl_path": r.inferences_ttl_path,
        "safe": r.safe,
    }
