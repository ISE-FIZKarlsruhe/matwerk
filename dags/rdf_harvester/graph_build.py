# rdf_harvester/graph_build.py
"""Assemble harvested RDF into a TriG dataset + a metadata (default) graph.

Each harvested file becomes its own **named graph** (verbatim triples). Two blocks
of metadata are written, and they serve different readers:

1. **Into the named graph itself** — the graph describes itself, in the same shape
   every pipeline graph uses (``docs/shapes/graph-metadata/pattern.md``): a
   ``void:Dataset`` with a *textual description* node, a load **process** carrying
   the temporal region, one information-content entity per source file (with its
   sha256), and VoID statistics. This is what the ``/graphs`` dashboard and the
   Superset statistics read, so a harvested graph looks like any other.

2. **Into the TriG's default graph** — the registry that links every file-graph
   back to the repository/record it came from (BFO has-part / part-of), plus the
   flat ``stat:`` view a spreadsheet can query in one hop.

Only BFO / IAO / nfdicore / VoID / W3C-Time vocabulary is used — deliberately no
PROV and no Dublin Core, matching the rest of the pipeline.

Two modelling rules are easy to get wrong and are enforced here:

* the description node is ``NFDI_0001018`` *textual description* — **not**
  ``NFDI_0000018`` *academic event*, one digit away, which sits on the right-hand
  side of ``denoted by`` (which requires information, not an event) and makes the
  graph logically inconsistent;
* ``occupies temporal region`` goes on the **load process only** — never on the
  graph, the description or an agent.

Graph IRIs are a deterministic function of (source, key, path), so a re-run updates
the same graph in place rather than minting a duplicate.
"""
from __future__ import annotations

import posixpath
import re
from typing import List, Optional

from rdflib import BNode, Dataset, Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD

try:  # normal Airflow import (dags/ is on sys.path)
    from common.mwo_terms import terms as _mwo_terms
except Exception:  # standalone / test use
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from common.mwo_terms import terms as _mwo_terms

from .harvest import SourceHarvest

MATWERK = "https://nfdi.fiz-karlsruhe.de/matwerk"
GITHUB_ROOT = f"{MATWERK}/github"
ZENODO_ROOT = f"{MATWERK}/zenodo"
METADATA_GRAPH_IRI = f"{MATWERK}/rdf_files_metadata"

OBO = "http://purl.obolibrary.org/obo/"
NFDI = "https://nfdi.fiz-karlsruhe.de/ontology/"
VOID = "http://rdfs.org/ns/void#"
# flat, machine-friendly predicates (the namespace the zenodo catalog already uses)
STAT = f"{MATWERK}/msekg/stat/"

# --- VoID (statistics, standard vocabulary) ---
VOID_DATASET = URIRef(VOID + "Dataset")
VOID_TRIPLES = URIRef(VOID + "triples")
VOID_ENTITIES = URIRef(VOID + "entities")
VOID_SUBJECTS = URIRef(VOID + "distinctSubjects")
VOID_CLASSES = URIRef(VOID + "classes")
VOID_PROPERTIES = URIRef(VOID + "properties")
VOID_CLASS_PARTITION = URIRef(VOID + "classPartition")
VOID_PROPERTY_PARTITION = URIRef(VOID + "propertyPartition")
VOID_CLASS = URIRef(VOID + "class")
VOID_PROPERTY = URIRef(VOID + "property")
VOID_DATA_DUMP = URIRef(VOID + "dataDump")

# --- stat: predicates consumed by the registration sheet ---
S_REGISTERED_FROM = URIRef(STAT + "registeredFrom")
S_SOURCE_TYPE = URIRef(STAT + "sourceType")
S_SOURCE_FILE = URIRef(STAT + "sourceFile")
S_TRIPLES = URIRef(STAT + "triples")
S_VERSION = URIRef(STAT + "version")
S_HARVESTED_AT = URIRef(STAT + "harvestedAt")
S_LANDING = URIRef(STAT + "landingPage")

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _seg(s: str) -> str:
    return _SAFE.sub("_", s).strip("_")


def source_node_iri(kind: str, key: str) -> str:
    """``github``/``owner/repo`` -> .../matwerk/github/owner/repo;
    ``zenodo``/``4195050``     -> .../matwerk/zenodo/4195050."""
    if kind == "github":
        owner, repo = key.split("/", 1)
        return f"{GITHUB_ROOT}/{_seg(owner)}/{_seg(repo)}"
    return f"{ZENODO_ROOT}/{_seg(key)}"


def file_graph_iri(kind: str, key: str, path: str) -> str:
    return f"{source_node_iri(kind, key)}/{_seg(path.replace('/', '__'))}"


def _anyuri(v: str) -> Literal:
    return Literal(v, datatype=XSD.anyURI)


def _int(v: int) -> Literal:
    return Literal(int(v), datatype=XSD.integer)


def graph_stats(g: Graph, *, top: int = 20) -> dict:
    """Cheap VoID-style statistics for one harvested graph."""
    subs, preds, classes = set(), set(), {}
    pcount: dict = {}
    for s, p, o in g:
        subs.add(s)
        preds.add(p)
        pcount[p] = pcount.get(p, 0) + 1
        if p == RDF.type and isinstance(o, URIRef):
            classes[o] = classes.get(o, 0) + 1
    return {
        "triples": len(g),
        "subjects": len(subs),
        "predicates": len(preds),
        "classes": len(classes),
        "top_classes": [{"iri": str(k), "count": v}
                        for k, v in sorted(classes.items(), key=lambda x: -x[1])[:top]],
        "top_predicates": [{"iri": str(k), "count": v}
                           for k, v in sorted(pcount.items(), key=lambda x: -x[1])[:top]],
    }


def _description_text(*, sh: SourceHarvest, f, landing: str, stats: dict,
                      harvested_at: str) -> str:
    """The pipeline-style ``key: value`` + ``Statistics:`` block the graphs
    dashboard renders as a graph's metadata card."""
    return "\n".join(x for x in [
        f"Imported RDF graph from {sh.kind}.",
        f"source: {sh.key}",
        f"registered_as: {sh.registered_url}",
        f"file: {f.path}",
        f"version: {sh.version or ''}",
        f"landing_page: {landing}",
        f"download_url: {f.download_url}",
        f"content_type: {f.content_type}",
        f"sha256: {f.sha256}",
        f"harvested_at: {harvested_at}",
        "Statistics:",
        f"  triples: {stats['triples']}",
        f"  subjects: {stats['subjects']}",
        f"  predicates: {stats['predicates']}",
        f"  classes: {stats['classes']}",
    ] if x)


def _add_self_description(
    ctx: Graph, *, graph_iri: URIRef, sh: SourceHarvest, f, landing: str,
    stats: dict, harvested_at: str, T,
) -> None:
    """Write the graph's self-description INTO the named graph (pattern:
    docs/shapes/graph-metadata/pattern.md)."""
    proc = URIRef(f"{graph_iri}#load")
    treg = URIRef(f"{graph_iri}#tr")
    inst = URIRef(f"{graph_iri}#t0")
    dnode = URIRef(f"{graph_iri}#desc")
    fice = URIRef(f"{graph_iri}#source")

    # the graph — a continuant; it persists, it does not happen
    ctx.add((graph_iri, RDF.type, VOID_DATASET))
    ctx.add((graph_iri, RDF.type, URIRef(T.DATASET)))
    ctx.add((graph_iri, RDFS.label, Literal(f"{sh.key} — {posixpath.basename(f.path)}")))
    ctx.add((graph_iri, VOID_TRIPLES, _int(stats["triples"])))
    ctx.add((graph_iri, VOID_SUBJECTS, _int(stats["subjects"])))
    ctx.add((graph_iri, VOID_CLASSES, _int(stats["classes"])))
    ctx.add((graph_iri, VOID_PROPERTIES, _int(stats["predicates"])))
    ctx.add((graph_iri, VOID_DATA_DUMP, _anyuri(f.download_url)))
    ctx.add((graph_iri, URIRef(T.IS_OUTPUT_OF), proc))

    # the description — information, not an event
    ctx.add((graph_iri, URIRef(T.DENOTED_BY), dnode))
    ctx.add((dnode, RDF.type, URIRef(T.TEXTUAL_DESCRIPTION)))
    ctx.add((dnode, RDFS.label, Literal("description")))
    ctx.add((dnode, URIRef(T.HAS_VALUE),
             Literal(_description_text(sh=sh, f=f, landing=landing, stats=stats,
                                       harvested_at=harvested_at))))

    # the load — the only occurrent, so the only thing that may occupy time
    ctx.add((proc, RDF.type, URIRef(T.PROCESS)))
    ctx.add((proc, RDFS.label, Literal(f"graph load of {posixpath.basename(f.path)}")))
    ctx.add((proc, URIRef(T.OCCUPIES_TR), treg))
    ctx.add((proc, URIRef(T.HAS_INPUT), fice))
    ctx.add((treg, RDF.type, URIRef(T.TR_1D)))
    ctx.add((treg, URIRef(T.HAS_FIRST_INSTANT), inst))
    ctx.add((treg, URIRef(T.HAS_LAST_INSTANT), inst))
    ctx.add((inst, RDF.type, URIRef(T.TR_0D)))
    ctx.add((inst, URIRef(T.IN_XSD_DATETIMESTAMP),
             Literal(harvested_at, datatype=XSD.dateTimeStamp)))

    # the input file itself, identified by content hash
    ctx.add((fice, RDF.type, URIRef(T.FILE)))
    ctx.add((fice, RDFS.label, Literal(posixpath.basename(f.path))))
    ctx.add((fice, URIRef(T.HAS_URL), _anyuri(f.download_url)))
    ctx.add((fice, URIRef(T.HAS_VALUE), Literal(f"sha256:{f.sha256}")))

    # VoID partitions — per-class / per-predicate counts, answerable by SPARQL
    for c in stats["top_classes"]:
        part = BNode()
        ctx.add((graph_iri, VOID_CLASS_PARTITION, part))
        ctx.add((part, VOID_CLASS, URIRef(c["iri"])))
        ctx.add((part, VOID_ENTITIES, _int(c["count"])))
    for p in stats["top_predicates"]:
        part = BNode()
        ctx.add((graph_iri, VOID_PROPERTY_PARTITION, part))
        ctx.add((part, VOID_PROPERTY, URIRef(p["iri"])))
        ctx.add((part, VOID_TRIPLES, _int(p["count"])))


def add_source(
    ds: Dataset,
    meta: Graph,
    sh: SourceHarvest,
    kg_iri: Optional[str],
    harvested_at: str,
    *,
    mwo_version: Optional[str] = None,
) -> List[dict]:
    """Add one registration's file-graphs (to ``ds``) and registry entries (to ``meta``).

    ``kg_iri`` is the KG subject IRI when the repository/record is already an entity
    in the KG — that subject is then used as the source node, so the harvested
    graphs hang off what the KG already knows. Otherwise a node is minted and typed.
    """
    T = _mwo_terms(mwo_version)
    src_iri = URIRef(kg_iri) if kg_iri else URIRef(source_node_iri(sh.kind, sh.key))
    landing = sh.landing_url or (f"https://github.com/{sh.key}" if sh.kind == "github"
                                 else f"https://zenodo.org/records/{sh.key}")

    # --- source node (additive; never re-type a registered KG subject) ---
    meta.add((src_iri, RDFS.label, Literal(sh.key)))
    if not kg_iri:
        meta.add((src_iri, RDF.type,
                  URIRef(T.DATA_REPOSITORY if sh.kind == "github" else T.DATASET)))
    hp = URIRef(source_node_iri(sh.kind, sh.key) + "/landing")
    meta.add((src_iri, URIRef(T.DENOTED_BY), hp))
    meta.add((hp, RDF.type, URIRef(T.WEBSITE)))
    meta.add((hp, URIRef(T.HAS_URL), _anyuri(landing)))

    infos: List[dict] = []
    for f in sh.files:
        g_iri = URIRef(file_graph_iri(sh.kind, sh.key, f.path))

        # 1) the named graph = verbatim file triples + its self-description
        ctx = ds.graph(g_iri)
        for t in f.graph:
            ctx.add(t)
        stats = graph_stats(f.graph)
        _add_self_description(ctx, graph_iri=g_iri, sh=sh, f=f, landing=landing,
                              stats=stats, harvested_at=harvested_at, T=T)

        # 2) registry entry in the default graph: never lose the link to the source
        ver_iri = URIRef(f"{g_iri}/version")
        meta.add((src_iri, URIRef(T.HAS_PART), g_iri))
        meta.add((g_iri, RDF.type, URIRef(T.FILE)))
        meta.add((g_iri, RDFS.label, Literal(posixpath.basename(f.path))))
        meta.add((g_iri, URIRef(T.PART_OF), src_iri))
        meta.add((g_iri, URIRef(T.HAS_URL), _anyuri(f.download_url)))
        if sh.version:
            meta.add((g_iri, URIRef(T.DENOTED_BY), ver_iri))
            meta.add((ver_iri, RDF.type, URIRef(T.IDENTIFIER)))
            meta.add((ver_iri, RDFS.label, Literal("version")))
            meta.add((ver_iri, URIRef(T.HAS_VALUE), Literal(sh.version)))

        # 3) flat stat: view — what the registration sheet queries
        meta.add((g_iri, S_REGISTERED_FROM, Literal(sh.registered_url)))
        meta.add((g_iri, S_SOURCE_TYPE, Literal(sh.kind)))
        meta.add((g_iri, S_SOURCE_FILE, Literal(f.path)))
        meta.add((g_iri, S_TRIPLES, _int(stats["triples"])))
        meta.add((g_iri, S_HARVESTED_AT, Literal(harvested_at, datatype=XSD.dateTimeStamp)))
        meta.add((g_iri, S_LANDING, _anyuri(landing)))
        if sh.version:
            meta.add((g_iri, S_VERSION, Literal(sh.version)))

        infos.append({
            "path": f.path, "graph_iri": str(g_iri), "source": f.source,
            "download_url": f.download_url, "version": sh.version,
            "content_type": f.content_type, "sha256": f.sha256,
            "triples": stats["triples"], "subjects": stats["subjects"],
            "classes": stats["classes"], "predicates": stats["predicates"],
        })
    return infos


def finalize(ds: Dataset, meta: Graph) -> None:
    """Copy the accumulated registry triples into the dataset's default graph."""
    for t in meta:
        ds.add(t)
