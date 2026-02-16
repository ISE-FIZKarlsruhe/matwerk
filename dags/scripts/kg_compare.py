#!/usr/bin/env python3
"""
KG Comparator & Visualizer (enhanced)
-------------------------------------

Adds to the original features:
- Global statistics **written to CSV and LaTeX** for each KG and a side-by-side comparison table.
- **OWL/DL construct presence** checks (per KG) written to CSV and LaTeX, plus a comparison CSV.
- Keeps previous outputs (text summaries, types/predicates CSVs, ego-subgraph PNG/TTL, etc.).

Example:

python3 scripts/kg_compare.py --kg1 data/all_NotReasoned.ttl --name1 BFO_MSE --kg2 data/MSE_KG_old/mse_v1.ttl --name2 SCHEMA_MSE --label "Ebrahim Norouzi" --type schema:Person --out data/compare_kgs

Notes
- CURIEs are supported if declared in the KG or provided with --prefix (e.g., --prefix schema=https://schema.org/).
- If pygraphviz is installed, drawings use Graphviz; otherwise matplotlib.
- LaTeX files contain standalone tabular environments (no preamble). You can \input{} them in your paper.
"""

import argparse
import csv
import os
from collections import Counter

from rdflib import Graph, URIRef, BNode, Literal
from rdflib.namespace import RDF, RDFS, OWL
import logging

# Optional reasoning (RDFS / OWL 2 RL)
_HAS_OWL_RL = False
try:
    from owlrl import DeductiveClosure, RDFS_Semantics, OWLRL_Semantics
    _HAS_OWL_RL = True
except Exception:
    pass

# Optional plotting backends
_HAS_PYGRAPHVIZ = False
try:
    import networkx as nx
    from networkx.drawing.nx_agraph import to_agraph  # requires pygraphviz
    _HAS_PYGRAPHVIZ = True
except Exception:
    try:
        import networkx as nx
        import matplotlib.pyplot as plt
    except Exception:
        nx = None
        plt = None

# Quiet rdflib's noisy literal-casting warnings (dateTime/decimal, etc.)
logging.getLogger("rdflib").setLevel(logging.ERROR)
logging.getLogger("rdflib.term").setLevel(logging.ERROR)

# ----------------------------
# FS helpers
# ----------------------------

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

# ----------------------------
# CURIE/QName helpers
# ----------------------------

def curie_or_qname(g: Graph, term):
    try:
        return g.namespace_manager.normalizeUri(term)
    except Exception:
        return str(term)


def parse_term(term_str: str, g: Graph):
    """Parse a string as URIRef or expand CURIE using graph namespaces."""
    if term_str.startswith("http://") or term_str.startswith("https://"):
        return URIRef(term_str)
    try:
        return g.namespace_manager.expand_curie(term_str)
    except Exception:
        return URIRef(term_str)

# ----------------------------
# DL construct checks (ported from gold_dataset.py)
# ----------------------------

RDFNS  = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
RDFSNS = URIRef("http://www.w3.org/2000/01/rdf-schema#")
OWLNS  = URIRef("http://www.w3.org/2002/07/owl#")

RDF_  = RDF
RDFS_ = RDFS
OWL_  = OWL


def _any(g, s=None, p=None, o=None, cond=None) -> bool:
    """Boolean: does any triple match (s, p, o) and optional cond?"""
    for (ss, pp, oo) in g.triples((s, p, o)):
        if cond is None or cond(ss, pp, oo):
            return True
    return False


def _count(g, s=None, p=None, o=None, cond=None) -> int:
    """Count how many triples match (s, p, o) and optional cond."""
    n = 0
    for (ss, pp, oo) in g.triples((s, p, o)):
        if cond is None or cond(ss, pp, oo):
            n += 1
    return n



def _is_uri(x):
    return isinstance(x, (URIRef, BNode)) and not isinstance(x, Literal)


def _is_lit(x):
    return isinstance(x, Literal)


# Individual construct predicates (now returning counts instead of booleans)

def count_construct_ROLE_INVERSE(g: Graph) -> int:
    return (
        _count(g, None, OWL.inverseOf, None)
        + _count(g, None, RDF.type, OWL.SymmetricProperty)
        + _count(g, None, RDF.type, OWL.InverseFunctionalProperty)
    )


def count_construct_D(g: Graph) -> int:
    c = 0
    # Datatype properties
    c += _count(g, None, RDF.type, OWL.DatatypeProperty)
    # Datatype complement
    c += _count(g, None, OWL.datatypeComplementOf, None)
    # OneOf (datatype enumerations – count how many lists have a literal head)
    for _x, _p, coll in g.triples((None, OWL.oneOf, None)):
        c += _count(g, coll, RDF.first, None, cond=lambda s, p, o: _is_lit(o))
    # Datatype restrictions: count onDatatype axioms
    c += _count(g, None, OWL.onDatatype, None)
    # hasValue with literals
    c += _count(g, None, OWL.hasValue, None, cond=lambda s, p, o: _is_lit(o))
    return c


def count_construct_CONCEPT_INTERSECTION(g: Graph) -> int:
    return _count(g, None, OWL.intersectionOf, None)


def count_construct_CONCEPT_UNION(g: Graph) -> int:
    c = 0
    c += _count(g, None, OWL.unionOf, None)
    # Class enumerations via oneOf with URI elements
    for _x, _p, coll in g.triples((None, OWL.oneOf, None)):
        c += _count(g, coll, RDF.first, None, cond=lambda s, p, o: _is_uri(o))
    c += _count(g, None, OWL.differentFrom, None)
    c += _count(g, None, RDF.type, OWL.AllDifferent)
    c += _count(g, None, OWL.disjointUnionOf, None)
    return c


def count_construct_CONCEPT_COMPLEX_NEGATION(g: Graph) -> int:
    return (
        _count(g, None, OWL.complementOf, None)
        + _count(g, None, OWL.disjointWith, None)
        + _count(g, None, RDF.type, OWL.AllDisjointClasses)
        + _count(g, None, OWL.differentFrom, None)
        + _count(g, None, RDF.type, OWL.AllDifferent)
        + _count(g, None, OWL.disjointUnionOf, None)
    )


def count_construct_FULL_EXISTENTIAL(g: Graph) -> int:
    return (
        _count(g, None, OWL.someValuesFrom, None, cond=lambda s, p, o: (o != OWL.Thing))
        + _count(g, None, OWL.hasValue, None, cond=lambda s, p, o: _is_uri(o))
    )


def count_construct_LIMITED_EXISTENTIAL(g: Graph) -> int:
    return _count(g, None, OWL.someValuesFrom, OWL.Thing)


def count_construct_UNIVERSAL_RESTRICTION(g: Graph) -> int:
    return _count(g, None, OWL.allValuesFrom, None)


def count_construct_NOMINALS(g: Graph) -> int:
    c = 0
    # hasValue with URI objects
    c += _count(g, None, OWL.hasValue, None, cond=lambda s, p, o: _is_uri(o))
    # OneOf with URIs
    for _x, _p, coll in g.triples((None, OWL.oneOf, None)):
        c += _count(g, coll, RDF.first, None, cond=lambda s, p, o: _is_uri(o))
    # sameAs / differentFrom / AllDifferent
    c += _count(g, None, OWL.differentFrom, None)
    c += _count(g, None, RDF.type, OWL.AllDifferent)
    c += _count(g, None, OWL.sameAs, None)
    return c


def count_construct_Q(g: Graph) -> int:
    """Qualified cardinalities (onClass != owl:Thing, onDataRange != rdfs:Literal)."""
    c = 0
    # minQualifiedCardinality
    for r, _, _ in g.triples((None, OWL.minQualifiedCardinality, None)):
        if ((r, OWL.onClass, OWL.Thing) not in g and (r, OWL.onClass, None) in g) or \
           ((r, OWL.onDataRange, RDFS.Literal) not in g and (r, OWL.onDataRange, None) in g):
            c += 1
    # qualifiedCardinality
    for r, _, _ in g.triples((None, OWL.qualifiedCardinality, None)):
        if ((r, OWL.onClass, OWL.Thing) not in g and (r, OWL.onClass, None) in g) or \
           ((r, OWL.onDataRange, RDFS.Literal) not in g and (r, OWL.onDataRange, None) in g):
            c += 1
    # maxQualifiedCardinality
    for r, _, _ in g.triples((None, OWL.maxQualifiedCardinality, None)):
        if ((r, OWL.onClass, OWL.Thing) not in g and (r, OWL.onClass, None) in g) or \
           ((r, OWL.onDataRange, RDFS.Literal) not in g and (r, OWL.onDataRange, None) in g):
            c += 1
    return c


def count_construct_N(g: Graph) -> int:
    """Unqualified number restrictions (and qualified ones with top concepts)."""
    c = 0
    # Standard unqualified cardinalities
    c += _count(g, None, OWL.minCardinality, None)
    c += _count(g, None, OWL.cardinality, None)
    c += _count(g, None, OWL.maxCardinality, None)

    # Qualified cardinalities where qualifier is owl:Thing / rdfs:Literal
    for r, _, _ in g.triples((None, OWL.minQualifiedCardinality, None)):
        if (r, OWL.onClass, OWL.Thing) in g or (r, OWL.onDataRange, RDFS.Literal) in g:
            c += 1
    for r, _, _ in g.triples((None, OWL.qualifiedCardinality, None)):
        if (r, OWL.onClass, OWL.Thing) in g or (r, OWL.onDataRange, RDFS.Literal) in g:
            c += 1
    for r, _, _ in g.triples((None, OWL.maxQualifiedCardinality, None)):
        if (r, OWL.onClass, OWL.Thing) in g or (r, OWL.onDataRange, RDFS.Literal) in g:
            c += 1
    return c


def count_construct_ROLE_COMPLEX(g: Graph) -> int:
    return (
        _count(g, None, OWL.hasSelf, None)
        + _count(g, None, RDF.type, OWL.AsymmetricProperty)
        + _count(g, None, OWL.propertyDisjointWith, None)
        + _count(g, None, RDF.type, OWL.AllDisjointProperties)
        + _count(g, None, RDF.type, OWL.IrreflexiveProperty)
    )


def count_construct_ROLE_REFLEXIVITY_CHAINS(g: Graph) -> int:
    return (
        _count(g, None, RDF.type, OWL.ReflexiveProperty)
        + _count(g, None, OWL.propertyChainAxiom, None)
    )


def count_construct_ROLE_DOMAIN_RANGE(g: Graph) -> int:
    return (
        _count(g, None, RDFS.domain, None)
        + _count(g, None, RDFS.range, None)
    )


def count_construct_ROLE_HIERARCHY(g: Graph) -> int:
    return (
        _count(g, None, OWL.equivalentProperty, None)
        + _count(g, None, RDFS.subPropertyOf, None)
    )


def count_construct_F(g: Graph) -> int:
    return (
        _count(g, None, RDF.type, OWL.FunctionalProperty)
        + _count(g, None, RDF.type, OWL.InverseFunctionalProperty)
    )


def count_construct_ROLE_TRANSITIVE(g: Graph) -> int:
    return _count(g, None, RDF.type, OWL.TransitiveProperty)


_CONSTRUCT_FUNCS = {
    "ROLE_INVERSE": count_construct_ROLE_INVERSE,
    "D": count_construct_D,
    "CONCEPT_INTERSECTION": count_construct_CONCEPT_INTERSECTION,
    "CONCEPT_UNION": count_construct_CONCEPT_UNION,
    "CONCEPT_COMPLEX_NEGATION": count_construct_CONCEPT_COMPLEX_NEGATION,
    "FULL_EXISTENTIAL": count_construct_FULL_EXISTENTIAL,
    "LIMITED_EXISTENTIAL": count_construct_LIMITED_EXISTENTIAL,
    "UNIVERSAL_RESTRICTION": count_construct_UNIVERSAL_RESTRICTION,
    "NOMINALS": count_construct_NOMINALS,
    "Q": count_construct_Q,
    "N": count_construct_N,
    "ROLE_COMPLEX": count_construct_ROLE_COMPLEX,
    "ROLE_REFLEXIVITY_CHAINS": count_construct_ROLE_REFLEXIVITY_CHAINS,
    "ROLE_DOMAIN_RANGE": count_construct_ROLE_DOMAIN_RANGE,
    "ROLE_HIERARCHY": count_construct_ROLE_HIERARCHY,
    "F": count_construct_F,
    "ROLE_TRANSITIVE": count_construct_ROLE_TRANSITIVE,
}
CONSTRUCT_NAMES = list(_CONSTRUCT_FUNCS.keys())


def compute_constructs(g: Graph) -> dict:
    """Return a dict: {construct_name: count}."""
    return {name: int(fn(g)) for name, fn in _CONSTRUCT_FUNCS.items()}

# ----------------------------
# Core statistics
# ----------------------------

def class_property_sets(g: Graph):
    classes = set(s for s, _, _ in g.triples((None, RDF.type, OWL.Class)))
    classes.update(s for s, _, _ in g.triples((None, RDF.type, RDFS.Class)))
    properties = set(s for s, _, _ in g.triples((None, RDF.type, RDF.Property)))
    properties.update(s for s, _, _ in g.triples((None, RDF.type, OWL.ObjectProperty)))
    properties.update(s for s, _, _ in g.triples((None, RDF.type, OWL.DatatypeProperty)))
    properties.update(s for s, _, _ in g.triples((None, RDF.type, OWL.AnnotationProperty)))
    return classes, properties


def graph_stats(g: Graph):
    triples = len(g)
    subjects = set()
    objects = set()
    predicates = set()

    literals = 0
    resources = set()

    for s, p, o in g:
        subjects.add(s)
        predicates.add(p)
        objects.add(o)
        resources.add(s)
        resources.add(p)
        if isinstance(o, Literal):
            literals += 1
        else:
            resources.add(o)

    typed_subjects = set(s for s, _, _ in g.triples((None, RDF.type, None)))

    classes = set(s for s, _, _ in g.triples((None, RDF.type, OWL.Class)))
    classes.update(s for s, _, _ in g.triples((None, RDF.type, RDFS.Class)))

    properties = set(s for s, _, _ in g.triples((None, RDF.type, RDF.Property)))
    properties.update(s for s, _, _ in g.triples((None, RDF.type, OWL.ObjectProperty)))
    properties.update(s for s, _, _ in g.triples((None, RDF.type, OWL.DatatypeProperty)))
    properties.update(s for s, _, _ in g.triples((None, RDF.type, OWL.AnnotationProperty)))

    individual_candidates = set()
    for s in typed_subjects:
        if s not in classes and s not in properties:
            individual_candidates.add(s)

    type_counts = Counter()
    for s in individual_candidates:
        for _, _, t in g.triples((s, RDF.type, None)):
            type_counts[t] += 1

    pred_counts = Counter()
    for _, p, _ in g:
        pred_counts[p] += 1

    return {
        "triples": triples,
        "num_subjects": len(subjects),
        "num_objects": len(objects),
        "num_predicates": len(predicates),
        "num_literals": literals,
        "num_resources": len(resources),
        "num_classes": len(classes),
        "num_properties": len(properties),
        "num_individuals": len(individual_candidates),
        "type_counts": type_counts,
        "predicate_counts": pred_counts,
    }

# ----------------------------
# Label-based instance extraction
# ----------------------------

def _normalize_label(s: str) -> str:
    return " ".join(str(s).strip().split()).casefold()

def find_instances_by_label_and_type(g: Graph, label: str, rdf_types, lang: str | None = None):
    """Find instances whose rdfs:label matches `label` (case/space-insensitive)
    and whose rdf:type is in `rdf_types` (if provided)."""
    if rdf_types is None:
        rdf_types = []
    if isinstance(rdf_types, (URIRef,)):
        rdf_types = [rdf_types]
    rdf_types = set(rdf_types)
    want = _normalize_label(label)
    matches = []
    for s, _, o in g.triples((None, RDFS.label, None)):
        if isinstance(o, Literal):
            if lang is not None and o.language != lang:
                continue
            if _normalize_label(o) == want:
                if not rdf_types:
                    matches.append(s)
                else:
                    for t in rdf_types:
                        if (s, RDF.type, t) in g:
                            matches.append(s)
                            break
    return matches

# ----------------------------
# Subgraph & drawing
# ----------------------------

def build_multihop_triples(
    g: Graph,
    centers,
    hops: int = 2,
    limit_triples: int = 5000,
    direction: str = "both",
    only_individuals: bool = False,
    classes=None,
    properties=None,
):
    if not isinstance(centers, (list, set, tuple)):
        centers = [centers]

    if only_individuals and (classes is None or properties is None):
        classes, properties = class_property_sets(g)

    def is_schema_node(n):
        if isinstance(n, Literal):
            return False
        return (n in classes) or (n in properties)

    frontier = set(centers)
    visited_nodes = set(centers)
    collected = set()

    def maybe_add_triple(s, p, o):
        if only_individuals:
            s_bad = is_schema_node(s)
            o_bad = (not isinstance(o, Literal)) and is_schema_node(o)
            if s_bad or o_bad:
                return False
        collected.add((s, p, o))
        return True

    depth = 0
    while frontier and (hops < 0 or depth <= hops):
        if len(collected) >= limit_triples:
            break
        next_front = set()

        for node in list(frontier):
            if direction in ("out", "both"):
                for s, p, o in g.triples((node, None, None)):
                    added = maybe_add_triple(s, p, o)
                    if added and not isinstance(o, Literal):
                        if (not only_individuals) or (o not in classes and o not in properties):
                            if o not in visited_nodes:
                                next_front.add(o)
                    if len(collected) >= limit_triples:
                        break
            if len(collected) >= limit_triples:
                break
            if direction in ("in", "both"):
                for s, p, o in g.triples((None, None, node)):
                    added = maybe_add_triple(s, p, o)
                    if added and not isinstance(s, Literal):
                        if (not only_individuals) or (s not in classes and s not in properties):
                            if s not in visited_nodes:
                                next_front.add(s)
                    if len(collected) >= limit_triples:
                        break
            if len(collected) >= limit_triples:
                break

        visited_nodes.update(frontier)
        frontier = next_front - visited_nodes
        depth += 1

    return list(collected)


def triples_to_networkx(g: Graph, triples):
    if nx is None:
        raise RuntimeError("networkx/matplotlib not available in this environment")
    G = nx.MultiDiGraph()
    for s, p, o in triples:
        s_label = curie_or_qname(g, s)
        p_label = curie_or_qname(g, p)
        if isinstance(o, Literal):
            o_label = f'"{o}"'
            G.add_node(s_label, kind="resource")
            G.add_node(o_label, kind="literal")
        else:
            o_label = curie_or_qname(g, o)
            G.add_node(s_label, kind="resource")
            G.add_node(o_label, kind="resource")
        G.add_edge(s_label, o_label, label=p_label)
    return G


def draw_graph(G, out_png: str, title: str = ""):
    ensure_dir(os.path.dirname(out_png))
    if _HAS_PYGRAPHVIZ:
        A = to_agraph(G)
        for n in G.nodes:
            kind = G.nodes[n].get("kind", "resource")
            node = A.get_node(n)
            if kind == "literal":
                node.attr.update(shape="box", style="rounded")
            else:
                node.attr.update(shape="ellipse")
        for u, v, key, data in G.edges(keys=True, data=True):
            e = A.get_edge(u, v)
            e.attr["label"] = data.get("label", "")
        A.graph_attr.update(rankdir="LR")
        A.layout("dot")
        A.draw(out_png)
        return
    if nx is None or plt is None:
        raise RuntimeError("Cannot draw graph: pygraphviz and matplotlib backends unavailable")
    pos = nx.spring_layout(G, seed=42)
    nx.draw_networkx_nodes(G, pos, node_size=900)
    nx.draw_networkx_labels(G, pos, font_size=8)
    nx.draw_networkx_edges(G, pos, arrows=True)
    edge_labels = {(u, v): d.get("label", "") for u, v, d in G.edges(data=True)}
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=7)
    plt.title(title)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close()

# ----------------------------
# Exports (TXT/CSV/LaTeX)
# ----------------------------

def write_stats_txt(path: str, name: str, stats: dict, g: Graph):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"=== Global stats: {name} ===\n")
        for k in [
            "triples",
            "num_subjects",
            "num_objects",
            "num_predicates",
            "num_literals",
            "num_resources",
            "num_classes",
            "num_properties",
            "num_individuals",
        ]:
            f.write(f"{k}: {stats[k]}\n")
        f.write("\nTop 25 types (individual counts):\n")
        for t, c in stats["type_counts"].most_common(25):
            f.write(f"  {curie_or_qname(g, t)}: {c}\n")
        f.write("\nTop 25 predicates:\n")
        for p, c in stats["predicate_counts"].most_common(25):
            f.write(f"  {curie_or_qname(g, p)}: {c}\n")


def write_counter_csv(path: str, counter: Counter, g: Graph, header=("term", "count")):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for term, c in counter.most_common():
            w.writerow([curie_or_qname(g, term), c])


def write_ego_stats(path: str, name: str, center, ego_triples, g: Graph):
    ensure_dir(os.path.dirname(path))
    preds = Counter([p for _, p, _ in ego_triples])
    neighbors = set()
    literals = 0
    for s, p, o in ego_triples:
        if isinstance(o, Literal):
            literals += 1
        else:
            neighbors.add(o)
        neighbors.add(s)
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"=== Ego stats: {name} ===\n")
        f.write(f"center: {curie_or_qname(g, center)}\n")
        f.write(f"triples: {len(ego_triples)}\n")
        f.write(f"unique neighbors (incl. center): {len(neighbors)}\n")
        f.write(f"literals: {literals}\n")
        f.write("predicate frequency:\n")
        for p, c in preds.most_common():
            f.write(f"  {curie_or_qname(g, p)}: {c}\n")


def write_subgraph_ttl(path: str, triples, g: Graph):
    ensure_dir(os.path.dirname(path))
    sg = Graph()
    for prefix, ns in g.namespace_manager.namespaces():
        sg.namespace_manager.bind(prefix, ns, replace=True)
    for s, p, o in triples:
        sg.add((s, p, o))
    sg.serialize(destination=path, format="turtle")

# --- New: CSV & LaTeX writers for global stats / constructs ---

_GLOBAL_ORDER = [
    "triples",
    "num_subjects",
    "num_objects",
    "num_predicates",
    "num_literals",
    "num_resources",
    "num_classes",
    "num_properties",
    "num_individuals",
]


def write_global_stats_csv(path: str, name: str, stats: dict):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metric", name])
        for k in _GLOBAL_ORDER:
            w.writerow([k, stats[k]])


def _latex_escape(s: str) -> str:
    return (
        str(s)
        .replace("\\", "\\textbackslash{}")
        .replace("_", "\\_")
        .replace("%", "\\%")
        .replace("&", "\\&")
        .replace("#", "\\#")
        .replace("{", "\\{")
        .replace("}", "\\}")
    )


def write_global_stats_latex(path: str, name1: str, stats1: dict, name2: str, stats2: dict):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write("% Auto-generated by kg_compare.py\n")
        f.write("\\begin{tabular}{lrr}\\hline\n")
        f.write(f"Metric & {_latex_escape(name1)} & {_latex_escape(name2)} \\\\ \\hline\n")
        for k in _GLOBAL_ORDER:
            f.write(f"{_latex_escape(k)} & {stats1[k]} & {stats2[k]} \\\\\n")
        f.write("\\hline\\end{tabular}\n")


def write_constructs_csv(path: str, name: str, constructs: dict):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["construct", name])
        for c in CONSTRUCT_NAMES:
            w.writerow([c, int(constructs.get(c, 0))])


def write_constructs_compare_csv(path: str, name1: str, constructs1: dict, name2: str, constructs2: dict):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["construct", name1, name2])
        for c in CONSTRUCT_NAMES:
            w.writerow([
                c,
                int(constructs1.get(c, 0)),
                int(constructs2.get(c, 0)),
            ])


def write_constructs_latex(path: str, name1: str, constructs1: dict, name2: str, constructs2: dict):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write("% Auto-generated by kg_compare.py\n")
        f.write("\\begin{tabular}{lrr}\\hline\n")
        f.write(f"Construct & {_latex_escape(name1)} & {_latex_escape(name2)} \\\\ \\hline\n")
        for c in CONSTRUCT_NAMES:
            v1 = int(constructs1.get(c, 0))
            v2 = int(constructs2.get(c, 0))
            f.write(f"{_latex_escape(c)} & {v1} & {v2} \\\\\n")
        f.write("\\hline\\end{tabular}\n")

# ----------------------------
# Main
# ----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kg1", required=True, help="Path to first Turtle file")
    ap.add_argument("--kg2", required=True, help="Path to second Turtle file")
    ap.add_argument("--name1", default="KG1", help="Short name for first KG")
    ap.add_argument("--name2", default="KG2", help="Short name for second KG")
    ap.add_argument("--prefix", action="append", default=[], help="Extra CURIE prefix mapping, format prefix=IRI e.g. schema=https://schema.org/")
    ap.add_argument("--label", help="Exact rdfs:label text to search")
    ap.add_argument("--type", help="Type IRI or CURIE for both KGs (used if --type1/--type2 not supplied)")
    ap.add_argument("--type1", help="Type IRI or CURIE for KG1 only", default=None)
    ap.add_argument("--type2", help="Type IRI or CURIE for KG2 only", default=None)
    ap.add_argument("--lang", default=None, help="Language tag to filter labels (e.g., en)")
    ap.add_argument("--out", default="out", help="Output directory")
    ap.add_argument("--hops", type=int, default=2, help="Number of hops to expand for visualization (-1 for unlimited)")
    ap.add_argument("--limit", type=int, default=5000, help="Max triples to collect during expansion")
    ap.add_argument("--direction", choices=["both", "out", "in"], default="both", help="Traversal direction for expansion")
    ap.add_argument("--only-individuals", action="store_true", help="Skip class/property nodes during expansion and output (keep literals)")
    # Reasoner settings
    ap.add_argument("--reasoner", choices=["none", "rdfs", "owlrl"], default="none", help="Apply a forward-chaining reasoner before computing statistics")
    ap.add_argument("--reasoner-axioms", action="store_true", help="Add axioms (TBox rules) when reasoning (owlrl only)")
    ap.add_argument("--reasoner-daxioms", action="store_true", help="Add datatype axioms when reasoning (owlrl only)")

    args = ap.parse_args()

    ensure_dir(args.out)

    # Parse graphs (supports TTL; extend if needed)
    g1 = Graph(); g1.parse(args.kg1, format="turtle")
    g2 = Graph(); g2.parse(args.kg2, format="turtle")

    # Inject extra prefixes if provided
    for pdef in args.prefix:
        if "=" in pdef:
            pref, iri = pdef.split("=", 1)
            g1.namespace_manager.bind(pref, URIRef(iri), replace=True)
            g2.namespace_manager.bind(pref, URIRef(iri), replace=True)

    # Precompute class/property sets for filtering if needed
    classes1, properties1 = class_property_sets(g1)
    classes2, properties2 = class_property_sets(g2)

    # Apply reasoning if requested
    if args.reasoner != "none":
        if not _HAS_OWL_RL:
            print("[WARN] owlrl not installed; skipping reasoning. pip install owlrl")
        else:
            if args.reasoner == "rdfs":
                print("[INFO] Applying RDFS reasoning…")
                DeductiveClosure(RDFS_Semantics).expand(g1)
                DeductiveClosure(RDFS_Semantics).expand(g2)
            elif args.reasoner == "owlrl":
                print("[INFO] Applying OWL 2 RL reasoning…")
                # Compatibility shim: older owlrl lacks axioms/daxioms kwargs
                try:
                    DeductiveClosure(
                        OWLRL_Semantics,
                        axioms=bool(args.reasoner_axioms),
                        daxioms=bool(args.reasoner_daxioms),
                    ).expand(g1)
                    DeductiveClosure(
                        OWLRL_Semantics,
                        axioms=bool(args.reasoner_axioms),
                        daxioms=bool(args.reasoner_daxioms),
                    ).expand(g2)
                except TypeError:
                    print("[WARN] owlrl version does not support axioms/daxioms; using defaults")
                    DeductiveClosure(OWLRL_Semantics).expand(g1)
                    DeductiveClosure(OWLRL_Semantics).expand(g2)


    # Global stats (post-reasoning if enabled)
    s1 = graph_stats(g1)
    s2 = graph_stats(g2)

    # Construct presence
    c1 = compute_constructs(g1)
    c2 = compute_constructs(g2)

    # Legacy text + per-counter CSVs
    write_stats_txt(os.path.join(args.out, f"{args.name1}_stats.txt"), args.name1, s1, g1)
    write_stats_txt(os.path.join(args.out, f"{args.name2}_stats.txt"), args.name2, s2, g2)
    write_counter_csv(os.path.join(args.out, f"{args.name1}_types.csv"), s1["type_counts"], g1, ("type", "count"))
    write_counter_csv(os.path.join(args.out, f"{args.name2}_types.csv"), s2["type_counts"], g2, ("type", "count"))
    write_counter_csv(os.path.join(args.out, f"{args.name1}_predicates.csv"), s1["predicate_counts"], g1, ("predicate", "count"))
    write_counter_csv(os.path.join(args.out, f"{args.name2}_predicates.csv"), s2["predicate_counts"], g2, ("predicate", "count"))

    # NEW: Global stats CSV + LaTeX (comparison)
    write_global_stats_csv(os.path.join(args.out, f"{args.name1}_global_stats.csv"), args.name1, s1)
    write_global_stats_csv(os.path.join(args.out, f"{args.name2}_global_stats.csv"), args.name2, s2)
    write_global_stats_latex(os.path.join(args.out, "global_stats_compare.tex"), args.name1, s1, args.name2, s2)

    # NEW: Constructs CSVs + LaTeX
    write_constructs_csv(os.path.join(args.out, f"{args.name1}_constructs.csv"), args.name1, c1)
    write_constructs_csv(os.path.join(args.out, f"{args.name2}_constructs.csv"), args.name2, c2)
    write_constructs_compare_csv(os.path.join(args.out, "constructs_compare.csv"), args.name1, c1, args.name2, c2)
    write_constructs_latex(os.path.join(args.out, "constructs_compare.tex"), args.name1, c1, args.name2, c2)

    # Optional label-centered ego graphs
    if args.label:
        t1_terms = []
        t2_terms = []
        if args.type1 or args.type2:
            if args.type1:
                t1_terms = [parse_term(args.type1, g1)]
            if args.type2:
                t2_terms = [parse_term(args.type2, g2)]
        elif args.type:
            t1_terms = [parse_term(args.type, g1)]
            t2_terms = [parse_term(args.type, g2)]

        m1 = find_instances_by_label_and_type(g1, args.label, t1_terms, lang=args.lang)
        m2 = find_instances_by_label_and_type(g2, args.label, t2_terms, lang=args.lang)

        if not m1 and not m2:
            print("No matching instances in either KG for the given label/type(s). Check --label/--lang/--type.")
        else:
            if m1:
                c = m1[0]
                triples1 = build_multihop_triples(
                    g1,
                    c,
                    hops=args.hops,
                    limit_triples=args.limit,
                    direction=args.direction,
                    only_individuals=args.only_individuals,
                    classes=classes1,
                    properties=properties1,
                )
                G1 = triples_to_networkx(g1, triples1)
                png1 = os.path.join(args.out, f"{args.name1}_{args.label}_h{args.hops}.png").replace(" ", "_")
                draw_graph(G1, png1, title=f"{args.name1}: {args.label} (hops={args.hops})")
                stats1_path = os.path.join(args.out, f"{args.name1}_{args.label}_h{args.hops}.txt").replace(" ", "_")
                write_ego_stats(stats1_path, args.name1, c, triples1, g1)
                ttl1 = os.path.join(args.out, f"{args.name1}_{args.label}_h{args.hops}.ttl").replace(" ", "_")
                write_subgraph_ttl(ttl1, triples1, g1)
            if m2:
                c = m2[0]
                triples2 = build_multihop_triples(
                    g2,
                    c,
                    hops=args.hops,
                    limit_triples=args.limit,
                    direction=args.direction,
                    only_individuals=args.only_individuals,
                    classes=classes2,
                    properties=properties2,
                )
                G2 = triples_to_networkx(g2, triples2)
                png2 = os.path.join(args.out, f"{args.name2}_{args.label}_h{args.hops}.png").replace(" ", "_")
                draw_graph(G2, png2, title=f"{args.name2}: {args.label} (hops={args.hops})")
                stats2_path = os.path.join(args.out, f"{args.name2}_{args.label}_h{args.hops}.txt").replace(" ", "_")
                write_ego_stats(stats2_path, args.name2, c, triples2, g2)
                ttl2 = os.path.join(args.out, f"{args.name2}_{args.label}_h{args.hops}.ttl").replace(" ", "_")
                write_subgraph_ttl(ttl2, triples2, g2)

    # Summary text comparison
    with open(os.path.join(args.out, "comparison_summary.txt"), "w", encoding="utf-8") as f:
        f.write("=== High-level comparison ===\n")
        if args.reasoner != "none":
            f.write(f"Reasoner: {args.reasoner} (axioms={bool(args.reasoner_axioms)}, daxioms={bool(args.reasoner_daxioms)})\n\n")
        def line(metric):
            return f"{metric}: {args.name1}={s1[metric]} | {args.name2}={s2[metric]}\n"
        for m in _GLOBAL_ORDER:
            f.write(line(m))
        f.write("\nCSV: *_global_stats.csv, *_constructs.csv, constructs_compare.csv\n")
        f.write("LaTeX: global_stats_compare.tex, constructs_compare.tex\n")


if __name__ == "__main__":
    main()
