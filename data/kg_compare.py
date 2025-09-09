#!/usr/bin/env python3
"""
KG Comparator & Visualizer
--------------------------

Compares two Turtle RDF knowledge graphs and generates:
- Global statistics for each KG
- Side-by-side class and predicate frequency CSVs
- A focused visualization + stats for a chosen instance selected by (rdfs:label, rdf:type)

Usage examples:

python kg_compare.py \
    --kg1 data/all.ttl --name1 BFO_MSE \
    --kg2 data/MSE_KG_old/all.ttl --name2 SCHEMA_MSE \
    --label "Ebrahim Norouzi" --type schema:Person \
    --out out

Notes
- CURIEs (e.g., schema:Person) are supported if the CURIE prefix is declared in the graph data or passed with --prefix.
- If Graphviz/pygraphviz is available, the PNGs will look nicer. Otherwise it falls back to matplotlib.
- The tool attempts language-insensitive label matching; you can force an exact language with --lang.
"""

import argparse
import csv
import os
from collections import Counter, defaultdict

from rdflib import Graph, URIRef, BNode, Literal
from rdflib.namespace import RDF, RDFS, OWL

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


# ----------------------------
# Helpers
# ----------------------------

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def curie_or_qname(g: Graph, term):
    try:
        return g.namespace_manager.normalizeUri(term)
    except Exception:
        return str(term)


def parse_term(term_str: str, g: Graph):
    """Parse a string as URIRef or expand CURIE using graph namespaces."""
    if term_str.startswith("http://") or term_str.startswith("https://"):
        return URIRef(term_str)
    # attempt CURIE expansion
    try:
        return g.namespace_manager.expand_curie(term_str)
    except Exception:
        # fall back: treat as URI
        return URIRef(term_str)


# ----------------------------
# Statistics
# ----------------------------

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

    # Individuals: subjects that have an rdf:type that is not a class definition triple
    # We'll count any subject with any rdf:type as an individual, EXCEPT when that subject is itself a Class/Property/AnnotationProperty/etc
    typed_subjects = set(s for s, _, _ in g.triples((None, RDF.type, None)))

    # Identify classes and properties
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

    # type distribution for individuals
    type_counts = Counter()
    for s in individual_candidates:
        for _, _, t in g.triples((s, RDF.type, None)):
            type_counts[t] += 1

    # predicate frequency
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


# Helper for class/property sets

def class_property_sets(g: Graph):
    classes = set(s for s, _, _ in g.triples((None, RDF.type, OWL.Class)))
    classes.update(s for s, _, _ in g.triples((None, RDF.type, RDFS.Class)))
    properties = set(s for s, _, _ in g.triples((None, RDF.type, RDF.Property)))
    properties.update(s for s, _, _ in g.triples((None, RDF.type, OWL.ObjectProperty)))
    properties.update(s for s, _, _ in g.triples((None, RDF.type, OWL.DatatypeProperty)))
    properties.update(s for s, _, _ in g.triples((None, RDF.type, OWL.AnnotationProperty)))
    return classes, properties

# ----------------------------
# Label-based instance extraction
# ----------------------------

def find_instances_by_label_and_type(g: Graph, label: str, rdf_types, lang: str | None = None):
    """Find instances whose rdfs:label == label and rdf:type is in rdf_types.
    rdf_types can be a single URIRef or an iterable of URIRefs."""
    if rdf_types is None:
        rdf_types = []
    if isinstance(rdf_types, (URIRef,)):
        rdf_types = [rdf_types]
    rdf_types = set(rdf_types)
    matches = []
    for s, _, o in g.triples((None, RDFS.label, None)):
        if isinstance(o, Literal):
            if lang is not None and o.language != lang:
                continue
            if str(o) == label:
                if not rdf_types:
                    matches.append(s)
                else:
                    for t in rdf_types:
                        if (s, RDF.type, t) in g:
                            matches.append(s)
                            break
    return matches


# ----------------------------
# Subgraph building & drawing
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
    """
    Expand from one or more center nodes up to `hops` ("-1" means unlimited) and
    return a list of triples encountered. Traversal follows subject and/or object
    links according to `direction` ("out", "in", "both"). Literals are not used
    to expand the frontier but are included in the triples.

    If `only_individuals` is True, we skip traversing into (and omit triples with)
    nodes that are known Classes/Properties. Literals are always kept.
    """
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
            # Outgoing
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
            # Incoming
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
# Exports
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
    """Serialize the collected visualization triples as a Turtle subgraph."""
    ensure_dir(os.path.dirname(path))
    sg = Graph()
    # copy namespaces for nicer CURIEs
    for prefix, ns in g.namespace_manager.namespaces():
        sg.namespace_manager.bind(prefix, ns, replace=True)
    for s, p, o in triples:
        sg.add((s, p, o))
    sg.serialize(destination=path, format="turtle")

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

    args = ap.parse_args()

    ensure_dir(args.out)

    g1 = Graph()
    g1.parse(args.kg1, format="turtle")
    g2 = Graph()
    g2.parse(args.kg2, format="turtle")

    # Precompute class/property sets for filtering if needed
    classes1, properties1 = class_property_sets(g1)
    classes2, properties2 = class_property_sets(g2)

    # Inject extra prefixes if provided
    for pdef in args.prefix:
        if "=" in pdef:
            pref, iri = pdef.split("=", 1)
            g1.namespace_manager.bind(pref, URIRef(iri), replace=True)
            g2.namespace_manager.bind(pref, URIRef(iri), replace=True)

    # Global stats
    s1 = graph_stats(g1)
    s2 = graph_stats(g2)

    write_stats_txt(os.path.join(args.out, f"{args.name1}_stats.txt"), args.name1, s1, g1)
    write_stats_txt(os.path.join(args.out, f"{args.name2}_stats.txt"), args.name2, s2, g2)

    write_counter_csv(os.path.join(args.out, f"{args.name1}_types.csv"), s1["type_counts"], g1, ("type", "count"))
    write_counter_csv(os.path.join(args.out, f"{args.name2}_types.csv"), s2["type_counts"], g2, ("type", "count"))
    write_counter_csv(os.path.join(args.out, f"{args.name1}_predicates.csv"), s1["predicate_counts"], g1, ("predicate", "count"))
    write_counter_csv(os.path.join(args.out, f"{args.name2}_predicates.csv"), s2["predicate_counts"], g2, ("predicate", "count"))

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

        if len(m1) == 0 and len(m2) == 0:
            print("No matching instances found in either KG for the given label/type(s). Consider checking --label, --lang, and --type1/--type2.")
        else:
            if m1:
                c1 = m1[0]
                triples1 = build_multihop_triples(g1, c1, hops=args.hops, limit_triples=args.limit, direction=args.direction, only_individuals=args.only_individuals, classes=classes1, properties=properties1)
                G1 = triples_to_networkx(g1, triples1)
                png1 = os.path.join(args.out, f"{args.name1}_{args.label}_h{args.hops}.png").replace(" ", "_")
                draw_graph(G1, png1, title=f"{args.name1}: {args.label} (hops={args.hops})")
                stats1_path = os.path.join(args.out, f"{args.name1}_{args.label}_h{args.hops}.txt").replace(" ", "_")
                write_ego_stats(stats1_path, args.name1, c1, triples1, g1)
                ttl1 = os.path.join(args.out, f"{args.name1}_{args.label}_h{args.hops}.ttl").replace(" ", "_")
                write_subgraph_ttl(ttl1, triples1, g1)
            if m2:
                c2 = m2[0]
                triples2 = build_multihop_triples(g2, c2, hops=args.hops, limit_triples=args.limit, direction=args.direction, only_individuals=args.only_individuals, classes=classes2, properties=properties2)
                G2 = triples_to_networkx(g2, triples2)
                png2 = os.path.join(args.out, f"{args.name2}_{args.label}_h{args.hops}.png").replace(" ", "_")
                draw_graph(G2, png2, title=f"{args.name2}: {args.label} (hops={args.hops})")
                stats2_path = os.path.join(args.out, f"{args.name2}_{args.label}_h{args.hops}.txt").replace(" ", "_")
                write_ego_stats(stats2_path, args.name2, c2, triples2, g2)
                ttl2 = os.path.join(args.out, f"{args.name2}_{args.label}_h{args.hops}.ttl").replace(" ", "_")
                write_subgraph_ttl(ttl2, triples2, g2)

    # Summary comparison file
    with open(os.path.join(args.out, "comparison_summary.txt"), "w", encoding="utf-8") as f:
        f.write("=== High-level comparison ===\n")
        def line(metric):
            return f"{metric}: {args.name1}={s1[metric]} | {args.name2}={s2[metric]}\n"
        for m in [
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
            f.write(line(m))
        f.write("\nSee *_stats.txt, *_types.csv, *_predicates.csv for details.\n")


if __name__ == "__main__":
    main()
