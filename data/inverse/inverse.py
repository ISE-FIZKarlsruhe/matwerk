from rdflib import Graph, Namespace, URIRef, RDF, RDFS, OWL, Literal
from collections import defaultdict
from pathlib import Path

all_ttl_path = "data/all.ttl"
inverse_ttl_path = "data/inverse/inverse_all.ttl"

g = Graph()
g.parse(all_ttl_path, format="turtle")

# Classify properties (kind may be missing; we'll infer by usage later)
PROP_KIND = {}  # URIRef -> "object" | "data" | "ann"
for p in set(g.subjects(RDF.type, OWL.ObjectProperty)):
    PROP_KIND[p] = "object"
for p in set(g.subjects(RDF.type, OWL.DatatypeProperty)):
    PROP_KIND[p] = "data"
for p in set(g.subjects(RDF.type, OWL.AnnotationProperty)):
    PROP_KIND[p] = "ann"

# Inverse pairs (only object properties)
inverse_pairs = set()
for p, _, inv in g.triples((None, OWL.inverseOf, None)):
    if isinstance(p, URIRef) and isinstance(inv, URIRef):
        # keep only if declared (or used) as object properties
        if PROP_KIND.get(p) == "object" or PROP_KIND.get(inv) == "object":
            inverse_pairs.add((p, inv))
            inverse_pairs.add((inv, p))

# Build subPropertyOf closure
prop_supers = defaultdict(set)
for p, _, sup in g.triples((None, RDFS.subPropertyOf, None)):
    if isinstance(p, URIRef) and isinstance(sup, URIRef):
        prop_supers[p].add(sup)
    prop_supers[p] |= {p}
    prop_supers[sup] |= {sup}

changed = True
while changed:
    changed = False
    for p in list(prop_supers.keys()):
        if p not in prop_supers[p]:
            prop_supers[p].add(p); changed = True
        new_supers = set()
        for s in list(prop_supers[p]):
            new_supers |= prop_supers.get(s, {s})
        before = len(prop_supers[p])
        prop_supers[p] |= new_supers
        changed = changed or (len(prop_supers[p]) > before)

# Ban TOP/BOTTOM properties
BANNED = {
    OWL.topObjectProperty, OWL.bottomObjectProperty,
    OWL.topDataProperty,   OWL.bottomDataProperty
}

# Helper to decide kind by usage if not declared
def kind_of_property(p: URIRef, example_object=None):
    k = PROP_KIND.get(p)
    if k:
        return k
    # Infer from usage if we have an example object
    if isinstance(example_object, Literal):
        return "data"
    else:
        return "object"  # default guess (safer for inverses/links)

out = Graph()
for t in g:
    out.add(t)

# 1) MATERIALIZE INVERSES FIRST (source = snapshot of OUT)
for p, inv in inverse_pairs:
    # snapshot to avoid chaining in the same pass
    src = list(out.triples((None, p, None)))
    for s, _, o in src:
        if not isinstance(o, Literal):  # only for object property assertions
            out.add((o, inv, s))
    src = list(out.triples((None, inv, None)))
    for s, _, o in src:
        if not isinstance(o, Literal):
            out.add((o, p, s))
    # keep the inverse axioms too (harmless if already present)
    out.add((p, OWL.inverseOf, inv))
    out.add((inv, OWL.inverseOf, p))

# 2) MATERIALIZE SUPER-PROPERTIES (source = snapshot of OUT)
src_triples = list(out)  # snapshot
inserted = 0
for s, p, o in src_triples:
    if not isinstance(p, URIRef):
        continue
    for sup in prop_supers.get(p, {p}):
        # skip reflexive + OWL top/bottom
        if sup == p or sup in BANNED or not isinstance(sup, URIRef):
            continue

        # compatibility guard: block only obvious mismatches
        sup_kind = PROP_KIND.get(sup)  # "object" | "data" | "ann" | None
        if isinstance(o, Literal):
            if sup_kind in ("object", "ann"):
                continue
        else:
            if sup_kind in ("data", "ann"):
                continue

        if (s, sup, o) not in out:
            out.add((s, sup, o))
            inserted += 1

print(f"Materialized {inserted} super-property assertions.")

out.serialize(destination=inverse_ttl_path, format="turtle")
print(f"Wrote materialized graph (safe) to {inverse_ttl_path}")
