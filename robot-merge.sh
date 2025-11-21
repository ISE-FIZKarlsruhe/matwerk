#!/bin/bash
set -e

COMPONENTSDIR=data
VALIDATIONSDIR=data/validation
SRC=ontology/mwo-full.owl
ONTBASE=https://nfdi.fiz-karlsruhe.de/ontology

mkdir -p "$VALIDATIONSDIR"
rm -f "$COMPONENTSDIR/*.owl" \
      "$COMPONENTSDIR/*.ttl" \
      "$VALIDATIONSDIR/*.md"

echo "Reasoning ontology (TBox) with Hermit"
robot reason \
    --reasoner hermit \
    --input "$SRC" \
    --axiom-generators "SubClass SubDataProperty ClassAssertion EquivalentObjectProperty PropertyAssertion InverseObjectProperties SubObjectProperty" \
    --output "data/components/mwo_reasoned.owl"

echo "Merge OWL components (TBox + components)"
robot merge \
    --include-annotations true \
    -i "$SRC" \
    -i "data/components/req_1.owl" \
    -i "data/components/req_2.owl" \
    --inputs "data/components/*.owl" \
    --output "$COMPONENTSDIR/all_NotReasoned.owl"

echo "Fix NFDI_0001008 datatype annotation in merged KG"
sed -i 's|<ontology:NFDI_0001008>|<ontology:NFDI_0001008 rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">|g' \
    "$COMPONENTSDIR/all_NotReasoned.owl"

echo "Merge KG with zenodo data (ABox included) into all_NotReasoned.owl"
robot merge \
    --include-annotations true \
    -i "$COMPONENTSDIR/all_NotReasoned.owl" \
    -i "data/zenodo/zenodo.ttl" \
    --output "$COMPONENTSDIR/all_NotReasoned.ttl"

