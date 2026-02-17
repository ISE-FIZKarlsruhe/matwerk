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
    --output "$COMPONENTSDIR/all_NotReasoned.ttl"
