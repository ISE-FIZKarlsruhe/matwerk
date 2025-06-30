#!/bin/bash
set -e

COMPONENTSDIR=data
VALIDATIONSDIR=data/validation
SRC=ontology/mwo-full.owl
ONTBASE=https://nfdi.fiz-karlsruhe.de/ontology

mkdir -p "$VALIDATIONSDIR"

# Merge OWL components
robot merge -i "$SRC" --inputs "data/components/*.owl" --output "$COMPONENTSDIR/all_NotReasoned.owl"

# Fix NFDI_0001008 datatype annotation
sed -i 's|<ontology:NFDI_0001008>|<ontology:NFDI_0001008 rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">|g' "$COMPONENTSDIR/all_NotReasoned.owl"

# Explanations and inconsistency checks
robot explain --input "$COMPONENTSDIR/all_NotReasoned.owl" -M inconsistency --explanation "$VALIDATIONSDIR/inconsistency.md"

robot explain --reasoner hermit --input "$SRC" -M inconsistency --explanation "$VALIDATIONSDIR/inconsistency_mwo.md"

robot explain --reasoner hermit --input "$COMPONENTSDIR/all_NotReasoned.owl" -M inconsistency --explanation "$VALIDATIONSDIR/inconsistency_hermit.md"

# Reasoning step
robot reason \
    --reasoner hermit \
    --input "$COMPONENTSDIR/all_NotReasoned.owl" \
    --axiom-generators "SubClass ClassAssertion" \
    --output "$COMPONENTSDIR/all.ttl"

# SHACL validations
echo "Running SHACL validations..."

python3 -m pyshacl -s shapes/shape4.ttl "$COMPONENTSDIR/all.ttl" > "$VALIDATIONSDIR/shape4.md"
python3 -m pyshacl -s shapes/shape3.ttl "$COMPONENTSDIR/all.ttl" > "$VALIDATIONSDIR/shape3.md"
python3 -m pyshacl -s shapes/shape2.ttl "$COMPONENTSDIR/all.ttl" > "$VALIDATIONSDIR/shape2.md"
python3 -m pyshacl -s shapes/shape1.ttl "$COMPONENTSDIR/all.ttl" > "$VALIDATIONSDIR/shape1.md"

# SPARQL verification
robot verify \
    --input "$COMPONENTSDIR/all.ttl" \
    --queries shapes/verify1.sparql \
    --output-dir "$VALIDATIONSDIR" \
    -vvv > "$VALIDATIONSDIR/verify1.md"

echo "All merge, reasoning, and validation steps completed."
