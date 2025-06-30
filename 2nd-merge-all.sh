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

# SHACL validations (with safe failure handling)
for i in 4 3 2 1; do
    echo "Running SHACL validations: shape $i"
    SHAPE_FILE="shapes/shape$i.ttl"
    OUTPUT_FILE="$VALIDATIONSDIR/shape$i.md"
    if ! python3 -m pyshacl -s "$SHAPE_FILE" "$COMPONENTSDIR/all.ttl" > "$OUTPUT_FILE"; then
        echo "SHACL validation for shape$i.ttl failed" >&2
    fi
done

# SPARQL verification (safe failure)
echo "Running SPARQL verification..."
if ! robot verify \
    --input "$COMPONENTSDIR/all.ttl" \
    --queries shapes/verify1.sparql \
    --output-dir "$VALIDATIONSDIR" \
    -vvv > "$VALIDATIONSDIR/verify1.md"; then
    echo "SPARQL verification failed" >&2
fi

echo "All merge, reasoning, and validation steps completed."