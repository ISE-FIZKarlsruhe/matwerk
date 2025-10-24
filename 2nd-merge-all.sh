#!/bin/bash
set -e

COMPONENTSDIR=data
VALIDATIONSDIR=data/validation
SRC=ontology/mwo-full.owl
ONTBASE=https://nfdi.fiz-karlsruhe.de/ontology

mkdir -p "$VALIDATIONSDIR"
rm -f "$COMPONENTSDIR/all_NotReasoned.owl" "$COMPONENTSDIR/all.ttl"

echo "Reasoning ontology"
robot reason \
    --reasoner hermit \
    --input "$SRC" \
    --axiom-generators "SubClass SubDataProperty ClassAssertion EquivalentObjectProperty PropertyAssertion InverseObjectProperties SubObjectProperty" \
    --output "data/components/mwo_reasoned.owl"

echo "Merge OWL components"
robot merge --include-annotations true -i "$SRC" -i "data/components/req_1.owl" -i "data/components/req_2.owl" --inputs "data/components/*.owl" --output "$COMPONENTSDIR/all_NotReasoned.owl"

echo "Fix NFDI_0001008 datatype annotation"
sed -i 's|<ontology:NFDI_0001008>|<ontology:NFDI_0001008 rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">|g' "$COMPONENTSDIR/all_NotReasoned.owl"

echo "Reasoning KG, for now no reasoning"
robot merge --include-annotations true -i "$SRC" -i "data/components/req_1.owl" -i "data/components/req_2.owl" --inputs "data/components/*.owl" -i "data/zenodo/zenodo.ttl" --output "$COMPONENTSDIR/all.ttl"

echo "Explanations and inconsistency checks"
robot explain --input "$COMPONENTSDIR/all_NotReasoned.owl" -M inconsistency --explanation "$VALIDATIONSDIR/inconsistency.md"

robot explain --reasoner hermit --input "$SRC" -M inconsistency --explanation "$VALIDATIONSDIR/inconsistency_mwo.md"

robot explain --reasoner hermit --input "$COMPONENTSDIR/all_NotReasoned.owl" -M inconsistency --explanation "$VALIDATIONSDIR/inconsistency_hermit.md"

#robot reason \
#    --reasoner hermit \
#    --input "$COMPONENTSDIR/all_NotReasoned.owl" \
#    --axiom-generators "SubClass ClassAssertion" \
#    --output "$COMPONENTSDIR/all.ttl"


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

#python3 ./scripts/kg_compare.py --kg1 ./matwerk/data/all.ttl --name1 BFO_MSE --kg2 ./matwerk/data/MSE_KG_old/mse_v1.ttl --name2 SCHEMA_MSE --label "Ebrahim Norouzi" --type schema:Person --out data/compare_kgs/ --reasoner rdfs
#python3 ./scripts/run_sparql_bench.py --kg1 ./matwerk/data/all.ttl --name1 BFO_MSE --kg2 ./matwerk/data/MSE_KG_old/mse_v1.ttl --name2 SCHEMA_MSE --runs 3 --reasoner none --out data/compare_kgs/sparql_results/

echo "All merge, reasoning, and validation steps completed."