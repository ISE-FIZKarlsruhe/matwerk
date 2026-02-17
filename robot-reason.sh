#!/bin/bash
set -e

COMPONENTSDIR=data
VALIDATIONSDIR=data/validation
SRC=ontology/mwo-full.owl

mkdir -p "$VALIDATIONSDIR"

###############################################################################
# ABox materialization via SPARQL UPDATE rules (replaces Hermit KG reasoning)
###############################################################################

RULEDIR="dags/scripts/rules"
OUTDIR="$COMPONENTSDIR/reasoned"
mkdir -p "$OUTDIR"

# Helper: get triple count for a given ontology file
get_triple_count() {
  local FILE="$1"
  local COUNT_TSV="$OUTDIR/$(basename "$FILE").count.tsv"

  robot query \
    -i "$FILE" \
    --query "$RULEDIR/count-triples.rq" \
    "$COUNT_TSV"

  # Skip header, take first column
  tail -n +2 "$COUNT_TSV" | cut -f1
}

echo "ABox materialization with SPARQL UPDATE rules"
CURRENT_FILE="$COMPONENTSDIR/all_NotReasoned.ttl"
CURRENT_COUNT=$(get_triple_count "$CURRENT_FILE")
echo "not reasoned: ${CURRENT_COUNT} triples"

# Helper to apply an update rule and report added triples
apply_update() {
  local RULE_FILE="$1"   # e.g. infer-domain-types.ru
  local LABEL="$2"       # label for printing
  local NEXT_FILE="$3"   # output file

  local BEFORE_COUNT="$CURRENT_COUNT"

  robot query \
    -i "$CURRENT_FILE" \
    --update "$RULEDIR/$RULE_FILE" \
    -o "$NEXT_FILE"

  CURRENT_FILE="$NEXT_FILE"
  CURRENT_COUNT=$(get_triple_count "$CURRENT_FILE")

  local ADDED=$(( CURRENT_COUNT - BEFORE_COUNT ))
  echo "${LABEL}: added ${ADDED} triples (total ${CURRENT_COUNT})"
}

# ORDER: property-level first, then typing, then sameAs

# 1) Equivalent properties (maximize property assertions early)
apply_update "infer-equivalent-props.ru" "infer-equivalent-props" "$OUTDIR/step1.ttl"

# 2) Inverse properties
apply_update "infer-inverses.ru"        "infer-inverses"        "$OUTDIR/step2.ttl"

# 3) Property chains (p1∘p2 -> r)
apply_update "infer-property-chains.ru" "infer-property-chains" "$OUTDIR/step3.ttl"

# 4) Transitive properties
apply_update "infer-transitive.ru"      "infer-transitive"      "$OUTDIR/step4.ttl"

# 5) SubProperty propagation (on all enriched property assertions)
apply_update "infer-subprops.ru"        "infer-subprops"        "$OUTDIR/step5.ttl"

# 6) Domain → rdf:type (after properties are propagated)
apply_update "infer-domain-types.ru"    "infer-domain-types"    "$OUTDIR/step6.ttl"

# 7) Range → rdf:type
apply_update "infer-range-types.ru"     "infer-range-types"     "$OUTDIR/step7.ttl"

# 8) SubClass type propagation (after all rdf:type are in place)
apply_update "infer-subclass-types.ru"  "infer-subclass-types"  "$OUTDIR/step8.ttl"

# 9) sameAs closure (symmetric + transitive)
apply_update "infer-sameas-closure.ru"  "infer-sameas-closure"  "$OUTDIR/step9.ttl"

# 10) Propagate types over sameAs
apply_update "infer-sameas-types.ru"    "infer-sameas-types"    "$OUTDIR/step10.ttl"

# 11) Propagate properties over sameAs
apply_update "infer-sameas-props.ru"    "infer-sameas-props"    "$OUTDIR/step11.ttl"

# Final KG output
cp "$OUTDIR/step11.ttl" "$COMPONENTSDIR/all.ttl"
# Remove any default empty prefix
sed -i '/^@prefix : </d' "$COMPONENTSDIR/all.ttl"
rm -f "$OUTDIR"/step*.ttl

echo "ABox materialization done: $COMPONENTSDIR/all.ttl (total ${CURRENT_COUNT} triples)"

###############################################################################
# Explanations and inconsistency checks
###############################################################################

echo "Explanations and inconsistency checks"
robot explain \
    --input "$COMPONENTSDIR/all_NotReasoned.ttl" \
    -M inconsistency \
    --explanation "$VALIDATIONSDIR/inconsistency.md"

robot explain \
    --reasoner hermit \
    --input "$SRC" \
    -M inconsistency \
    --explanation "$VALIDATIONSDIR/inconsistency_mwo.md"

robot explain \
    --reasoner hermit \
    --input "$COMPONENTSDIR/all.ttl" \
    -M inconsistency \
    --explanation "$VALIDATIONSDIR/inconsistency_hermit.md"

###############################################################################
# SHACL validations (with safe failure handling)
###############################################################################

for i in 7 6 5 4 3 2 1; do
    echo "Running SHACL validations: shape $i"
    SHAPE_FILE="shapes/shape$i.ttl"
    OUTPUT_FILE="$VALIDATIONSDIR/shape$i.md"
    if ! python3 -m pyshacl -s "$SHAPE_FILE" "$COMPONENTSDIR/all.ttl" > "$OUTPUT_FILE"; then
        echo "SHACL validation for shape$i.ttl failed" >&2
    fi
done

###############################################################################
# SPARQL verification (safe failure)
###############################################################################

echo "Running SPARQL verification..."
if ! robot verify \
    --input "$COMPONENTSDIR/all.ttl" \
    --queries shapes/verify1.sparql \
    --output-dir "$VALIDATIONSDIR" \
    -vvv > "$VALIDATIONSDIR/verify1.md"; then
    echo "SPARQL verification failed" >&2
fi

echo "All ABox materialization, reasoning, and validation steps completed."
