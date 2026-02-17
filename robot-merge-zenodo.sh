#!/bin/bash
set -e

COMPONENTSDIR=data
VALIDATIONSDIR=data/validation
SRC=ontology/mwo-full.owl
ONTBASE=https://nfdi.fiz-karlsruhe.de/ontology

echo "Merge KG with zenodo data (ABox included) into all_NotReasoned.owl"
robot merge \
    --include-annotations true \
    -i "$COMPONENTSDIR/all_NotReasoned.owl" \
    -i "data/zenodo/zenodo.ttl" \
    --output "$COMPONENTSDIR/all_NotReasoned.ttl"

