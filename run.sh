#!/bin/bash

# Set paths
COMPONENTSDIR=data
VALIDATIONSDIR=data/validation
SRC=ontology/mwo-full.owl
ROBOT_IMAGE=obolibrary/robot
ONTBASE=https://nfdi.fiz-karlsruhe.de/ontology
KONCLUDE_IMAGE=konclude/konclude

# Merge ontology components
podman run --rm -v "$(pwd)":/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" \
    $ROBOT_IMAGE robot merge -i $SRC --inputs "data/components/*.owl" --output $COMPONENTSDIR/all_NotReasoned.owl

# Fix datatype issue using sed
sed -i 's|<ontology:NFDI_0001008>|<ontology:NFDI_0001008 rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">|g' $COMPONENTSDIR/all_NotReasoned.owl

# Run various robot explanation commands
podman run --rm -v "$(pwd)":/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" \
    $ROBOT_IMAGE robot explain --input $COMPONENTSDIR/all_NotReasoned.owl -M inconsistency --explanation $VALIDATIONSDIR/inconsistency.md

podman run --rm -v "$(pwd)":/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" \
    $ROBOT_IMAGE robot explain --reasoner hermit --input $SRC -M inconsistency --explanation $VALIDATIONSDIR/inconsistency_mwo.md

podman run --rm -v "$(pwd)":/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" \
    $ROBOT_IMAGE robot explain --reasoner hermit --input $COMPONENTSDIR/all_NotReasoned.owl -M inconsistency --explanation $VALIDATIONSDIR/inconsistency_hermit.md

# Reason with hermit
podman run --rm -v "$(pwd)":/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" \
    $ROBOT_IMAGE robot reason --reasoner hermit --input $COMPONENTSDIR/all_NotReasoned.owl --axiom-generators "SubClass EquivalentClass DisjointClasses ClassAssertion PropertyAssertion SubObjectProperty EquivalentObjectProperty InverseObjectProperties ObjectPropertyDomain ObjectPropertyRange ObjectPropertyCharacteristic SubDataProperty EquivalentDataProperties DataPropertyCharacteristic" --include-indirect true reduce --output $COMPONENTSDIR/all.ttl

# Run Konclude
podman run --rm -v "$(pwd)":/data $KONCLUDE_IMAGE realize \
    -i /data/$COMPONENTSDIR/all_NotReasoned.owl -o /data/$COMPONENTSDIR/konclude/classification-result.owl

# SHACL validation
python3 -m pyshacl -s shapes/shape4.ttl $COMPONENTSDIR/all.ttl > $VALIDATIONSDIR/shape4.md
echo "no roles without bearers"
python3 -m pyshacl -s shapes/shape3.ttl $COMPONENTSDIR/all.ttl > $VALIDATIONSDIR/shape3.md
echo "no punning"
python3 -m pyshacl -s shapes/shape2.ttl $COMPONENTSDIR/all.ttl > $VALIDATIONSDIR/shape2.md
echo "no orphaned textual entities"
python3 -m pyshacl -s shapes/shape1.ttl $COMPONENTSDIR/all.ttl > $VALIDATIONSDIR/shape1.md

# SPARQL-based validation
podman run --rm -v "$(pwd)":/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" \
    $ROBOT_IMAGE robot verify --input $COMPONENTSDIR/all.ttl --queries shapes/verify1.sparql --output-dir data/validation/ -vvv > $VALIDATIONSDIR/verify1.md
