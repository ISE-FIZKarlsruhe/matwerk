#!/bin/bash
set -e

COMPONENTSDIR=data/components
REASONER=data/components/reasoner
SRC=ontology/mwo-full.owl
ONTBASE=https://nfdi.fiz-karlsruhe.de/ontology

mkdir -p "$COMPONENTSDIR" "$REASONER"

echo "📥 Downloading TSV files from Google Sheets..."

declare -A files=(
    [req_1]=394894036
    [req_2]=0
    [agent]=2077140060
    [role]=1425127117
    [process]=1169992315
    [city]=1469482382
    [people]=1666156492
    [organization]=447157523
    [dataset]=1079878268
    [publication]=1747331228
    [software]=1275685399
    [dataportal]=923160190
    [instrument]=2015927839
    [largescalefacility]=370181939
    [metadata]=278046522
    [matwerkta]=1489640604
    [matwerkiuc]=281962521
    [matwerkpp]=606786541
    [temporal]=1265818056
    [event]=638946284
    [collaboration]=266847052
    [service]=130394813
    [sparql_endpoints]=1732373290
    [FDOs]=152649677
)

for name in "${!files[@]}"; do
    gid="${files[$name]}"
    curl -s -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=${gid}&single=true&output=tsv" \
        -o "$COMPONENTSDIR/$name.tsv"
    echo "✔️ Downloaded: $name.tsv"
done

echo "⚙️ Running ROBOT merge + explain for each component..."

function run_robot_merge() {
    local INPUTS=$1
    local TEMPLATE=$2
    local OUTPUT=$3
    echo "🔧 Merging to $OUTPUT"
    if ! robot merge --include-annotations true $INPUTS template --template "$TEMPLATE" \
        --prefix "nfdicore: $ONTBASE/" --output "$OUTPUT"; then
        echo "❗ Merge failed for $OUTPUT, retrying with -vvv"
        robot -vvv merge --include-annotations true $INPUTS template --template "$TEMPLATE" \
            --prefix "nfdicore: $ONTBASE/" --output "$OUTPUT"
    fi
}

function run_robot_explain() {
    local INPUT=$1
    local OUTPUT=$2
    echo "🔍 Explaining inconsistencies for $INPUT"
    if ! robot explain --reasoner hermit --input "$INPUT" \
        -M inconsistency --explanation "$OUTPUT"; then
        echo "❗ Explain failed for $INPUT, retrying with -vvv"
        robot -vvv explain --reasoner hermit --input "$INPUT" \
            -M inconsistency --explanation "$OUTPUT"
    fi
}

# Ordered logic with dependency chaining
run_robot_merge "-i $SRC" "$COMPONENTSDIR/req_1.tsv" "$COMPONENTSDIR/req_1.owl"
run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl" "$COMPONENTSDIR/req_2.tsv" "$COMPONENTSDIR/req_2.owl"
sed -i 's|<ontology:NFDI_0001008>|<ontology:NFDI_0001008 rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">|g' "$COMPONENTSDIR/req_1.owl"
sed -i 's|<ontology:NFDI_0001008>|<ontology:NFDI_0001008 rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">|g' "$COMPONENTSDIR/req_2.owl"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_2.owl" "$COMPONENTSDIR/agent.tsv" "$COMPONENTSDIR/agent.owl"
run_robot_explain "$COMPONENTSDIR/agent.owl" "$REASONER/agent_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/agent.owl" "$COMPONENTSDIR/role.tsv" "$COMPONENTSDIR/role.owl"
run_robot_explain "$COMPONENTSDIR/role.owl" "$REASONER/role_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/role.owl" "$COMPONENTSDIR/process.tsv" "$COMPONENTSDIR/process.owl"
run_robot_explain "$COMPONENTSDIR/process.owl" "$REASONER/process_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl" "$COMPONENTSDIR/city.tsv" "$COMPONENTSDIR/city.owl"
run_robot_explain "$COMPONENTSDIR/city.owl" "$REASONER/city_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/organization.owl" "$COMPONENTSDIR/people.tsv" "$COMPONENTSDIR/people.owl"
run_robot_explain "$COMPONENTSDIR/people.owl" "$REASONER/people_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/city.owl" "$COMPONENTSDIR/organization.tsv" "$COMPONENTSDIR/organization.owl"
run_robot_explain "$COMPONENTSDIR/organization.owl" "$REASONER/organization_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/role.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/dataset.tsv" "$COMPONENTSDIR/dataset.owl"
run_robot_explain "$COMPONENTSDIR/dataset.owl" "$REASONER/dataset_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/publication.tsv" "$COMPONENTSDIR/publication.owl"
run_robot_explain "$COMPONENTSDIR/publication.owl" "$REASONER/publication_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/publication.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/software.tsv" "$COMPONENTSDIR/software.owl"
run_robot_explain "$COMPONENTSDIR/software.owl" "$REASONER/software_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/publication.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/dataportal.tsv" "$COMPONENTSDIR/dataportal.owl"
run_robot_explain "$COMPONENTSDIR/dataportal.owl" "$REASONER/dataportal_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/publication.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/role.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/instrument.tsv" "$COMPONENTSDIR/instrument.owl"
run_robot_explain "$COMPONENTSDIR/instrument.owl" "$REASONER/instrument_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/publication.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/role.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/largescalefacility.tsv" "$COMPONENTSDIR/largescalefacility.owl"
run_robot_explain "$COMPONENTSDIR/largescalefacility.owl" "$REASONER/largescalefacility_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/publication.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/metadata.tsv" "$COMPONENTSDIR/metadata.owl"
run_robot_explain "$COMPONENTSDIR/metadata.owl" "$REASONER/metadata_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/role.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/matwerkta.tsv" "$COMPONENTSDIR/matwerkta.owl"
run_robot_explain "$COMPONENTSDIR/matwerkta.owl" "$REASONER/matwerkta_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/matwerkta.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/role.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/matwerkiuc.tsv" "$COMPONENTSDIR/matwerkiuc.owl"
run_robot_explain "$COMPONENTSDIR/matwerkiuc.owl" "$REASONER/matwerkiuc_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/matwerkta.owl -i $COMPONENTSDIR/matwerkiuc.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/role.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/matwerkpp.tsv" "$COMPONENTSDIR/matwerkpp.owl"
run_robot_explain "$COMPONENTSDIR/matwerkpp.owl" "$REASONER/matwerkpp_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/publication.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/temporal.tsv" "$COMPONENTSDIR/temporal.owl"
run_robot_explain "$COMPONENTSDIR/temporal.owl" "$REASONER/temporal_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/temporal.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/role.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/event.tsv" "$COMPONENTSDIR/event.owl"
run_robot_explain "$COMPONENTSDIR/event.owl" "$REASONER/event_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/temporal.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/collaboration.tsv" "$COMPONENTSDIR/collaboration.owl"
run_robot_explain "$COMPONENTSDIR/collaboration.owl" "$REASONER/collaboration_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/temporal.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/role.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/service.tsv" "$COMPONENTSDIR/service.owl"
run_robot_explain "$COMPONENTSDIR/service.owl" "$REASONER/service_inconsistency.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/temporal.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/role.owl -i $COMPONENTSDIR/process.owl" "$COMPONENTSDIR/sparql_endpoints.tsv" "$COMPONENTSDIR/sparql_endpoints.owl"
run_robot_explain "$COMPONENTSDIR/sparql_endpoints.owl" "$REASONER/sparql_endpoints.md"

run_robot_merge "-i $SRC -i $COMPONENTSDIR/req_1.owl -i $COMPONENTSDIR/req_2.owl -i $COMPONENTSDIR/organization.owl -i $COMPONENTSDIR/temporal.owl -i $COMPONENTSDIR/agent.owl -i $COMPONENTSDIR/role.owl -i $COMPONENTSDIR/process.owl -i $COMPONENTSDIR/dataset.owl" "$COMPONENTSDIR/FDOs.tsv" "$COMPONENTSDIR/FDOs.owl"
run_robot_explain "$COMPONENTSDIR/FDOs.owl" "$REASONER/FDOs.md"

sed -i 's|<ontology:NFDI_0001008>|<ontology:NFDI_0001008 rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">|g' data/components/*.owl
echo "✅ All components generated and explained."
