from __future__ import annotations

import os

from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator


"""
Airflow DAG: merge_validation

Merges all component OWLs present in sharedfs into spreadsheets_asserted_NotReasoned.owl,
converts to spreadsheets_asserted.ttl, then runs SHACL + SPARQL validations and gates the run.

Airflow Variables:
- sharedfs   : path to shared filesystem where *.owl are located and outputs are written
- robotcmd   : command/path to invoke ROBOT CLI
- shapesdir  : directory containing shape1.ttl..shape4.ttl and verify1.sparql

Outputs (in sharedfs):
- spreadsheets_asserted_NotReasoned.owl
- spreadsheets_asserted.ttl
- validation/shape{i}.md
- validation/verify1.md
"""

OUT_BASENAME = "spreadsheets_asserted"
OUT_OWL = f"{OUT_BASENAME}_NotReasoned.owl"
OUT_TTL = f"{OUT_BASENAME}.ttl"


@dag(
    schedule=None,
    catchup=False,
    tags=["ontology", "merge", "validation"],
)
def merge_validation():

    @task()
    def assert_inputs():
        sharedfs = Variable.get("sharedfs")
        shapesdir = Variable.get("shapesdir")

        if not os.path.isdir(sharedfs):
            raise AirflowFailException(f"sharedfs does not exist or is not a directory: {sharedfs}")
        if not os.path.isdir(shapesdir):
            raise AirflowFailException(f"shapesdir does not exist or is not a directory: {shapesdir}")

        owl_files = [f for f in os.listdir(sharedfs) if f.endswith(".owl")]
        if not owl_files:
            raise AirflowFailException(f"No .owl files found in sharedfs: {sharedfs}")

        required_assets = [f"shape{i}.ttl" for i in (1, 2, 3, 4)] + ["verify1.sparql"]
        missing = [p for p in required_assets if not os.path.exists(os.path.join(shapesdir, p))]
        if missing:
            raise AirflowFailException(f"Missing validation assets in shapesdir={shapesdir}: {missing}")

        return {"sharedfs": sharedfs, "shapesdir": shapesdir, "owl_count": len(owl_files)}

    merge_and_upload = BashOperator(
        task_id="merge_and_upload",
        bash_command=rf"""
set -euo pipefail

DATA="{{{{ var.value.sharedfs }}}}"
ROBOT="{{{{ var.value.robotcmd }}}}"
VALIDATIONSDIR="$DATA/validation"

mkdir -p "$VALIDATIONSDIR"

# Clean previous run artifacts
rm -f "$DATA"/{OUT_BASENAME}*.owl "$DATA"/{OUT_BASENAME}*.ttl "$VALIDATIONSDIR"/*.md || true

echo "Merge OWL components found in $DATA"
$ROBOT merge --include-annotations true \
  --inputs "$DATA"/*.owl \
  --output "$DATA/{OUT_OWL}"

echo "Convert merged OWL to Turtle"
$ROBOT convert \
  --input "$DATA/{OUT_OWL}" \
  --format ttl \
  --output "$DATA/{OUT_TTL}"

echo "Upload step: not configured (placeholder)"
""",
    )

    verify_sparql = BashOperator(
        task_id="verify_sparql",
        bash_command=rf"""
set -euo pipefail

DATA="{{{{ var.value.sharedfs }}}}"
ROBOT="{{{{ var.value.robotcmd }}}}"
VALIDATIONSDIR="$DATA/validation"
SHAPES_DIR="{{{{ var.value.shapesdir }}}}"

mkdir -p "$VALIDATIONSDIR"

echo "Running SPARQL verification..."
if ! $ROBOT verify \
  --input "$DATA/{OUT_TTL}" \
  --queries "$SHAPES_DIR/verify1.sparql" \
  --output-dir "$VALIDATIONSDIR" \
  -vvv > "$VALIDATIONSDIR/verify1.md"; then
  echo "SPARQL verification failed" >&2
fi
""",
    )

    shacl_validate = BashOperator(
        task_id="shacl_validate",
        bash_command=rf"""
set -euo pipefail

DATA="{{{{ var.value.sharedfs }}}}"
VALIDATIONSDIR="$DATA/validation"
SHAPES_DIR="{{{{ var.value.shapesdir }}}}"

mkdir -p "$VALIDATIONSDIR"

for i in 4 3 2 1; do
  echo "Running SHACL validations: shape $i"
  SHAPE_FILE="$SHAPES_DIR/shape${{i}}.ttl"
  OUTPUT_FILE="$VALIDATIONSDIR/shape${{i}}.md"

  if ! python3 -m pyshacl -s "$SHAPE_FILE" "$DATA/{OUT_TTL}" > "$OUTPUT_FILE"; then
    echo "SHACL validation for shape${{i}}.ttl failed" >&2
  fi
done
""",
    )

    @task()
    def final_consistency_check():
        sharedfs = Variable.get("sharedfs")
        vdir = os.path.join(sharedfs, "validation")

        problems: list[str] = []

        for i in (1, 2, 3, 4):
            p = os.path.join(vdir, f"shape{i}.md")
            if not os.path.exists(p):
                problems.append(f"Missing SHACL output: {p}")
                continue
            txt = open(p, "r", encoding="utf-8", errors="replace").read()
            if "Conforms: True" not in txt:
                problems.append(f"SHACL non-conformance or error in shape{i} (see {p})")

        vp = os.path.join(vdir, "verify1.md")
        if not os.path.exists(vp):
            problems.append(f"Missing SPARQL verify output: {vp}")
        else:
            vtxt = open(vp, "r", encoding="utf-8", errors="replace").read().lower()
            bad_markers = ["fail", "violation", "violations", "error", "exception"]
            if any(m in vtxt for m in bad_markers):
                problems.append(f"SPARQL verification indicates failures (see {vp})")

        if problems:
            raise AirflowFailException("Final consistency gate failed:\n- " + "\n- ".join(problems))

    inputs_ok = assert_inputs()
    inputs_ok >> merge_and_upload
    merge_and_upload >> [verify_sparql, shacl_validate]
    [verify_sparql, shacl_validate] >> final_consistency_check()


merge_validation()
