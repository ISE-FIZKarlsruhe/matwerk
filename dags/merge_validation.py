from __future__ import annotations

import os

from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator

"""
Airflow DAG: merge_validation

Merges all component OWLs from the latest successful process_spreadsheets run,
converts to spreadsheets_asserted.ttl, then runs SHACL + SPARQL validations and gates the run.

Outputs:
  {{ var.value.sharedfs }}/runs/merge_validation/<run_id>/
    - spreadsheets_asserted_NotReasoned.owl
    - spreadsheets_asserted.ttl
    - validation/shape{i}.md
    - validation/verify1.md
"""

OUT_BASENAME = "spreadsheets_asserted"
OUT_OWL = f"{OUT_BASENAME}_NotReasoned.owl"
OUT_TTL = f"{OUT_BASENAME}.ttl"

RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
RUN_DIR = "{{ var.value.sharedfs }}/runs/merge_validation/" + RUN_ID_SAFE
COMPONENTS_DIR = RUN_DIR + "/components"
VALIDATION_DIR = RUN_DIR + "/validation"
SHAPES_DIR = RUN_DIR + "/_shapes"  # cache shapes per-run

# NOTE: you said process_spreadsheets writes flat here:
PROCESS_RUNS_ROOT = "{{ var.value.sharedfs }}/runs/spreadsheet_asserted"

SHAPES_REPO = "https://github.com/ISE-FIZKarlsruhe/matwerk"
SHAPES_REF = "main"
SHAPES_PATH_IN_REPO = "shapes"
REQUIRED_ASSETS = [f"shape{i}.ttl" for i in (1, 2, 3, 4)] + ["verify1.sparql"]


@dag(
    schedule=None,
    catchup=False,
    tags=["ontology", "merge", "validation", "spreadsheets"],
)
def merge_validation():

    @task()
    def assert_inputs():
        sharedfs = Variable.get("sharedfs")
        if not os.path.isdir(sharedfs):
            raise AirflowFailException(f"sharedfs does not exist or is not a directory: {sharedfs}")
        return {"sharedfs": sharedfs}

    init_run_dir = BashOperator(
        task_id="init_run_dir",
        bash_command=f"""
set -euo pipefail
IFS=$'\\n\\t'
mkdir -p "{COMPONENTS_DIR}"
mkdir -p "{VALIDATION_DIR}"
mkdir -p "{SHAPES_DIR}"
""",
    )

    fetch_shapes_from_github = BashOperator(
        task_id="fetch_shapes_from_github",
        bash_command=rf"""
set -euo pipefail
IFS=$'\n\t'

DEST="{SHAPES_DIR}"
rm -f "$DEST"/* || true

echo "Fetching shapes from {SHAPES_REPO} ({SHAPES_REF}:{SHAPES_PATH_IN_REPO}) into $DEST"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

safe_put() {{
  local SRC="$1"
  local DESTFILE="$2"
  test -s "$SRC" || (echo "Source missing/empty: $SRC" >&2 && exit 2)

  local TMP_OUT="$TMPDIR/out.tmp"
  rm -f "$TMP_OUT" || true
  cat "$SRC" > "$TMP_OUT"

  if mv -f "$TMP_OUT" "$DESTFILE" 2>/dev/null; then
    :
  else
    rm -f "$DESTFILE" || true
    cat "$SRC" > "$DESTFILE"
    rm -f "$TMP_OUT" || true
  fi
  test -s "$DESTFILE" || (echo "Failed to write: $DESTFILE" >&2 && exit 2)
}}

SRC_DIR="$TMPDIR/src"
mkdir -p "$SRC_DIR"

if command -v git >/dev/null 2>&1; then
  git clone --depth 1 --filter=blob:none --sparse --branch "{SHAPES_REF}" "{SHAPES_REPO}" "$TMPDIR/repo"
  (cd "$TMPDIR/repo" && git sparse-checkout set "{SHAPES_PATH_IN_REPO}")

  for f in {" ".join(REQUIRED_ASSETS)}; do
    test -s "$TMPDIR/repo/{SHAPES_PATH_IN_REPO}/$f" || (echo "Missing in repo: $f" >&2 && exit 2)
    cp "$TMPDIR/repo/{SHAPES_PATH_IN_REPO}/$f" "$SRC_DIR/$f"
  done
else
  BASE="https://raw.githubusercontent.com/ISE-FIZKarlsruhe/matwerk/{SHAPES_REF}/{SHAPES_PATH_IN_REPO}"
  for f in {" ".join(REQUIRED_ASSETS)}; do
    curl -fsSL "$BASE/$f" -o "$SRC_DIR/$f"
    test -s "$SRC_DIR/$f" || (echo "Download empty: $f" >&2 && exit 2)
  done
fi

for f in {" ".join(REQUIRED_ASSETS)}; do
  safe_put "$SRC_DIR/$f" "$DEST/$f"
done

echo "Shapes fetched OK:"
ls -la "$DEST"
""",
    )

    stage_latest_components = BashOperator(
        task_id="stage_latest_components",
        bash_command=f"""
set -eEuo pipefail
IFS=$'\\n\\t'

DEST="{COMPONENTS_DIR}"
rm -f "$DEST"/*.owl || true

ROOT="{PROCESS_RUNS_ROOT}"

# Choose newest run that looks like process_spreadsheets output:
LATEST_DIR=""
for d in $(ls -1dt "$ROOT"/* 2>/dev/null); do
  if [ -s "$d/req_2.owl" ] && [ "$(ls -1 "$d"/*.owl 2>/dev/null | wc -l | tr -d ' ')" -ge 25 ]; then
    LATEST_DIR="$d"
    break
  fi
done

if [ -z "$LATEST_DIR" ]; then
  echo "[ERROR] No suitable process_spreadsheets run found under: $ROOT" >&2
  exit 1
fi

echo "$LATEST_DIR" > "{RUN_DIR}/source_process_spreadsheets_run_dir.txt"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

safe_put() {{
  local SRC="$1"
  local DESTFILE="$2"
  test -s "$SRC" || (echo "Source missing/empty: $SRC" >&2 && exit 2)

  local TMP_OUT="$TMPDIR/out.tmp"
  rm -f "$TMP_OUT" || true
  cat "$SRC" > "$TMP_OUT"

  if mv -f "$TMP_OUT" "$DESTFILE" 2>/dev/null; then
    :
  else
    rm -f "$DESTFILE" || true
    cat "$SRC" > "$DESTFILE"
    rm -f "$TMP_OUT" || true
  fi
  test -s "$DESTFILE" || (echo "Failed to write: $DESTFILE" >&2 && exit 2)
}}

for f in "$LATEST_DIR"/*.owl; do
  bn="$(basename "$f")"
  safe_put "$f" "$DEST/$bn"
done

echo "Staged components from: $LATEST_DIR"
ls -lah "$DEST" | sed -n '1,200p'
""",
    )

    merge_and_convert = BashOperator(
        task_id="merge_and_convert",
        bash_command=rf"""
set -eEuo pipefail
IFS=$'\n\t'

COMP="{COMPONENTS_DIR}"
OUT="{RUN_DIR}"

rm -f "$OUT"/{OUT_BASENAME}*.owl "$OUT"/{OUT_BASENAME}*.ttl || true
rm -f "{VALIDATION_DIR}"/*.md || true

# Sanity check: ensure there are OWL inputs
ls -1 "$COMP"/*.owl >/dev/null 2>&1 || {{
  echo "[ERROR] No OWL files in $COMP" >&2
  ls -lah "$COMP" || true
  exit 1
}}

echo "Merging OWL components from $COMP"
{{{{ var.value.robotcmd }}}} merge --include-annotations true \
  --inputs "$COMP/*.owl" \
  --output "$OUT/{OUT_OWL}"

echo "Convert merged OWL to Turtle"
{{{{ var.value.robotcmd }}}} convert \
  --input "$OUT/{OUT_OWL}" \
  --format ttl \
  --output "$OUT/{OUT_TTL}"

test -s "$OUT/{OUT_TTL}"
""",
    )

    verify_sparql = BashOperator(
        task_id="verify_sparql",
        bash_command=rf"""
set -euo pipefail
IFS=$'\n\t'

SHAPES="{SHAPES_DIR}"
OUT="{RUN_DIR}"
VDIR="{VALIDATION_DIR}"

echo "Running SPARQL verification..."
if ! {{{{ var.value.robotcmd }}}} verify \
  --input "$OUT/{OUT_TTL}" \
  --queries "$SHAPES/verify1.sparql" \
  --output-dir "$VDIR" \
  -vvv > "$VDIR/verify1.md"; then
  echo "SPARQL verification failed" >&2
fi

test -s "$VDIR/verify1.md"
""",
    )

    shacl_validate = BashOperator(
        task_id="shacl_validate",
        bash_command=rf"""
set -euo pipefail
IFS=$'\n\t'

SHAPES="{SHAPES_DIR}"
OUT="{RUN_DIR}"
VDIR="{VALIDATION_DIR}"

for i in 4 3 2 1; do
  echo "Running SHACL validations: shape $i"
  SHAPE_FILE="$SHAPES/shape${{i}}.ttl"
  OUTPUT_FILE="$VDIR/shape${{i}}.md"

  if ! python3 -m pyshacl -s "$SHAPE_FILE" "$OUT/{OUT_TTL}" > "$OUTPUT_FILE"; then
    echo "SHACL validation for shape${{i}}.ttl failed" >&2
  fi
  test -s "$OUTPUT_FILE"
done
""",
    )

    @task()
    def final_consistency_check():
        problems: list[str] = []

        for i in (1, 2, 3, 4):
            p = os.path.join(VALIDATION_DIR, f"shape{i}.md")
            if not os.path.exists(p):
                problems.append(f"Missing SHACL output: {p}")
                continue
            txt = open(p, "r", encoding="utf-8", errors="replace").read()
            if "Conforms: True" not in txt:
                problems.append(f"SHACL non-conformance or error in shape{i} (see {p})")

        vp = os.path.join(VALIDATION_DIR, "verify1.md")
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
    inputs_ok >> init_run_dir >> fetch_shapes_from_github >> stage_latest_components >> merge_and_convert
    merge_and_convert >> [verify_sparql, shacl_validate]
    [verify_sparql, shacl_validate] >> final_consistency_check()


merge_validation()
