from __future__ import annotations

import os

from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator

"""
Airflow DAG: merge_validation

Merges all component OWLs from the latest successful process_spreadsheets run 
(spreadsheets_asserted.ttl), then runs SHACL + SPARQL validations and gates the run.

Outputs:
  {{ var.value.sharedfs }}/runs/merge_validation/<run_id>/
    - spreadsheets_asserted_NotReasoned.owl
    - spreadsheets_asserted.ttl
    - validation/shape{i}.md
    - validation/verify1.md
"""

OUT_BASENAME = "spreadsheets_asserted"
OUT_TTL = f"{OUT_BASENAME}.ttl"

RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
RUN_DIR = "{{ var.value.sharedfs }}/runs/merge_validation/" + RUN_ID_SAFE
COMPONENTS_DIR = RUN_DIR + "/components"
VALIDATION_DIR = RUN_DIR + "/validation"
SHAPES_DIR = RUN_DIR + "/_shapes"  # cache shapes per-run

PROCESS_RUNS_ROOT = "{{ var.value.sharedfs }}/runs/spreadsheet_asserted"

SHAPES_REPO = "https://github.com/ISE-FIZKarlsruhe/matwerk"
SHAPES_REF = "main"
SHAPES_PATH_IN_REPO = "shapes"
REQUIRED_ASSETS = [f"shape{i}.ttl" for i in (1, 2, 3, 4)] + ["verify1.sparql"]

HERMIT_REPORT = "inconsistency_hermit.md"


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
  --output "$OUT/{OUT_TTL}"

test -s "$OUT/{OUT_TTL}"
""",
    )

    hermit_consistency = BashOperator(
    task_id="hermit_consistency",
    bash_command=rf"""
set -euo pipefail
IFS=$'\n\t'

OUT="{RUN_DIR}"
VDIR="{VALIDATION_DIR}"
TTL="$OUT/{OUT_TTL}"
REPORT="$VDIR/{HERMIT_REPORT}"

test -s "$TTL"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# First: reasoner check (fast signal; non-zero means issues like inconsistency/unsatisfiable)
if {{{{ var.value.robotcmd }}}} reason --reasoner hermit --input "$TTL" > "$TMPDIR/reason.log" 2>&1; then
  cat > "$REPORT" <<'MD'
# HermiT consistency check

Status: CONSISTENT

## robot reason output
MD
  echo '```' >> "$REPORT"
  sed -n '1,200p' "$TMPDIR/reason.log" >> "$REPORT"
  echo '```' >> "$REPORT"
else
  cat > "$REPORT" <<'MD'
# HermiT consistency check

Status: INCONSISTENT

## robot reason output
MD
  echo '```' >> "$REPORT"
  sed -n '1,200p' "$TMPDIR/reason.log" >> "$REPORT"
  echo '```' >> "$REPORT"

  echo "" >> "$REPORT"
  echo "## Explanation (robot explain -M inconsistency)" >> "$REPORT"
  echo "" >> "$REPORT"

  EXPL="$TMPDIR/explain.md"
  # Try to generate an explanation; do not fail the task here—let the Python gate decide.
  if ! {{{{ var.value.robotcmd }}}} explain --reasoner hermit --input "$TTL" -M inconsistency --explanation "$EXPL" \
      > "$TMPDIR/explain.log" 2>&1; then
    echo "_robot explain returned non-zero; log excerpt:_ " >> "$REPORT"
    echo '```' >> "$REPORT"
    sed -n '1,200p' "$TMPDIR/explain.log" >> "$REPORT"
    echo '```' >> "$REPORT"
  fi

  if [ -s "$EXPL" ]; then
    cat "$EXPL" >> "$REPORT"
  else
    echo "_No explanation produced._" >> "$REPORT"
  fi
fi

test -s "$REPORT"
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
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        run_id = ctx["dag_run"].run_id
        run_id_safe = run_id.replace(":", "_").replace("+", "_").replace("/", "_")

        sharedfs = Variable.get("sharedfs")
        run_dir = os.path.join(sharedfs, "runs", "merge_validation", run_id_safe)
        validation_dir = os.path.join(run_dir, "validation")

        problems: list[str] = []

        # SHACL checks
        for i in (1, 2, 3, 4):
            p = os.path.join(validation_dir, f"shape{i}.md")
            if not os.path.exists(p):
                problems.append(f"Missing SHACL output: {p}")
                continue
            txt = open(p, "r", encoding="utf-8", errors="replace").read()
            if "Conforms: True" not in txt:
                problems.append(f"SHACL non-conformance or error in shape{i} (see {p})")

        # SPARQL verify checks
        vp = os.path.join(validation_dir, "verify1.md")
        if not os.path.exists(vp):
            problems.append(f"Missing SPARQL verify output: {vp}")
        else:
            vtxt = open(vp, "r", encoding="utf-8", errors="replace").read().lower()
            bad_markers = ["fail", "violation", "violations", "error", "exception"]
            if any(m in vtxt for m in bad_markers):
                problems.append(f"SPARQL verification indicates failures (see {vp})")

        # HermiT consistency gate
        hp = os.path.join(validation_dir, "inconsistency_hermit.md")
        if not os.path.exists(hp):
            problems.append(f"Missing HermiT consistency report: {hp}")
        else:
            htxt = open(hp, "r", encoding="utf-8", errors="replace").read()
            if "Status: CONSISTENT" not in htxt:
                problems.append(f"Ontology not consistent according to HermiT (see {hp})")

        if problems:
            raise AirflowFailException("Final consistency gate failed:\n- " + "\n- ".join(problems))

    load_merge_to_virtuoso = BashOperator(
    task_id="load_merge_to_virtuoso",
    bash_command=r"""
set -eEuo pipefail
IFS=$'\n\t'

RUN_DIR="{{ var.value.sharedfs }}/runs/merge_validation/{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
TTL="$RUN_DIR/spreadsheets_asserted.ttl"

echo "[INFO] RUN_DIR=$RUN_DIR"
echo "[INFO] Expect TTL at: $TTL"
ls -lah "$RUN_DIR" || true
test -s "$TTL"

GRAPH="https://purls.helmholtz-metadaten.de/msekg/merge_validation/{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
export TTL_FILE="$TTL"
export GRAPH_VAL="$GRAPH"

VCRUD="{{ var.value.get('virtuoso_crud', '') }}"
VSPARQL="{{ var.value.get('virtuoso_sparql', '') }}"
VUSER="{{ var.value.get('virtuoso_user', '') }}"
VPASS="{{ var.value.get('virtuoso_pass', '') }}"

export VIRTUOSO_CRUD="${VCRUD:-${VIRTUOSO_CRUD:-}}"
export VIRTUOSO_SPARQL="${VSPARQL:-${VIRTUOSO_SPARQL:-}}"
export VIRTUOSO_USER="${VUSER:-${VIRTUOSO_USER:-}}"
export VIRTUOSO_PASS="${VPASS:-${VIRTUOSO_PASS:-}}"

test -n "$VIRTUOSO_CRUD"
test -n "$VIRTUOSO_SPARQL"
test -n "$VIRTUOSO_USER"
test -n "$VIRTUOSO_PASS"

export VIRTUOSO_DELETE_FIRST="{{ var.value.get('virtuoso_delete_first', '0') }}"
export VIRTUOSO_CHUNK_BYTES="{{ var.value.get('virtuoso_chunk_bytes', '5242880') }}"
export VIRTUOSO_RETRY="{{ var.value.get('virtuoso_retry', '5') }}"

python3 - <<'PY'
import os, time
import requests
from requests.auth import HTTPDigestAuth

ttl_path = os.environ["TTL_FILE"]
graph = os.environ["GRAPH_VAL"]

endpoint = os.environ["VIRTUOSO_SPARQL"].rstrip("/")
endpoint_crud = os.environ["VIRTUOSO_CRUD"].rstrip("/")
username = os.environ["VIRTUOSO_USER"]
password = os.environ["VIRTUOSO_PASS"]

retry = int(os.environ.get("VIRTUOSO_RETRY", "5"))
chunk_bytes = int(os.environ.get("VIRTUOSO_CHUNK_BYTES", "5242880"))
delete_first = os.environ.get("VIRTUOSO_DELETE_FIRST", "0") == "1"

def delete_graph(g: str):
    api_url = (
        endpoint
        + "?default-graph-uri=&query=drop+silent+graph+%3CGGGGG%3E+&format=text%2Fhtml&timeout=0&signal_void=on"
    ).replace("GGGGG", g)
    last = None
    for _ in range(retry):
        try:
            last = requests.get(api_url, auth=HTTPDigestAuth(username, password), timeout=60)
            if last.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError(f"deleteGraph failed: {last.status_code if last else 'no response'} {last.text if last else ''}")

def post_chunk(g: str, data: bytes):
    api_url = endpoint_crud + "?graph=" + g
    headers = {"Content-Type": "text/turtle"}
    last_exc = None
    for attempt in range(1, retry + 1):
        try:
            r = requests.post(
                api_url,
                data=data,
                headers=headers,
                auth=HTTPDigestAuth(username, password),
                timeout=(10, 300),
                verify=os.environ.get("VIRTUOSO_VERIFY_TLS", "1") == "1",
            )
            if r.status_code in (200, 201, 204):
                return
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:500]}")
        except Exception as e:
            last_exc = e
            print(f"[Virtuoso] POST attempt {attempt}/{retry} failed: {type(e).__name__}: {e}", flush=True)
            time.sleep(2)
    raise RuntimeError(f"storeDataToGraph failed: {type(last_exc).__name__}: {last_exc}")

def stream_file(path: str, chunk: int):
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            yield b

print(f"Virtuoso load: {ttl_path} -> <{graph}>")

if delete_first:
    delete_graph(graph)

with open(ttl_path, "rb") as f:
    data = f.read()
post_chunk(graph, data)


print("Virtuoso load complete")
PY
""",
)

    inputs_ok = assert_inputs()
    inputs_ok >> init_run_dir >> fetch_shapes_from_github >> stage_latest_components >> merge_and_convert

    merge_and_convert >> [verify_sparql, shacl_validate, hermit_consistency]

    gate = final_consistency_check()
    [verify_sparql, shacl_validate, hermit_consistency] >> gate
    gate >> load_merge_to_virtuoso




merge_validation()
