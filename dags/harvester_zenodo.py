from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DAG_ID = "harvester_zenodo"

RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
RUN_DIR = "{{ var.value.sharedfs }}/runs/" + DAG_ID + "/" + RUN_ID_SAFE

# merge_validation now writes here:
MERGE_RUNS_ROOT = "{{ var.value.sharedfs }}/runs/merge_validation"

SCRIPTS_REPO = "https://github.com/ISE-FIZKarlsruhe/matwerk"
SCRIPTS_REF = "main"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args={"retries": 1},
    tags=["kg", "harvester", "zenodo"],
) as dag:

    init_run_dir = BashOperator(
        task_id="init_run_dir",
        bash_command=f"""
set -eEuo pipefail
IFS=$'\\n\\t'
RUN_DIR="{RUN_DIR}"
mkdir -p "$RUN_DIR"
""",
    )

    fetch_scripts = BashOperator(
        task_id="fetch_scripts_from_github",
        bash_command=f"""
set -eEuo pipefail
IFS=$'\\n\\t'

RUN_DIR="{RUN_DIR}"
REPO_DIR="$RUN_DIR/repo"

rm -rf "$REPO_DIR"
mkdir -p "$REPO_DIR"

echo "Fetching scripts/ from {SCRIPTS_REPO} (ref: {SCRIPTS_REF}) into $REPO_DIR"

TMP_REPO="$(mktemp -d)"
cleanup() {{ rm -rf "$TMP_REPO"; }}
trap cleanup EXIT

copy_scripts() {{
  local SRC="$1"
  rm -rf "$REPO_DIR/scripts"
  mkdir -p "$REPO_DIR"

  if tar --help 2>/dev/null | grep -q -- '--touch'; then
    (cd "$SRC" && tar -cf - scripts) | (cd "$REPO_DIR" && tar -xf - --touch --no-same-owner --no-same-permissions)
  else
    cp -R "$SRC/scripts" "$REPO_DIR/"
  fi

  if command -v find >/dev/null 2>&1; then
    find "$REPO_DIR/scripts" -exec touch -c {{}} + >/dev/null 2>&1 || true
  fi
}}

if command -v git >/dev/null 2>&1; then
  echo "git found; cloning into temp dir $TMP_REPO"
  git clone --depth 1 --filter=blob:none --sparse --branch "{SCRIPTS_REF}" "{SCRIPTS_REPO}" "$TMP_REPO"
  (cd "$TMP_REPO" && git sparse-checkout set "scripts")
  copy_scripts "$TMP_REPO"
else
  echo "git not found; falling back to GitHub tarball download" >&2
  ARCH="$TMP_REPO/repo.tgz"
  curl -fsSL -L "{SCRIPTS_REPO}/archive/refs/heads/{SCRIPTS_REF}.tar.gz" -o "$ARCH"
  tar -xzf "$ARCH" -C "$TMP_REPO"
  ROOT="$(tar -tzf "$ARCH" | head -n 1 | cut -d/ -f1)"
  copy_scripts "$TMP_REPO/$ROOT"
fi

test -s "$REPO_DIR/scripts/fetch_zenodo.py"
test -d "$REPO_DIR/scripts/zenodo" || (echo "[ERROR] missing scripts/zenodo package dir" && exit 1)

echo "Fetched scripts OK:"
find "$REPO_DIR/scripts" -maxdepth 3 -type f | sed -n '1,200p'
""",
    )

    harvester_zenodo = BashOperator(
        task_id="harvester_zenodo",
        bash_command=f"""
set -eEuo pipefail
IFS=$'\\n\\t'

RUN_DIR="{RUN_DIR}"
REPO_DIR="$RUN_DIR/repo"
OUT_DIR="$RUN_DIR"
mkdir -p "$OUT_DIR"

export PYTHONPATH="$REPO_DIR:${{PYTHONPATH:-}}"
cd "$REPO_DIR"

ROOT="{MERGE_RUNS_ROOT}"
LATEST_TTL=""
LATEST_RUN=""

# Prefer _SUCCESS marker (recommended).
for d in $(ls -1dt "$ROOT"/* 2>/dev/null); do
  if [ -s "$d/spreadsheets_asserted.ttl" ]; then
    LATEST_RUN="$d"
    LATEST_TTL="$d/spreadsheets_asserted.ttl"
    break
  fi
done

# Fallback: accept runs with ttl + all shape conforms (no _SUCCESS available)
if [ -z "$LATEST_TTL" ]; then
  for d in $(ls -1dt "$ROOT"/* 2>/dev/null); do
    CAND="$d/spreadsheets_asserted.ttl"
    if [ -s "$CAND" ] \
       && [ -s "$d/validation/verify1.md" ] \
       && grep -q "Conforms: True" "$d/validation/shape1.md" 2>/dev/null \
       && grep -q "Conforms: True" "$d/validation/shape2.md" 2>/dev/null \
       && grep -q "Conforms: True" "$d/validation/shape3.md" 2>/dev/null \
       && grep -q "Conforms: True" "$d/validation/shape4.md" 2>/dev/null; then
      LATEST_RUN="$d"
      LATEST_TTL="$CAND"
      break
    fi
  done
fi

if [ -z "$LATEST_TTL" ]; then
  echo "[ERROR] No successful spreadsheets_asserted.ttl found under $ROOT" >&2
  echo "Hint: run merge_validation successfully (and ideally write _SUCCESS at the end)." >&2
  ls -lah "$ROOT" | sed -n '1,200p' >&2 || true
  exit 1
fi

echo "Using merge_validation run: $LATEST_RUN"
echo "$LATEST_TTL" > "$OUT_DIR/source_spreadsheets_asserted_path.txt"
test -s "$LATEST_TTL"

# 1) Export Zenodo
ZENODO_TTL="$OUT_DIR/zenodo.ttl"
python -m scripts.zenodo.export_zenodo --make-snapshots --out "$ZENODO_TTL"
test -s "$ZENODO_TTL"

# 2) Harvest based on asserted TTL
python "$REPO_DIR/scripts/fetch_zenodo.py" \\
  --data "$LATEST_TTL" \\
  --out-csv "$OUT_DIR/datasets_urls.csv" \\
  --out-dir "$OUT_DIR/harvested"

test -s "$OUT_DIR/datasets_urls.csv"
test -d "$OUT_DIR/harvested"


""",
    )

    init_run_dir >> fetch_scripts >> harvester_zenodo
