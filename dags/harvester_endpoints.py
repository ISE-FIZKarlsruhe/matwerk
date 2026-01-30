from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DAG_ID = "harvester_endpoints"

RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
# Save SPARQL data separately:
RUN_DIR = "{{ var.value.sharedfs }}/runs/" + DAG_ID + "/" + RUN_ID_SAFE


# merge_validation output root (new layout)
MERGE_RUNS_ROOT = "{{ var.value.sharedfs }}/runs/merge_validation"

SCRIPTS_REPO = "https://github.com/ISE-FIZKarlsruhe/matwerk"
SCRIPTS_REF = "main"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args={"retries": 1},
    tags=["kg", "harvester", "sparql"],
) as dag:

    init_run_dir = BashOperator(
    task_id="init_run_dir",
    bash_command=f"""
set -eEuo pipefail
IFS=$'\\n\\t'
RUN_DIR="{RUN_DIR}"
mkdir -p "$RUN_DIR"
mkdir -p "$RUN_DIR/ontology"
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

test -s "$REPO_DIR/scripts/fetch_endpoints.py"

echo "Fetched scripts OK:"
find "$REPO_DIR/scripts" -maxdepth 3 -type f | sed -n '1,200p'
""",
    )

    harvester_endpoints = BashOperator(
    task_id="harvester_endpoints",
    bash_command=f"""
set -eEuo pipefail
IFS=$'\\n\\t'

RUN_DIR="{RUN_DIR}"
REPO_DIR="$RUN_DIR/repo"
OUT_DIR="$RUN_DIR"
mkdir -p "$OUT_DIR"
mkdir -p "$RUN_DIR/ontology"

export PYTHONPATH="$REPO_DIR:${{PYTHONPATH:-}}"
cd "$REPO_DIR"

ROOT="{MERGE_RUNS_ROOT}"
LATEST_TTL=""
LATEST_RUN=""

# Prefer _SUCCESS marker (if you add it later)
for d in $(ls -1dt "$ROOT"/* 2>/dev/null); do
  if [ -s "$d/spreadsheets_asserted.ttl" ]; then
    LATEST_RUN="$d"
    LATEST_TTL="$d/spreadsheets_asserted.ttl"
    break
  fi
done

# Fallback: ttl exists (you can tighten if you want)
if [ -z "$LATEST_TTL" ]; then
  for d in $(ls -1dt "$ROOT"/* 2>/dev/null); do
    if [ -s "$d/spreadsheets_asserted.ttl" ]; then
      LATEST_RUN="$d"
      LATEST_TTL="$d/spreadsheets_asserted.ttl"
      break
    fi
  done
fi

if [ -z "$LATEST_TTL" ]; then
  echo "[ERROR] No spreadsheets_asserted.ttl found under $ROOT" >&2
  ls -lah "$ROOT" | sed -n '1,200p' >&2 || true
  exit 1
fi

echo "Using merge_validation run: $LATEST_RUN"
echo "$LATEST_TTL" > "$OUT_DIR/source_spreadsheets_asserted_path.txt"
test -s "$LATEST_TTL"

# ---- Provide MWO ontology for fetch_endpoints.py ----
# Your script default expects ontology/mwo-full.owl, but you actually use mwo.owl.
# Put it into this run dir and point MWO_OWL_PATH there.
MWO_LOCAL="$RUN_DIR/ontology/mwo.owl"
curl -fsSL "https://raw.githubusercontent.com/ISE-FIZKarlsruhe/mwo/refs/tags/v3.0.0/mwo.owl" -o "$MWO_LOCAL"
test -s "$MWO_LOCAL"

export MWO_OWL_PATH="$MWO_LOCAL"

# ---- Run fetch_endpoints.py with OUT_DIR-local outputs ----
export ALL_TTL="$LATEST_TTL"
export STATE_JSON="$OUT_DIR/sparql_sources.json"
export SUMMARY_JSON="$OUT_DIR/sparql_sources_list.json"
export STATS_TTL="$OUT_DIR/dataset_stats.ttl"
export NAMED_GRAPHS_DIR="$OUT_DIR/named_graphs/"

python "$REPO_DIR/scripts/fetch_endpoints.py"

test -s "$STATE_JSON" || (echo "[ERROR] state json missing" && exit 1)
test -s "$SUMMARY_JSON" || (echo "[ERROR] summary json missing" && exit 1)
test -s "$STATS_TTL" || (echo "[ERROR] stats ttl missing" && exit 1)
test -d "$NAMED_GRAPHS_DIR" || (echo "[ERROR] named graphs dir missing" && exit 1)

echo "Done."
ls -lah "$OUT_DIR" | sed -n '1,200p'
""",
)


    init_run_dir >> fetch_scripts >> harvester_endpoints
