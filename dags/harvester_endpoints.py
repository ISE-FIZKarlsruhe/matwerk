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
    load_endpoints_to_virtuoso = BashOperator(
    task_id="load_endpoints_to_virtuoso",
    bash_command=r"""
set -eEuo pipefail
IFS=$'\n\t'

RUN_DIR="{{ var.value.sharedfs }}/runs/harvester_endpoints/{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"

# This DAG writes outputs directly into RUN_DIR (per your current file)
TTL="$RUN_DIR/dataset_stats.ttl"

echo "[INFO] RUN_DIR=$RUN_DIR"
echo "[INFO] Expect TTL at: $TTL"
ls -lah "$RUN_DIR" || true

test -s "$TTL"

GRAPH="https://purls.helmholtz-metadaten.de/msekg/harvester_endpoints/{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
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

for chunk in stream_file(ttl_path, chunk_bytes):
    post_chunk(graph, chunk)

print("Virtuoso load complete")
PY
""",
)


    init_run_dir >> fetch_scripts >> harvester_endpoints >> load_endpoints_to_virtuoso

