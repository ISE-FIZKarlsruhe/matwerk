from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Assumes repo layout: <repo_root>/dags/<this_file>
REPO_ROOT = str(Path(__file__).resolve().parents[1])

RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
RUN_DIR = "{{ var.value.sharedfs }}/runs/kg_harvesters_asserted/" + RUN_ID_SAFE

# Where to publish a copy of the run artifacts (server filesystem)
PUBLISH_ROOT = "{{ var.value.sharedfs }}/output/kg_harvesters_asserted/" + RUN_ID_SAFE

# Where to locate spreadsheets runs to pick latest spreadsheets_asserted.ttl
SPREADSHEETS_RUNS_ROOT = "{{ var.value.sharedfs }}/runs/kg_spreadsheets_asserted"

with DAG(
    dag_id="kg_harvesters_asserted",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args={"retries": 1},
    tags=["kg", "harvester"],
) as dag:

    init_run_dir = BashOperator(
        task_id="init_run_dir",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'
        RUN_DIR="{RUN_DIR}"
        mkdir -p "$RUN_DIR/data/zenodo"
        mkdir -p "$RUN_DIR/data/sparql_endpoints"
        """,
    )

    harvester_zenodo = BashOperator(
        task_id="harvester_zenodo",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        export PYTHONPATH="{REPO_ROOT}:{REPO_ROOT}/dags:${{PYTHONPATH:-}}"
        cd "{REPO_ROOT}"

        RUN_DIR="{RUN_DIR}"
        mkdir -p "$RUN_DIR/data/zenodo"

        # 1) Export Zenodo
        ZENODO_TTL="$RUN_DIR/data/zenodo/zenodo.ttl"
        python -m scripts.zenodo.export_zenodo --make-snapshots --out "$ZENODO_TTL"
        test -s "$ZENODO_TTL"

        # 2) Fetch (harvest) based on most recent spreadsheets asserted TTL
        SPREADSHEETS_ROOT="{SPREADSHEETS_RUNS_ROOT}"
        LATEST_TTL="$(ls -1t "$SPREADSHEETS_ROOT"/*/data/spreadsheets_asserted.ttl 2>/dev/null | head -n 1 || true)"
        if [ -z "${{LATEST_TTL}}" ]; then
          echo "[ERROR] No $SPREADSHEETS_ROOT/*/data/spreadsheets_asserted.ttl found."
          exit 1
        fi
        test -s "${{LATEST_TTL}}"
        echo "${{LATEST_TTL}}" > "$RUN_DIR/data/zenodo/source_spreadsheets_asserted_path.txt"

        python "{REPO_ROOT}/scripts/fetch_zenodo.py" \\
          --data "${{LATEST_TTL}}" \\
          --out-csv "$RUN_DIR/data/zenodo/datasets_urls.csv" \\
          --out-dir "$RUN_DIR/data/zenodo/harvested"

        test -s "$RUN_DIR/data/zenodo/datasets_urls.csv"
        test -d "$RUN_DIR/data/zenodo/harvested"

        # Graph IRI + provenance (local)
        TS="$(date +%s%3N)"
        GRAPH_BASE="https://purls.helmholtz-metadaten.de/msekg/"
        G="${{GRAPH_BASE}}${{TS}}"
        echo "$G" > "$RUN_DIR/data/zenodo/zenodo_graph_iri.txt"

        NOW_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        DAG_ID="{{{{ dag.dag_id }}}}"
        RUN_ID="{{{{ dag_run.run_id }}}}"
        TASK_ID="{{{{ ti.task_id }}}}"
        LOG_URL="{{{{ ti.log_url }}}}"

        PROV_TTL="$RUN_DIR/data/zenodo/zenodo_provenance.ttl"
        cat > "$PROV_TTL" <<EOF
@prefix dct: <http://purl.org/dc/terms/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix mse: <https://purls.helmholtz-metadaten.de/msekg/vocab/> .

[] a prov:Activity ;
  dct:created "${{NOW_ISO}}"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
  mse:airflowDagId "${{DAG_ID}}" ;
  mse:airflowRunId "${{RUN_ID}}" ;
  mse:airflowTaskId "${{TASK_ID}}" ;
  mse:producedGraph <${{G}}> ;
  mse:usedFile "${{LATEST_TTL}}" ;
  mse:producedFile "zenodo.ttl" ;
  mse:producedFile "datasets_urls.csv" ;
  rdfs:seeAlso <${{LOG_URL}}> .
EOF
        test -s "$PROV_TTL"

        echo "Done."
        ls -lah "$RUN_DIR/data/zenodo" | sed -n '1,200p'
        """,
    )

    harvester_endpoints = BashOperator(
        task_id="harvester_endpoints",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        export PYTHONPATH="{REPO_ROOT}:{REPO_ROOT}/dags:${{PYTHONPATH:-}}"
        cd "{REPO_ROOT}"

        RUN_DIR="{RUN_DIR}"
        OUT_DIR="$RUN_DIR/data/sparql_endpoints"
        mkdir -p "$OUT_DIR"

        # Pick most recent spreadsheets asserted TTL (source KG)
        SPREADSHEETS_ROOT="{SPREADSHEETS_RUNS_ROOT}"
        LATEST_TTL="$(ls -1t "$SPREADSHEETS_ROOT"/*/data/spreadsheets_asserted.ttl 2>/dev/null | head -n 1 || true)"
        if [ -z "${{LATEST_TTL}}" ]; then
          echo "[ERROR] No $SPREADSHEETS_ROOT/*/data/spreadsheets_asserted.ttl found."
          exit 1
        fi
        test -s "${{LATEST_TTL}}"
        echo "${{LATEST_TTL}}" > "$OUT_DIR/source_spreadsheets_asserted_path.txt"

        # Run fetch_endpoints with RUN_DIR-local outputs
        export ALL_TTL="${{LATEST_TTL}}"
        export STATE_JSON="$OUT_DIR/sparql_sources.json"
        export SUMMARY_JSON="$OUT_DIR/sparql_sources_list.json"
        export STATS_TTL="$OUT_DIR/dataset_stats.ttl"
        export NAMED_GRAPHS_DIR="$OUT_DIR/named_graphs/"

        python "{REPO_ROOT}/scripts/fetch_endpoints.py"

        # sanity checks
        test -s "$STATE_JSON" || (echo "[ERROR] state json missing" && exit 1)
        test -s "$SUMMARY_JSON" || (echo "[ERROR] summary json missing" && exit 1)
        test -s "$STATS_TTL" || (echo "[ERROR] stats ttl missing" && exit 1)
        test -d "$NAMED_GRAPHS_DIR" || (echo "[ERROR] named graphs dir missing" && exit 1)

        # Graph IRI + provenance (local)
        TS="$(date +%s%3N)"
        GRAPH_BASE="https://purls.helmholtz-metadaten.de/msekg/"
        G="${{GRAPH_BASE}}${{TS}}"
        echo "$G" > "$OUT_DIR/sparql_endpoints_graph_iri.txt"

        NOW_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        DAG_ID="{{{{ dag.dag_id }}}}"
        RUN_ID="{{{{ dag_run.run_id }}}}"
        TASK_ID="{{{{ ti.task_id }}}}"
        LOG_URL="{{{{ ti.log_url }}}}"

        PROV_TTL="$OUT_DIR/sparql_endpoints_provenance.ttl"
        cat > "$PROV_TTL" <<EOF
@prefix dct: <http://purl.org/dc/terms/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix mse: <https://purls.helmholtz-metadaten.de/msekg/vocab/> .

[] a prov:Activity ;
  dct:created "${{NOW_ISO}}"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
  mse:airflowDagId "${{DAG_ID}}" ;
  mse:airflowRunId "${{RUN_ID}}" ;
  mse:airflowTaskId "${{TASK_ID}}" ;
  mse:producedGraph <${{G}}> ;
  mse:usedFile "${{LATEST_TTL}}" ;
  mse:producedFile "dataset_stats.ttl" ;
  mse:producedFile "sparql_sources.json" ;
  mse:producedFile "sparql_sources_list.json" ;
  rdfs:seeAlso <${{LOG_URL}}> .
EOF
        test -s "$PROV_TTL"

        echo "Done."
        ls -lah "$OUT_DIR" | sed -n '1,200p'
        """,
    )

    publish = BashOperator(
        task_id="publish_run_artifacts",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        RUN_DIR="{RUN_DIR}"
        DEST="{PUBLISH_ROOT}"

        mkdir -p "$DEST"
        cp -a "$RUN_DIR/." "$DEST/"

        echo "Published run artifacts to: $DEST"
        find "$DEST" -maxdepth 7 -type f | sed -n '1,200p'
        """,
    )

    init_run_dir >> [harvester_zenodo, harvester_endpoints] >> publish
