from __future__ import annotations

from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# -----------------------------
# Image
# -----------------------------
KG_IMAGE = "{{ dag_run.conf.get('kg_image', 'mse-kg-runner:local') }}"

# -----------------------------
# Run paths
# -----------------------------
RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
RUN_DIR = f"/workspace/runs/{RUN_ID_SAFE}"

# -----------------------------
# Mounts
# -----------------------------
WORKSPACE_MOUNT = Mount(source="kg_workspace", target="/workspace", type="volume")

CODE_MOUNT = Mount(
    source="/mnt/c/Users/eno/Documents/NFDI-MatWerk/gitlab/matwerk",
    target="/app",
    type="bind",
)

HOST_OUT_DIR = "/mnt/c/Users/eno/Documents/NFDI-MatWerk/gitlab/matwerk/output"
HOST_OUT_MOUNT = Mount(source=HOST_OUT_DIR, target="/host_out", type="bind")

MOUNTS = [WORKSPACE_MOUNT, CODE_MOUNT]
PUBLISH_MOUNTS = [WORKSPACE_MOUNT, HOST_OUT_MOUNT]

VIRTUOSO_IMAGE = "openlink/virtuoso-opensource-7:latest"
VIRTUOSO_DB_MOUNT = Mount(source="virtuoso_db", target="/database", type="volume")
VIRTUOSO_LOADER_MOUNTS = [WORKSPACE_MOUNT, VIRTUOSO_DB_MOUNT]

# -----------------------------
# Env passthrough
# -----------------------------
ENV = {
    "PYTHONPATH": "/app:/app/dags",
    "TRIPLESTORE_SPARQL_ENDPOINT": os.environ.get("TRIPLESTORE_SPARQL_ENDPOINT", "http://virtuoso:8890/sparql"),
    "TRIPLESTORE_GRAPH_CRUD": os.environ.get("TRIPLESTORE_GRAPH_CRUD", "http://virtuoso:8890/sparql-graph-crud"),
    "TRIPLESTORE_USER": os.environ.get("TRIPLESTORE_USER", "dba"),
    "TRIPLESTORE_PASSWORD": os.environ.get("TRIPLESTORE_PASSWORD", ""),
    "MSEKG_GRAPH_BASE": os.environ.get("MSEKG_GRAPH_BASE", "https://purls.helmholtz-metadaten.de/msekg/"),
    "MSEKG_REGISTRY_GRAPH": os.environ.get("MSEKG_REGISTRY_GRAPH", "https://purls.helmholtz-metadaten.de/msekg/0"),
}

COMPOSE_NETWORK = os.environ.get("COMPOSE_NETWORK", "matwerk_default")


def kg_task(task_id: str, command: str, mem_limit: str | None = None, cpus: float | None = None, mounts=None) -> DockerOperator:
    kwargs = dict(
        task_id=task_id,
        image=KG_IMAGE,
        command=["bash", "-lc", command],
        mounts=mounts or MOUNTS,
        working_dir="/app",
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode=COMPOSE_NETWORK,
        environment=ENV,
        force_pull=False,
    )
    if mem_limit:
        kwargs["mem_limit"] = mem_limit
    if cpus:
        kwargs["cpus"] = cpus
    return DockerOperator(**kwargs)

with DAG(
    dag_id="kg_harvesters_asserted",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args={"retries": 1},
    tags=["kg", "harvester"],
) as dag:

    init_run_dir = kg_task(
        "init_run_dir",
        r"""
        set -eEuo pipefail
        IFS=$'\n\t'
        RUN_DIR="__RUN_DIR__"
        mkdir -p "$RUN_DIR/data/zenodo"
        """.replace("__RUN_DIR__", RUN_DIR),
    )

    harvester_zenodo = DockerOperator(
        task_id="harvester_zenodo",
        image=KG_IMAGE,
        command=["bash", "-lc", r"""
    set -eEuo pipefail
    IFS=$'\n\t'

    RUN_DIR="__RUN_DIR__"
    mkdir -p "$RUN_DIR/data/zenodo"

    # -------------------------
    # 1) Export Zenodo
    # -------------------------
    ZENODO_TTL="$RUN_DIR/data/zenodo/zenodo.ttl"
    python -m scripts.zenodo.export_zenodo --make-snapshots --out "$ZENODO_TTL"
    test -s "$ZENODO_TTL"

    # -------------------------
    # 2) Fetch (harvest) based on a source KG TTL
    #    Option A: use spreadsheets asserted from latest run
    #    Option B: use the zenodo.ttl you just produced
    # -------------------------

    # Option A (recommended): derive targets from the KG asserted TTL
    LATEST_TTL="$(ls -1t /workspace/runs/*/data/spreadsheets_asserted.ttl 2>/dev/null | head -n 1 || true)"
    if [ -z "${LATEST_TTL}" ]; then
      echo "[ERROR] No /workspace/runs/*/data/spreadsheets_asserted.ttl found."
      exit 1
    fi
    test -s "${LATEST_TTL}"
    echo "${LATEST_TTL}" > "$RUN_DIR/data/zenodo/source_spreadsheets_asserted_path.txt"

    python /app/scripts/fetch_zenodo.py \
      --data "${LATEST_TTL}" \
      --out-csv "$RUN_DIR/data/zenodo/datasets_urls.csv" \
      --out-dir "$RUN_DIR/data/zenodo/harvested"

    # Option B (only if your zenodo.ttl contains the dataset->zenodo URL triples the script expects):
    # python /app/scripts/fetch_zenodo.py \
    #   --data "$ZENODO_TTL" \
    #   --out-csv "$RUN_DIR/data/zenodo/datasets_urls.csv" \
    #   --out-dir "$RUN_DIR/data/zenodo/harvested"

    test -s "$RUN_DIR/data/zenodo/datasets_urls.csv"
    test -d "$RUN_DIR/data/zenodo/harvested"

    # -------------------------
    # Graph IRI + provenance (local)
    # -------------------------
    TS="$(date +%s%3N)"
    GRAPH_BASE="https://purls.helmholtz-metadaten.de/msekg/"
    G="${GRAPH_BASE}${TS}"
    echo "$G" > "$RUN_DIR/data/zenodo/zenodo_graph_iri.txt"

    NOW_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    DAG_ID="{{ dag.dag_id }}"
    RUN_ID="{{ dag_run.run_id }}"
    TASK_ID="{{ ti.task_id }}"
    LOG_URL="{{ ti.log_url }}"

    PROV_TTL="$RUN_DIR/data/zenodo/zenodo_provenance.ttl"
    cat > "$PROV_TTL" <<EOF
@prefix dct: <http://purl.org/dc/terms/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix mse: <https://purls.helmholtz-metadaten.de/msekg/vocab/> .

[] a prov:Activity ;
  dct:created "${NOW_ISO}"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
  mse:airflowDagId "${DAG_ID}" ;
  mse:airflowRunId "${RUN_ID}" ;
  mse:airflowTaskId "${TASK_ID}" ;
  mse:producedGraph <${G}> ;
  mse:usedFile "${LATEST_TTL}" ;
  mse:producedFile "zenodo.ttl" ;
  mse:producedFile "datasets_urls.csv" ;
  rdfs:seeAlso <${LOG_URL}> .
EOF
    test -s "$PROV_TTL"

    echo "Done."
    ls -lah "$RUN_DIR/data/zenodo" | sed -n '1,200p'
    """.replace("__RUN_DIR__", RUN_DIR)],
        mounts=MOUNTS,
        working_dir="/app",
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode=os.environ.get("COMPOSE_NETWORK", "matwerk_default"),
        environment=ENV,
        force_pull=False,
    )
    
    harvester_endpoints = DockerOperator(
        task_id="harvester_endpoints",
        image=KG_IMAGE,
        command=["bash", "-lc", r"""
    set -eEuo pipefail
    IFS=$'\n\t'

    RUN_DIR="__RUN_DIR__"
    OUT_DIR="$RUN_DIR/data/sparql_endpoints"
    mkdir -p "$OUT_DIR"

    # -------------------------
    # Pick most recent spreadsheets TTL (source KG)
    # -------------------------
    LATEST_TTL="$(ls -1t /workspace/runs/*/data/spreadsheets_asserted.ttl 2>/dev/null | head -n 1 || true)"
    if [ -z "${LATEST_TTL}" ]; then
      echo "[ERROR] No /workspace/runs/*/data/spreadsheets_asserted.ttl found."
      exit 1
    fi
    test -s "${LATEST_TTL}"
    echo "${LATEST_TTL}" > "$OUT_DIR/source_spreadsheets_asserted_path.txt"

    # -------------------------
    # Run fetch_endpoints with RUN_DIR-local outputs
    # -------------------------
    export ALL_TTL="${LATEST_TTL}"
    export STATE_JSON="$OUT_DIR/sparql_sources.json"
    export SUMMARY_JSON="$OUT_DIR/sparql_sources_list.json"
    export STATS_TTL="$OUT_DIR/dataset_stats.ttl"
    export NAMED_GRAPHS_DIR="$OUT_DIR/named_graphs/"

    python /app/scripts/fetch_endpoints.py

    # sanity checks
    test -s "$STATE_JSON" || (echo "[ERROR] state json missing" && exit 1)
    test -s "$SUMMARY_JSON" || (echo "[ERROR] summary json missing" && exit 1)
    test -s "$STATS_TTL" || (echo "[ERROR] stats ttl missing" && exit 1)
    test -d "$NAMED_GRAPHS_DIR" || (echo "[ERROR] named graphs dir missing" && exit 1)

    # -------------------------
    # Graph IRI + provenance (local)
    # -------------------------
    TS="$(date +%s%3N)"
    GRAPH_BASE="https://purls.helmholtz-metadaten.de/msekg/"
    G="${GRAPH_BASE}${TS}"
    echo "$G" > "$OUT_DIR/sparql_endpoints_graph_iri.txt"

    NOW_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    DAG_ID="{{ dag.dag_id }}"
    RUN_ID="{{ dag_run.run_id }}"
    TASK_ID="{{ ti.task_id }}"
    LOG_URL="{{ ti.log_url }}"

    PROV_TTL="$OUT_DIR/sparql_endpoints_provenance.ttl"
    cat > "$PROV_TTL" <<EOF
@prefix dct: <http://purl.org/dc/terms/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix mse: <https://purls.helmholtz-metadaten.de/msekg/vocab/> .

[] a prov:Activity ;
  dct:created "${NOW_ISO}"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
  mse:airflowDagId "${DAG_ID}" ;
  mse:airflowRunId "${RUN_ID}" ;
  mse:airflowTaskId "${TASK_ID}" ;
  mse:producedGraph <${G}> ;
  mse:usedFile "${LATEST_TTL}" ;
  mse:producedFile "dataset_stats.ttl" ;
  mse:producedFile "sparql_sources.json" ;
  mse:producedFile "sparql_sources_list.json" ;
  rdfs:seeAlso <${LOG_URL}> .
EOF
    test -s "$PROV_TTL"

    echo "Done."
    ls -lah "$OUT_DIR" | sed -n '1,200p'
    """.replace("__RUN_DIR__", RUN_DIR)],
        mounts=MOUNTS,
        working_dir="/app",
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode=os.environ.get("COMPOSE_NETWORK", "matwerk_default"),
        environment=ENV,
        force_pull=False,
    )

    publish = kg_task(
        "publish_zenodo_artifacts",
        r"""
        set -eEuo pipefail
        IFS=$'\n\t'

        RUN_DIR="__RUN_DIR__"
        RUN_ID_SAFE="__RUN_ID_SAFE__"
        DAG_ID="kg_harvesters_asserted"

        DEST="/host_out/${DAG_ID}/${RUN_ID_SAFE}"
        mkdir -p "$DEST/data"

        # Copy Zenodo artifacts (if present)
        if [ -d "$RUN_DIR/data/zenodo" ]; then
          mkdir -p "$DEST/data/zenodo"
          cp -a "$RUN_DIR/data/zenodo/." "$DEST/data/zenodo/"
        else
          echo "[WARN] No $RUN_DIR/data/zenodo to publish."
        fi

        # Copy SPARQL endpoints artifacts (if present)
        if [ -d "$RUN_DIR/data/sparql_endpoints" ]; then
          mkdir -p "$DEST/data/sparql_endpoints"
          cp -a "$RUN_DIR/data/sparql_endpoints/." "$DEST/data/sparql_endpoints/"
        else
          echo "[WARN] No $RUN_DIR/data/sparql_endpoints to publish."
        fi

        echo "Published to: $DEST"
        find "$DEST" -maxdepth 7 -type f | sed -n '1,200p'
        """.replace("__RUN_DIR__", RUN_DIR).replace("__RUN_ID_SAFE__", RUN_ID_SAFE),
        mounts=PUBLISH_MOUNTS,
        mem_limit="2g",
        cpus=1.0,
    )
    
    bulk_load_to_virtuoso = DockerOperator(
        task_id="bulk_load_to_virtuoso",
        image=VIRTUOSO_IMAGE,
        mounts=VIRTUOSO_LOADER_MOUNTS,
        working_dir="/",
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode=COMPOSE_NETWORK,
        environment=ENV,
        force_pull=False,
        command=["bash", "-lc", r"""
    set -eEuo pipefail
    IFS=$'\n\t'

    RUN_DIR="__RUN_DIR__"
    RUN_ID_SAFE="$(basename "$RUN_DIR")"

    LOAD_ROOT="/database/airflow/kg_harvesters_asserted/${RUN_ID_SAFE}"
    mkdir -p "$LOAD_ROOT"

    # helper: copy a file into /database and load into a graph (clearing first for idempotency)
    load_one() {
      local src="$1"
      local graph="$2"
      local dest_dir="$3"

      test -s "$src"
      test -n "$graph"

      mkdir -p "$dest_dir"
      local base="$(basename "$src")"
      cp -f "$src" "$dest_dir/$base"
      chmod 0644 "$dest_dir/$base"

      # NOTE: use isql-vt to talk to the running virtuoso container on the docker network
      isql-vt "virtuoso:1111" "$TRIPLESTORE_USER" "$TRIPLESTORE_PASSWORD" <<SQL
    SPARQL CLEAR GRAPH <$graph>;
    ld_dir('$dest_dir', '$base', '$graph');
    rdf_loader_run();
    checkpoint;
    SQL

      # verify something landed
      isql-vt "virtuoso:1111" "$TRIPLESTORE_USER" "$TRIPLESTORE_PASSWORD" <<SQL | sed -n '1,200p'
    SPARQL ASK { GRAPH <$graph> { ?s ?p ?o } };
    SQL
    }

    REG_GRAPH="${MSEKG_REGISTRY_GRAPH}"

    # -------------------------
    # Zenodo outputs
    # -------------------------
    ZEN_DIR="$RUN_DIR/data/zenodo"
    if [ -d "$ZEN_DIR" ]; then
      ZEN_DATA="$ZEN_DIR/zenodo.ttl"
      ZEN_G="$(cat "$ZEN_DIR/zenodo_graph_iri.txt" 2>/dev/null || true)"
      if [ -n "${ZEN_G}" ] && [ -s "$ZEN_DATA" ]; then
        load_one "$ZEN_DATA" "$ZEN_G" "$LOAD_ROOT/zenodo"
      else
        echo "[WARN] Zenodo graph IRI or data TTL missing; skipping zenodo bulk load."
      fi

      ZEN_PROV="$ZEN_DIR/zenodo_provenance.ttl"
      if [ -s "$ZEN_PROV" ]; then
        load_one "$ZEN_PROV" "$REG_GRAPH" "$LOAD_ROOT/provenance"
      fi
    fi

    # -------------------------
    # Endpoints outputs
    # -------------------------
    EP_DIR="$RUN_DIR/data/sparql_endpoints"
    if [ -d "$EP_DIR" ]; then
      EP_DATA="$EP_DIR/dataset_stats.ttl"
      EP_G="$(cat "$EP_DIR/sparql_endpoints_graph_iri.txt" 2>/dev/null || true)"
      if [ -n "${EP_G}" ] && [ -s "$EP_DATA" ]; then
        load_one "$EP_DATA" "$EP_G" "$LOAD_ROOT/sparql_endpoints"
      else
        echo "[WARN] Endpoints graph IRI or dataset_stats.ttl missing; skipping endpoints bulk load."
      fi

      EP_PROV="$EP_DIR/sparql_endpoints_provenance.ttl"
      if [ -s "$EP_PROV" ]; then
        load_one "$EP_PROV" "$REG_GRAPH" "$LOAD_ROOT/provenance"
      fi
    fi

    echo "Bulk load done for run: $RUN_ID_SAFE"
    """.replace("__RUN_DIR__", RUN_DIR)],
    )


    # If endpoints runs in parallel, gate publish on BOTH
    init_run_dir >> [harvester_zenodo, harvester_endpoints] >> bulk_load_to_virtuoso >> publish

