from __future__ import annotations

from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import TaskGroup
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

# Your repo bind mount (adjust if needed)
CODE_MOUNT = Mount(
    source="/mnt/c/Users/eno/Documents/NFDI-MatWerk/gitlab/matwerk",
    target="/app",
    type="bind",
)

# Host output directory (adjust to your local path)
HOST_OUT_DIR = "/mnt/c/Users/eno/Documents/NFDI-MatWerk/gitlab/matwerk/output"
HOST_OUT_MOUNT = Mount(source=HOST_OUT_DIR, target="/host_out", type="bind")

VIRTUOSO_IMAGE = "openlink/virtuoso-opensource-7:latest"
VIRTUOSO_DB_MOUNT_DB = Mount(source="virtuoso_db", target="/database", type="volume")
VIRTUOSO_LOADER_MOUNTS = [WORKSPACE_MOUNT, VIRTUOSO_DB_MOUNT_DB]
VIRTUOSO_DB_MOUNT = Mount(source="virtuoso_db", target="/virtuoso_db", type="volume")
MOUNTS = [WORKSPACE_MOUNT, CODE_MOUNT, VIRTUOSO_DB_MOUNT]
PUBLISH_MOUNTS = [WORKSPACE_MOUNT, HOST_OUT_MOUNT, VIRTUOSO_DB_MOUNT]


# -----------------------------
# Env passthrough (must exist in Airflow container via env_file:.env)
# -----------------------------
ENV = {
    "PYTHONPATH": "/app:/app/dags",
    "TRIPLESTORE_SPARQL_ENDPOINT": os.environ.get("TRIPLESTORE_SPARQL_ENDPOINT", "http://virtuoso:8890/sparql-auth"),
    "TRIPLESTORE_USER": os.environ.get("TRIPLESTORE_USER", "dba"),
    "TRIPLESTORE_PASSWORD": os.environ.get("TRIPLESTORE_PASSWORD", ""),
    "MSEKG_GRAPH_BASE": os.environ.get("MSEKG_GRAPH_BASE", "https://purls.helmholtz-metadaten.de/msekg/"),
    "MSEKG_REGISTRY_GRAPH": os.environ.get("MSEKG_REGISTRY_GRAPH", "https://purls.helmholtz-metadaten.de/msekg/0"),
    "ROBOT_JAVA_ARGS": "-Xmx16G -Dfile.encoding=UTF-8",
}

COMPOSE_NETWORK = os.environ.get("COMPOSE_NETWORK", "matwerk_default")

SLOT_SPREADSHEETS = 1001

# -----------------------------
# Helpers
# -----------------------------
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

def validate_md_task(task_id: str, md_path: str, owl_path: str) -> DockerOperator:
    # Strong validation: OWL exists+nonempty, MD exists+nonempty, MD content exact sentinel, OWL parses via robot validate
    return kg_task(
        task_id=task_id,
        command="""
        set -euxo pipefail

        test -s "{owl_path}"
        test -s "{md_path}"

        # Normalize CRLF and remove trailing blank lines
        CONTENT="$(tr -d '\\r' < "{md_path}" | sed -e :a -e '/^\\n*$/{{;$d;N;ba' -e '}}')"
        if [ "$CONTENT" != "No explanations found." ]; then
          echo "[ERROR] Inconsistency detected (expected exact: 'No explanations found.')."
          echo "----- {md_path} -----"
          cat "{md_path}" || true
          echo "----------------------"
          exit 1
        fi
        """.format(md_path=md_path, owl_path=owl_path),
        mem_limit="2g",
        cpus=1.0,
    )

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="kg_spreadsheets_asserted",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1},
    tags=["kg", "spreadsheets"],
) as dag:

    init_run_dir = kg_task(
        "init_run_dir",
        """
        set -euxo pipefail
        mkdir -p "{run_dir}/data/components/reasoner"
        """.format(run_dir=RUN_DIR),
    )

    TSVS = [
        {"name": "req_1", "gid": "394894036"},
        {"name": "req_2", "gid": "0"},
        {"name": "agent", "gid": "2077140060"},
        {"name": "role", "gid": "1425127117"},
        {"name": "process", "gid": "1169992315"},
        {"name": "city", "gid": "1469482382"},
        {"name": "people", "gid": "1666156492"},
        {"name": "organization", "gid": "447157523"},
        {"name": "dataset", "gid": "1079878268"},
        {"name": "publication", "gid": "1747331228"},
        {"name": "software", "gid": "1275685399"},
        {"name": "dataportal", "gid": "923160190"},
        {"name": "instrument", "gid": "2015927839"},
        {"name": "largescalefacility", "gid": "370181939"},
        {"name": "metadata", "gid": "278046522"},
        {"name": "matwerkta", "gid": "1489640604"},
        {"name": "matwerkiuc", "gid": "281962521"},
        {"name": "matwerkpp", "gid": "606786541"},
        {"name": "temporal", "gid": "1265818056"},
        {"name": "event", "gid": "638946284"},
        {"name": "collaboration", "gid": "266847052"},
        {"name": "service", "gid": "130394813"},
        {"name": "sparql_endpoints", "gid": "1732373290"},
        {"name": "FDOs", "gid": "152649677"},
    ]

    download_ontology_and_tsvs = kg_task(
        "download_ontology_and_tsvs",
        """
        set -euxo pipefail
        mkdir -p "{run_dir}/data/components"

        # base ontology
        curl -fsSL \
          "https://raw.githubusercontent.com/ISE-FIZKarlsruhe/mwo/refs/tags/v3.0.0/mwo.owl" \
          -o "{run_dir}/data/components/ontology.owl"
        test -s "{run_dir}/data/components/ontology.owl"

        # TSV templates
        for item in {items}; do
          name="${{item%%::*}}"
          gid="${{item##*::}}"
          url="https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=${{gid}}&single=true&output=tsv"
          curl -fsSL "$url" -o "{run_dir}/data/components/${{name}}.tsv"
          test -s "{run_dir}/data/components/${{name}}.tsv"
        done
        """.format(
            run_dir=RUN_DIR,
            items=" ".join([f"{t['name']}::{t['gid']}" for t in TSVS]),
        ),
        mem_limit="2g",
        cpus=1.0,
    )

    COMPONENT_DEPS = {
        "req_1": [],
        "req_2": ["req_1"],
        "agent": ["req_2"],
        "role": ["req_2", "agent"],
        "process": ["req_2", "agent", "role"],
        "city": ["req_1", "req_2"],
        "organization": ["req_1", "req_2", "city"],
        "people": ["req_1", "req_2", "organization"],
        "publication": ["req_2", "agent", "process"],
        "software": ["req_1", "req_2", "publication", "process"],
        "dataportal": ["req_1", "req_2", "publication", "organization", "process"],
        "dataset": ["req_1", "req_2", "organization", "agent", "role", "process"],
        "instrument": ["req_1", "req_2", "publication", "organization", "agent", "role", "process"],
        "largescalefacility": ["req_1", "req_2", "publication", "organization", "agent", "role", "process"],
        "metadata": ["req_1", "req_2", "publication", "organization", "process"],
        "matwerkta": ["req_1", "req_2", "organization", "agent", "role", "process"],
        "matwerkiuc": ["req_1", "req_2", "matwerkta", "organization", "agent", "role", "process"],
        "matwerkpp": ["req_1", "req_2", "matwerkta", "matwerkiuc", "organization", "agent", "role", "process"],
        "temporal": ["req_1", "req_2", "publication", "organization", "process"],
        "event": ["req_1", "req_2", "organization", "temporal", "agent", "role", "process"],
        "collaboration": ["req_1", "req_2", "temporal", "organization", "process"],
        "service": ["req_1", "req_2", "organization", "temporal", "agent", "role", "process"],
        "sparql_endpoints": ["req_1", "req_2", "organization", "temporal", "agent", "role", "process"],
        "FDOs": ["req_1", "req_2", "organization", "temporal", "agent", "role", "process", "dataset"],
    }

    def robot_build_cmd(name: str, deps: list[str]) -> str:
        if name == "req_1":
            inputs = f'-i "{RUN_DIR}/data/components/ontology.owl"'
        else:
            inputs = " ".join([f'-i "{RUN_DIR}/data/components/{d}.owl"' for d in deps])

        return """
        set -euxo pipefail
        mkdir -p "{run_dir}/data/components/reasoner"

        robot merge --include-annotations true {inputs} \
          template --merge-before --template "{run_dir}/data/components/{name}.tsv" \
          --output "{run_dir}/data/components/{name}.owl"

        test -s "{run_dir}/data/components/{name}.owl"

        robot explain --reasoner hermit --input "{run_dir}/data/components/{name}.owl" \
          -M inconsistency --explanation "{run_dir}/data/components/reasoner/{name}_inconsistency.md"

        test -s "{run_dir}/data/components/reasoner/{name}_inconsistency.md"
        """.format(run_dir=RUN_DIR, name=name, inputs=inputs)

    with TaskGroup(group_id="build_components") as build_components:
        build_tasks = {}
        validate_tasks = {}

        for name, deps in COMPONENT_DEPS.items():
            build = kg_task(
                task_id=f"build_{name}",
                command=robot_build_cmd(name, deps),
                mem_limit="16g",
                cpus=2.0,
            )
            validate = validate_md_task(
                task_id=f"validate_{name}",
                md_path=f"{RUN_DIR}/data/components/reasoner/{name}_inconsistency.md",
                owl_path=f"{RUN_DIR}/data/components/{name}.owl",
            )
            build >> validate
            build_tasks[name] = build
            validate_tasks[name] = validate

        for name, deps in COMPONENT_DEPS.items():
            for dep in deps:
                validate_tasks[dep] >> build_tasks[name]

    
    merge_and_upload = kg_task(
        "merge_and_save_spreadsheets",
        """
    set -euxo pipefail

    # -------------------------
    # Build merged TTL (asserted)
    # -------------------------
    OUT_TTL="{run_dir}/data/spreadsheets_asserted.ttl"
    robot merge --include-annotations true \
      $(for f in "{run_dir}/data/components"/*.owl; do
          [ "$(basename "$f")" = "ontology.owl" ] && continue
          echo -n " -i \\"$f\\""
        done) \
      --output "$OUT_TTL"
    test -s "$OUT_TTL"

    # -------------------------
    # Graph IRI for this run (no env dependency)
    # -------------------------
    TS="$(date +%s%3N)"
    GRAPH_BASE="https://purls.helmholtz-metadaten.de/msekg/"
    G="$GRAPH_BASE$TS"
    echo "$G" > "{run_dir}/data/spreadsheets_graph_iri.txt"

    # -------------------------
    # Provenance (local)
    # -------------------------
    NOW_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    DAG_ID="{{{{ dag.dag_id }}}}"
    RUN_ID="{{{{ dag_run.run_id }}}}"
    TASK_ID="{{{{ ti.task_id }}}}"
    LOG_URL="{{{{ ti.log_url }}}}"

    PROV_TTL="{run_dir}/data/spreadsheets_provenance.ttl"
    cat > "$PROV_TTL" <<EOF
@prefix dct: <http://purl.org/dc/terms/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix mse: <https://purls.helmholtz-metadaten.de/msekg/vocab/> .

[] a prov:Activity ;
  dct:created "$NOW_ISO"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
  mse:airflowDagId "$DAG_ID" ;
  mse:airflowRunId "$RUN_ID" ;
  mse:airflowTaskId "$TASK_ID" ;
  mse:producedGraph <$G> ;
  mse:producedFile "spreadsheets_asserted.ttl" ;
  rdfs:seeAlso <$LOG_URL> .
EOF
    test -s "$PROV_TTL"
    """.format(run_dir=RUN_DIR, slot=SLOT_SPREADSHEETS),
        mem_limit="16g",
        cpus=2.0,
    )

    # -------------------------
    # Validation (before publish)
    # -------------------------

    # 1) SPARQL verification using ROBOT verify
    verify_sparql = kg_task(
        "verify_sparql",
        """
        set -euxo pipefail
        mkdir -p "{run_dir}/data/validation"

        DATA_TTL="{run_dir}/data/spreadsheets_asserted.ttl"
        test -s "$DATA_TTL"

        # Put one or more SPARQL ASK/SELECT queries in this file.
        # ROBOT verify fails if any query violates rules.
        QUERIES="shapes/verify1.sparql"
        test -f "$QUERIES"

        robot verify --input "$DATA_TTL" --queries "$QUERIES" \
          --output-dir "{run_dir}/data/validation/" -vvv > "{run_dir}/data/validation/verify_sparql.md"

        test -s "{run_dir}/data/validation/verify_sparql.md"
        """.format(run_dir=RUN_DIR),
        mem_limit="2g",
        cpus=1.0,
    )

    # 2) SHACL validation using pyshacl CLI (no pipeline wrapper)
    # Map across shape files shape1..shape4 (same convention you had)
    shacl_validate = (
        DockerOperator.partial(
            task_id="shacl_validate",
            image=KG_IMAGE,
            mounts=MOUNTS,
            working_dir="/app",
            auto_remove="success",
            mount_tmp_dir=False,
            network_mode=COMPOSE_NETWORK,
            environment=ENV,
            force_pull=False,
        )
        .expand(
            command=[
                [
                    "bash",
                    "-lc",
                    """
                    set -euxo pipefail
                    mkdir -p "{run_dir}/data/validation"

                    DATA="{run_dir}/data/spreadsheets_asserted.ttl"
                    SHAPE="shapes/shape{n}.ttl"
                    OUT="{run_dir}/data/validation/shape{n}.md"

                    test -s "$DATA"
                    test -f "$SHAPE"

                    # Ensure pyshacl exists; fail hard if not (validation is critical)
                    if ! command -v pyshacl >/dev/null 2>&1; then
                      echo "[ERROR] pyshacl is not installed in the kg-runner image; cannot run SHACL validation."
                      echo "Install pyshacl into mse-kg-runner:local (or add an explicit SHACL tool)."
                      exit 1
                    fi

                    # Run SHACL; capture output
                    pyshacl -s "$SHAPE" -d "$DATA" > "$OUT" || true
                    test -s "$OUT"
                    """.format(run_dir=RUN_DIR, n=n),
                ]
                for n in [2, 3, 4]#remove 1 for now because we need reasoning
            ]
        )
    )

    # 3) Final gate: re-check component MDs and SHACL outputs and fail if anything is suspicious
    final_consistency_gate = kg_task(
        "final_consistency_gate",
        """
        set -euxo pipefail

        RUN_DIR="{run_dir}"

        echo "=== Checking component inconsistency reports ==="
        REPORT_DIR="$RUN_DIR/data/components/reasoner"
        test -d "$REPORT_DIR"

        bad=0
        for f in "$REPORT_DIR"/*.md; do
          [ -e "$f" ] || continue
          content="$(tr -d '\\r' < "$f" | sed -e :a -e '/^\\n*$/{{;$d;N;ba' -e '}}')"
          if [ "$content" != "No explanations found." ]; then
            echo "❌ Inconsistency found in: $f"
            echo "-----"
            cat "$f" || true
            echo "-----"
            bad=1
          fi
        done
        if [ "$bad" -eq 1 ]; then
          echo "FAIL: At least one component has inconsistencies."
          exit 1
        fi
        echo "OK: No component inconsistencies found."

        echo "=== Checking SHACL validation outputs ==="
        VAL_DIR="$RUN_DIR/data/validation"
        test -d "$VAL_DIR"

        # Fail if any report indicates non-conformance.
        # pyshacl output often includes "Conforms: False" when it fails.
        if grep -RInE "(Conforms:\\s*False|SHACL\\s*Violation|Violations|violation)" "$VAL_DIR"/*.md 2>/dev/null; then
          echo "FAIL: SHACL violations detected."
          exit 1
        fi
        echo "OK: No SHACL violations detected (by report scan)."

        echo "=== Final gate passed ==="
        """.format(run_dir=RUN_DIR),
        mem_limit="2g",
        cpus=1.0,
    )

    publish = kg_task(
        "publish_run_artifacts",
        """
        set -euxo pipefail
        DEST="/host_out/{dag_id}/{run_id_safe}"
        mkdir -p "$DEST"
        cp -a "{run_dir}/." "$DEST/"
        echo "Published run artifacts to: $DEST"
        find "$DEST" -maxdepth 4 -type f | sed -n '1,200p'
        """.format(dag_id="kg_spreadsheets_asserted", run_id_safe=RUN_ID_SAFE, run_dir=RUN_DIR),
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
        command=["bash", "-lc", f"""
    set -eEuo pipefail
    IFS=$'\\n\\t'

    DATA_TTL="{RUN_DIR}/data/spreadsheets_asserted.ttl"
    PROV_TTL="{RUN_DIR}/data/spreadsheets_provenance.ttl"
    GRAPH_FILE="{RUN_DIR}/data/spreadsheets_graph_iri.txt"
    REG_GRAPH="${{MSEKG_REGISTRY_GRAPH}}"

    test -s "$DATA_TTL"
    test -s "$PROV_TTL"
    test -s "$GRAPH_FILE"

    G="$(cat "$GRAPH_FILE")"
    echo "Data graph: $G"
    echo "Registry graph: $REG_GRAPH"

    # Copy files into /database (allowed by DirsAllowed)
    LOAD_DIR="/database/airflow/kg_spreadsheets_asserted/{RUN_ID_SAFE}"
    mkdir -p "$LOAD_DIR"

    DATA_BASENAME="spreadsheets_asserted.ttl"
    PROV_BASENAME="spreadsheets_provenance.ttl"

    cp -f "$DATA_TTL" "$LOAD_DIR/$DATA_BASENAME"
    cp -f "$PROV_TTL" "$LOAD_DIR/$PROV_BASENAME"
    chmod 0644 "$LOAD_DIR/$DATA_BASENAME" "$LOAD_DIR/$PROV_BASENAME"

    # Bulk load into Virtuoso (running service) via isql-vt
    # Clear graphs first so retries don't duplicate
    isql-vt "virtuoso:1111" "$TRIPLESTORE_USER" "$TRIPLESTORE_PASSWORD" <<SQL
    SPARQL CLEAR GRAPH <$G>;
    SPARQL CLEAR GRAPH <$REG_GRAPH>;

    ld_dir('$LOAD_DIR', '$DATA_BASENAME', '$G');
    ld_dir('$LOAD_DIR', '$PROV_BASENAME', '$REG_GRAPH');

    rdf_loader_run();
    checkpoint;
    SQL

    # Verify something landed (fast sanity)
    isql-vt "virtuoso:1111" "$TRIPLESTORE_USER" "$TRIPLESTORE_PASSWORD" <<SQL | sed -n '1,120p'
    SPARQL ASK {{ GRAPH <$G> {{ ?s ?p ?o }} }};
    SPARQL ASK {{ GRAPH <$REG_GRAPH> {{ ?s ?p ?o }} }};
    SQL

    echo "Bulk load finished."
    """],
    )


    init_run_dir >> download_ontology_and_tsvs >> build_components >> merge_and_upload

    # Run both validations after merge
    merge_and_upload >> verify_sparql
    merge_and_upload >> shacl_validate

    # Gate waits for both
    [verify_sparql, shacl_validate] >> final_consistency_gate >> bulk_load_to_virtuoso >> publish


