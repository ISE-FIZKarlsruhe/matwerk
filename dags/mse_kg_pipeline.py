from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount

# -----------------------------
# Images (templated overrides)
# -----------------------------
KG_IMAGE = "{{ dag_run.conf.get('kg_image', 'mse-kg-runner:local') }}"
SHMARQL_IMAGE = "{{ dag_run.conf.get('shmarql_image', 'ghcr.io/epoz/shmarql:v0.66') }}"

# -----------------------------
# Run paths
# -----------------------------
RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
RUN_DIR = f"/workspace/runs/{RUN_ID_SAFE}"

# -----------------------------
# Mounts
# -----------------------------
WORKSPACE_MOUNT = Mount(source="kg_workspace", target="/workspace", type="volume")

# Dev mount: host repo -> /app so pipeline edits apply immediately
CODE_MOUNT = Mount(
    source="/mnt/c/Users/eno/Documents/NFDI-MatWerk/gitlab/matwerk",
    target="/app",
    type="bind",
)

MOUNTS = [WORKSPACE_MOUNT, CODE_MOUNT]

# Host output directory (bind)
HOST_OUT_MOUNT = Mount(
    source="/mnt/c/Users/eno/Documents/NFDI-MatWerk/gitlab/matwerk/out",
    target="/host_out",
    type="bind",
)

PUBLISH_MOUNTS = [WORKSPACE_MOUNT, HOST_OUT_MOUNT]


# -----------------------------
# Helpers
# -----------------------------
def inputs_for_component(name: str, deps: dict[str, list[str]], run_dir: str) -> list[str]:
    return [f"{run_dir}/data/components/{dep}.owl" for dep in deps.get(name, [])]


def kg_container_task(
    *,
    task_id: str,
    command: str,
    mem_limit: str | None = None,
    cpus: float | None = None,
    environment: dict | None = None,
    mounts=None,
    image: str | None = None,
) -> DockerOperator:
    base_kwargs = dict(
        task_id=task_id,
        image=image or KG_IMAGE,
        command=["bash", "-lc", command],
        mounts=mounts or MOUNTS,
        working_dir="/app",
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="bridge",
        environment={
            "PYTHONPATH": "/app:/app/dags",
            **(environment or {}),
        },
        force_pull=False,
    )
    if mem_limit is not None:
        base_kwargs["mem_limit"] = mem_limit
    if cpus is not None:
        base_kwargs["cpus"] = cpus
    return DockerOperator(**base_kwargs)


def validate_md_task(task_id: str, md_path: str) -> DockerOperator:
    """
    Fail if ROBOT explain produced anything other than "No explanations found."
    Mirrors the online DAG behavior.
    """
    return kg_container_task(
        task_id=task_id,
        command=f"""
        set -euxo pipefail
        test -f "{md_path}"
        echo "----- {md_path} -----"
        cat "{md_path}" || true
        echo "----------------------"

        # Normalize CRLF, strip trailing blank lines; then compare
        CONTENT="$(tr -d '\\r' < "{md_path}" | sed -e :a -e '/^\\n*$/{{;$d;N;ba' -e '}}')"
        if [ "$CONTENT" != "No explanations found." ]; then
          echo "[ERROR] Inconsistency detected (expected: 'No explanations found.')."
          exit 1
        fi
        """,
        mem_limit="2g",
        cpus=1.0,
    )


# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="mse_kg_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
    tags=["kg", "mse"],
) as dag:
    # -------------
    # Init workspace
    # -------------
    init_run_dir = kg_container_task(
        task_id="init_run_dir",
        command=f"""
        set -euxo pipefail
        mkdir -p "{RUN_DIR}/data" "{RUN_DIR}/logs"
        mkdir -p "{RUN_DIR}/data/components" "{RUN_DIR}/data/zenodo" "{RUN_DIR}/data/validation"
        """,
    )

    # --------------------------
    # Download TSVs (parallel)
    # --------------------------
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

    download_tsvs = (
        DockerOperator.partial(
            task_id="download_tsvs",
            image=KG_IMAGE,
            mounts=MOUNTS,
            working_dir="/app",
            auto_remove="success",
            mount_tmp_dir=False,
            network_mode="bridge",
            force_pull=False,
        )
        .expand(
            command=[
                [
                    "bash",
                    "-lc",
                    f"""
                    set -euxo pipefail
                    mkdir -p "{RUN_DIR}/data/components"
                    python -m dags.pipeline.download_tsv \
                      --name "{t['name']}" \
                      --gid "{t['gid']}" \
                      --out "{RUN_DIR}/data/components/{t['name']}.tsv"
                    """,
                ]
                for t in TSVS
            ]
        )
    )

    # -------------------------
    # Build OWL components graph
    # -------------------------
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

    with TaskGroup(group_id="build_components") as build_components:
        build_tasks = {}
        validate_tasks = {}

        for name in COMPONENT_DEPS.keys():
            dep_inputs = inputs_for_component(name, COMPONENT_DEPS, RUN_DIR)
            inputs_args = " ".join([f'--inputs "{p}"' for p in dep_inputs])

            build = kg_container_task(
                task_id=f"build_{name}",
                command=f"""
                set -euxo pipefail
                mkdir -p "{RUN_DIR}/data/components/reasoner"
                python -m dags.pipeline.build_component \
                  --name "{name}" \
                  --src-owl "ontology/mwo-full.owl" \
                  --tsv "{RUN_DIR}/data/components/{name}.tsv" \
                  --out-owl "{RUN_DIR}/data/components/{name}.owl" \
                  --out-explain "{RUN_DIR}/data/components/reasoner/{name}_inconsistency.md" \
                  {inputs_args}
                """,
                mem_limit="16g",
                cpus=2.0,
                environment={"ROBOT_JAVA_ARGS": "-Xmx16G -Dfile.encoding=UTF-8"},
            )

            validate = validate_md_task(
                task_id=f"validate_{name}",
                md_path=f"{RUN_DIR}/data/components/reasoner/{name}_inconsistency.md",
            )

            build >> validate

            build_tasks[name] = build
            validate_tasks[name] = validate

        # dependencies should gate on "validated upstream"
        for name, deps in COMPONENT_DEPS.items():
            for dep in deps:
                validate_tasks[dep] >> build_tasks[name]

    # -------------------------
    # Merge components only (1st merge)
    # -------------------------
    merge_components_only = kg_container_task(
        task_id="merge_components_only",
        command=f"""
        set -euxo pipefail
        python -m dags.pipeline.merge_components \
          --src-owl "ontology/mwo-full.owl" \
          --components-dir "{RUN_DIR}/data/components" \
          --out-owl "{RUN_DIR}/data/all_NotReasoned.base.owl" \
          --out-ttl "{RUN_DIR}/data/all_NotReasoned.base.ttl"
        """,
        mem_limit="16g",
        cpus=2.0,
    )

    # -------------------------
    # Export Zenodo (after base merge)
    # -------------------------
    export_zenodo = kg_container_task(
        task_id="export_zenodo",
        command=f"""
        set -euxo pipefail
        python -m dags.pipeline.export_zenodo \
          --run-dir "{RUN_DIR}" \
          --make-snapshots
        """,
    )

    # -------------------------
    # Merge base + Zenodo (2nd merge) -> all_NotReasoned.ttl
    # -------------------------
    merge_with_zenodo = kg_container_task(
        task_id="merge_with_zenodo",
        command=f"""
        set -euxo pipefail
        python -m dags.pipeline.merge_zenodo \
          --base-ttl "{RUN_DIR}/data/all_NotReasoned.base.ttl" \
          --zenodo-ttl "{RUN_DIR}/data/zenodo/zenodo.ttl" \
          --out-ttl "{RUN_DIR}/data/all_NotReasoned.ttl"
        """,
        mem_limit="16g",
        cpus=2.0,
    )

    # ----------------
    # Fetch endpoints (reads all_NotReasoned.ttl)
    # ----------------
    fetch_endpoints = kg_container_task(
        task_id="fetch_endpoints",
        command=f"""
        set -euxo pipefail
        python -m dags.pipeline.fetch_endpoints \
          --in-ttl "{RUN_DIR}/data/all_NotReasoned.ttl" \
          --run-dir "{RUN_DIR}"
        """,
    )

    # -------------------------
    # Reason ABox (produces all.ttl)
    # -------------------------
    reason_abox = kg_container_task(
        task_id="reason_abox_rules",
        command=f"""
        set -euxo pipefail
        python -m dags.pipeline.reason_abox_rules \
          --in-ttl "{RUN_DIR}/data/all_NotReasoned.ttl" \
          --rules-dir "scripts/rules" \
          --out-ttl "{RUN_DIR}/data/all.ttl" \
          --work-dir "{RUN_DIR}/data/reasoned_steps"
        """,
        mem_limit="16g",
        cpus=2.0,
    )

    # ----------------
    # Fetch zenodo (queries all.ttl)
    # ----------------
    fetch_zenodo = kg_container_task(
        task_id="fetch_zenodo",
        command=f"""
        set -euxo pipefail
        python -m dags.pipeline.fetch_zenodo \
          --data "{RUN_DIR}/data/all.ttl" \
          --run-dir "{RUN_DIR}"
        """,
    )

    # -------------------------
    # Validation
    # -------------------------
    shacl_validate = (
        DockerOperator.partial(
            task_id="shacl_validate",
            image=KG_IMAGE,
            mounts=MOUNTS,
            working_dir="/app",
            auto_remove="success",
            mount_tmp_dir=False,
            network_mode="bridge",
            force_pull=False,
        )
        .expand(
            command=[
                [
                    "bash",
                    "-lc",
                    f"""
                    set -euxo pipefail
                    mkdir -p "{RUN_DIR}/data/validation"
                    python -m dags.pipeline.validate_shacl \
                      --shape "shapes/shape{n}.ttl" \
                      --data "{RUN_DIR}/data/all.ttl" \
                      --out "{RUN_DIR}/data/validation/shape{n}.md"
                    """,
                ]
                for n in [1, 2, 3, 4]
            ]
        )
    )

    verify_sparql = kg_container_task(
        task_id="verify_sparql",
        command=f"""
        set -euxo pipefail
        mkdir -p "{RUN_DIR}/data/validation"
        python -m dags.pipeline.verify_sparql \
          --data "{RUN_DIR}/data/all.ttl" \
          --queries "shapes/verify1.sparql" \
          --out-dir "{RUN_DIR}/data/validation"
        """,
    )

    # -------------------------
    # Docs
    # -------------------------
    widoco_docs = kg_container_task(
        task_id="widoco_docs",
        command=f"""
        set -euxo pipefail
        python -m dags.pipeline.docs_widoco \
          --in-ttl "{RUN_DIR}/data/all.ttl" \
          --out-dir "{RUN_DIR}/docs"
        """,
        mem_limit="8g",
        cpus=2.0,
    )

    # Shmarql runs in its own image and writes /src/site by default; copy into RUN_DIR/site
    shmarql_docs = DockerOperator(
        task_id="shmarql_docs",
        image=SHMARQL_IMAGE,
        mounts=MOUNTS,
        working_dir="/app",
        entrypoint=["bash", "-lc"],
        command=f"""
        set -euxo pipefail

        test -d "{RUN_DIR}/docs"
        test -f "/app/a.yml"
        test -f "/app/mkdocs.yml"

        rm -rf /src/docs /src/site
        mkdir -p /src/site
        ln -s "{RUN_DIR}/docs" /src/docs

        cd /app
        uv run python -m shmarql docs_build -f a.yml

        mkdir -p "{RUN_DIR}/site"
        rm -rf "{RUN_DIR}/site"/*
        cp -r /src/site/. "{RUN_DIR}/site/"
        """,
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="bridge",
        force_pull=False,
    )

    final_consistency_gate = DockerOperator(
        task_id="final_consistency_gate",
        image=KG_IMAGE,
        mounts=MOUNTS,
        working_dir="/app",
        entrypoint=["bash", "-lc"],
        command=f"""
        set -euxo pipefail

        RUN_DIR="{RUN_DIR}"

        echo "=== Checking component inconsistency reports ==="
        REPORT_DIR="$RUN_DIR/data/components/reasoner"
        if [ -d "$REPORT_DIR" ]; then
        # Any report that is NOT exactly "No explanations found." should fail.
        bad=0
        for f in "$REPORT_DIR"/*.md; do
            [ -e "$f" ] || continue
            content="$(cat "$f" | tr -d '\\r' | sed -e 's/[[:space:]]\\+$//' )"
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
        else
        echo "WARNING: No component reasoner report directory found: $REPORT_DIR"
        fi

        echo "=== Checking SHACL validation outputs ==="
        VAL_DIR="$RUN_DIR/data/validation"
        if [ -d "$VAL_DIR" ]; then
        # Heuristic: fail if any validation markdown mentions common violation indicators.
        # Adjust patterns to your validator output.
        if grep -RInE "(Violation|violations|Conforms:\\s*false|SHACL\\s*Violation)" "$VAL_DIR"/*.md 2>/dev/null; then
            echo "FAIL: SHACL violations detected in validation reports."
            exit 1
        fi
        echo "OK: No SHACL violations detected (by heuristic scan)."
        else
        echo "WARNING: No validation directory found: $VAL_DIR"
        fi

        echo "=== Final gate passed ==="
        """,
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="bridge",
        force_pull=False,
    )

    # -------------------------
    # Publish artifacts to host out/<run_id>/*
    # -------------------------
    publish = DockerOperator(
        task_id="publish",
        image=KG_IMAGE,
        command=["bash", "-lc", f"""
        set -euxo pipefail

        OUT="/host_out/{RUN_ID_SAFE}"
        mkdir -p "$OUT"

        echo "Copying entire run dir:"
        echo "  SRC: {RUN_DIR}"
        echo "  DST: $OUT"

        # If rsync exists, it's the best option. Fallback to cp otherwise.
        if command -v rsync >/dev/null 2>&1; then
        rsync -aH --delete \
            --exclude='tmp/' \
            --exclude='*.tmp' \
            --exclude='__pycache__/' \
            "{RUN_DIR}/" "$OUT/"
        else
        # Busybox/cp fallback (no delete, but copies everything)
        mkdir -p "$OUT"
        cp -a "{RUN_DIR}/." "$OUT/"
        fi

        echo "Published to host: $OUT"
        find "$OUT" -maxdepth 5 -type f | sed -n '1,200p'
        """],
        mounts=PUBLISH_MOUNTS,
        working_dir="/app",
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="bridge",
        force_pull=False,
    )


    # -------------------------
    # Dependency wiring
    # -------------------------
    init_run_dir >> download_tsvs >> build_components >> merge_components_only >> export_zenodo >> merge_with_zenodo
    merge_with_zenodo >> fetch_endpoints >> reason_abox >> fetch_zenodo

    reason_abox >> shacl_validate
    reason_abox >> verify_sparql
    reason_abox >> widoco_docs >> shmarql_docs

    # Publish should wait for everything you want exported to host
    [verify_sparql, shmarql_docs, shacl_validate, fetch_zenodo] >> final_consistency_gate >> publish

