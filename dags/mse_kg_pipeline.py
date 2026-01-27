from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

KG_IMAGE = "{{ dag_run.conf.get('kg_image', 'mse-kg-runner:local') }}"
SHMARQL_IMAGE = "{{ dag_run.conf.get('shmarql_image', 'ghcr.io/epoz/shmarql:latest') }}"
RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
RUN_DIR = f"/workspace/runs/{RUN_ID_SAFE}"

WORKSPACE_MOUNT = Mount(source="kg_workspace", target="/workspace", type="volume")

CODE_MOUNT = Mount(
    source="/mnt/c/Users/eno/Documents/NFDI-MatWerk/gitlab/matwerk",
    target="/app",
    type="bind",
)

MOUNTS = [WORKSPACE_MOUNT, 
          CODE_MOUNT
          ]

HOST_OUT_MOUNT = Mount(
    source="/mnt/c/Users/eno/Documents/NFDI-MatWerk/gitlab/matwerk/out",
    target="/host_out",
    type="bind",
)

PUBLISH_MOUNTS = [WORKSPACE_MOUNT, HOST_OUT_MOUNT]

def inputs_for_component(name: str, deps: dict[str, list[str]], run_dir: str) -> list[str]:
    return [f"{run_dir}/data/components/{dep}.owl" for dep in deps.get(name, [])]


def kg_container_task(
    *,
    task_id: str,
    command: str,
    mem_limit: str | None = None,
    cpus: float | None = None,
    environment: dict | None = None,
) -> DockerOperator:
    base_kwargs = dict(
        task_id=task_id,
        image=KG_IMAGE,
        command=["bash", "-lc", command],
        mounts=MOUNTS,                 # IMPORTANT: use MOUNTS (workspace + code)
        working_dir="/app",
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="bridge",
        environment=environment or {},
        force_pull=False,
    )
    if mem_limit is not None:
        base_kwargs["mem_limit"] = mem_limit
    if cpus is not None:
        base_kwargs["cpus"] = cpus
    return DockerOperator(**base_kwargs)


with DAG(
    dag_id="mse_kg_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
    tags=["kg", "mse"],
) as dag:

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
            mounts=MOUNTS,              # IMPORTANT
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
                    python -m pipeline.download_tsv \
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
        component_tasks = {}
        for name in COMPONENT_DEPS.keys():
            dep_inputs = inputs_for_component(name, COMPONENT_DEPS, RUN_DIR)
            inputs_args = " ".join([f'--inputs "{p}"' for p in dep_inputs])
            component_tasks[name] = kg_container_task(
                task_id=f"build_{name}",
                command=f"""
                set -euxo pipefail
                mkdir -p "{RUN_DIR}/data/components/reasoner"
                python -m pipeline.build_component \
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

        for name, deps in COMPONENT_DEPS.items():
            for dep in deps:
                component_tasks[dep] >> component_tasks[name]

    # -------------------------
    # Merge components only (1st merge)
    # -------------------------
    merge_components_only = kg_container_task(
        task_id="merge_components_only",
        command=f"""
        set -euxo pipefail
        python -m pipeline.merge_components \
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
        python -m pipeline.export_zenodo \
          --run-dir "{RUN_DIR}" \
          --make-snapshots
        """,
    )

    # -------------------------
    # Merge base + Zenodo (2nd merge) -> final all_NotReasoned.ttl
    # -------------------------
    merge_with_zenodo = kg_container_task(
        task_id="merge_with_zenodo",
        command=f"""
        set -euxo pipefail
        python -m pipeline.merge_zenodo \
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
        python -m pipeline.fetch_endpoints \
          --in-ttl "{RUN_DIR}/data/all_NotReasoned.ttl" \
          --run-dir "{RUN_DIR}"
        """,
    )

    # -------------------------
    # Reason ABox (produces all.ttl) # TODOs: implement reasoning (Gunjan)
    # -------------------------
    reason_abox = kg_container_task(
        task_id="reason_abox_rules",
        command=f"""
        set -euxo pipefail
        python -m pipeline.reason_abox_rules \
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
        python -m pipeline.fetch_zenodo \
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
            mounts=MOUNTS,              # IMPORTANT
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
                    python -m pipeline.validate_shacl \
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
        python -m pipeline.verify_sparql \
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
        python -m pipeline.docs_widoco \
          --in-ttl "{RUN_DIR}/data/all.ttl" \
          --out-dir "{RUN_DIR}/docs"
        """,
        mem_limit="8g",
        cpus=2.0,
    )

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




    publish = DockerOperator(
        task_id="publish",
        image=KG_IMAGE,
        command=["bash", "-lc", f"""
        set -euxo pipefail
        echo "RUN_DIR={RUN_DIR}"

        # Create a local folder per run id
        OUT="/host_out/{RUN_ID_SAFE}"
        mkdir -p "$OUT"

        # Copy artifacts to host
        cp -v "{RUN_DIR}/data/all.ttl" "$OUT/all.ttl"
        cp -v "{RUN_DIR}/data/all_NotReasoned.ttl" "$OUT/all_NotReasoned.ttl" || true
        if [ -d "{RUN_DIR}/site" ]; then
            rm -rf "$OUT/site"
            cp -rv "{RUN_DIR}/site" "$OUT/site"
        fi

        echo "Wrote to host folder: $OUT"
        ls -lah "$OUT"
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

    [verify_sparql, shmarql_docs, shacl_validate, fetch_zenodo] >> publish
