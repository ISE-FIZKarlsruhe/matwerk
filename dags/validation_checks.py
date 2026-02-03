from __future__ import annotations

import os
import shutil
from glob import glob

import requests

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

DAG_ID = "validation_checks"
OUT_TTL = "spreadsheets_asserted.ttl"

LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "last_sucessfull_merge_run"
LAST_SUCCESSFUL_VALIDATED_RUN_VARIABLE_NAME = "last_sucessfull_validated_run"

@dag(
    schedule=None,
    catchup=False,
    dag_id=DAG_ID,
)
def validation_checks():
    
    @task
    def init_data_dir(ti=None):
        ctx = get_current_context()
        run_id = ctx["dag_run"].run_id

        data_dir = os.path.join(
            Variable.get("sharedfs"),
            "runs",
            ctx["dag"].dag_id,
            run_id,
        )
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(os.path.join(data_dir, "_shapes"), exist_ok=True)

        ti.xcom_push(key="datadir", value=data_dir)
        print("Validation run dir:", data_dir)

    @task
    def pull_merge_output(ti=None):
        src_dir = Variable.get(LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME)
        src_ttl = os.path.join(src_dir, OUT_TTL)

        if not os.path.exists(src_ttl):
            raise AirflowFailException(f"Missing merge output TTL: {src_ttl}")

        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        dst_ttl = os.path.join(data_dir, OUT_TTL)
        shutil.copyfile(src_ttl, dst_ttl)

        # Keep track of original merge run id for graph naming later
        source_run_id = os.path.basename(src_dir.rstrip("/"))
        ti.xcom_push(key="source_merge_dir", value=src_dir)
        ti.xcom_push(key="source_run_id", value=source_run_id)

        print("Copied merge TTL:", src_ttl, "->", dst_ttl)
        print("Source merge run_id:", source_run_id)


    @task
    def fetch_shapes(ti=None):
        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        shapes_dir = os.path.join(data_dir, "_shapes")

        api = "https://api.github.com/repos/ISE-FIZKarlsruhe/matwerk/contents/shapes?ref=main"
        items = requests.get(api).json()

        for it in items:
            if it.get("type") != "file":
                continue
            name = it.get("name", "")
            dl = it.get("download_url", "")
            if not name or not dl:
                continue
            out = os.path.join(shapes_dir, name)
            r = requests.get(dl)
            r.raise_for_status()
            with open(out, "wb") as f:
                f.write(r.content)

        shape_files = sorted([os.path.basename(p) for p in glob(os.path.join(shapes_dir, "*.ttl"))])
        sparql_files = sorted([os.path.basename(p) for p in glob(os.path.join(shapes_dir, "*.sparql"))])

        print("Downloaded SHACL shapes:", shape_files)
        print("Downloaded SPARQL queries:", sparql_files)

        ti.xcom_push(key="shape_files", value=shape_files)
        ti.xcom_push(key="sparql_files", value=sparql_files)

    def robotVerifySparqlCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        ttl = os.path.join(DATA_DIR, OUT_TTL)

        cmd = (
            "{% for q in ti.xcom_pull(task_ids='fetch_shapes', key='sparql_files') %}\n"
            f"echo 'Running ROBOT verify with: {os.path.join(DATA_DIR, '_shapes', '{{ q }}')}'\n"
            f"{ROBOT} verify --input \"{ttl}\" --queries '{os.path.join(DATA_DIR, '_shapes', '{{ q }}')}' -vvv\n"
            "{% endfor %}\n"
        )
        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    @task
    def shacl_validate(ti=None):
        from pyshacl import validate

        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        shape_files = ti.xcom_pull(task_ids="fetch_shapes", key="shape_files")

        ttl_path = os.path.join(data_dir, OUT_TTL)

        any_bad = False
        for shp_name in shape_files:
            shp = os.path.join(data_dir, "_shapes", shp_name)
            print("Running SHACL:", shp)
            conforms, _, results_text = validate(
                data_graph=ttl_path,
                shacl_graph=shp,
                data_graph_format="turtle",
                shacl_graph_format="turtle",
                inference="rdfs",
                abort_on_error=False,
                meta_shacl=False,
                debug=False,
            )
            print(results_text or "")
            if not conforms:
                any_bad = True

        if any_bad:
            raise AirflowFailException("SHACL validation failed (see logs)")

    @task
    def mark_validated_successful(ti=None):
        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        ttl_path = os.path.join(data_dir, OUT_TTL)
        if not os.path.exists(ttl_path):
            raise AirflowFailException(f"Expected TTL not found: {ttl_path}")

        Variable.set(LAST_SUCCESSFUL_VALIDATED_RUN_VARIABLE_NAME, data_dir)
        print(f"Set {LAST_SUCCESSFUL_VALIDATED_RUN_VARIABLE_NAME}={data_dir}")


    start = EmptyOperator(task_id="start")

    init = init_data_dir()
    pull = pull_merge_output()
    shapes = fetch_shapes()

    robot_verify = BashOperator(
        task_id="robot_verify_sparql",
        bash_command=robotVerifySparqlCmdTemplate(),
    )

    shacl = shacl_validate()
    done = mark_validated_successful()

    start >> init >> pull >> shapes
    shapes >> [robot_verify, shacl] >> done


validation_checks()
