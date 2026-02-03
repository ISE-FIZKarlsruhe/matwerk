from __future__ import annotations

import os
import shutil
from glob import glob

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

DAG_ID = "merge"
OUT_TTL = "spreadsheets_asserted.ttl"

SUCCESFULL_RUN_VARIABLE_NAME = "last_sucessfull_spreadsheet_run"
LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "last_sucessfull_merge_run"


@dag(
    schedule=None,
    catchup=False,
    dag_id=DAG_ID,
)
def merge():
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
        print("Creating directory for run:", data_dir)
        os.makedirs(os.path.join(data_dir, "components"), exist_ok=True)
        ti.xcom_push(key="datadir", value=data_dir)
        ti.xcom_push(key="run_id", value=run_id)

    @task()
    def stage_components(ti=None):
        source_run_dir = Variable.get(SUCCESFULL_RUN_VARIABLE_NAME)

        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        components_dir = os.path.join(data_dir, "components")

        owls = glob(os.path.join(source_run_dir, "*.owl"))
        component_names: list[str] = []
        for f in owls:
            base = os.path.basename(f)
            name, _ = os.path.splitext(base)
            component_names.append(name)
            shutil.copyfile(f, os.path.join(components_dir, f"{name}.owl"))

        print("Staged OWL components to:", components_dir)
        print("Component names:", component_names)
        ti.xcom_push(key="component_names", value=component_names)

    # -----------------------------
    # ROBOT command templates
    # -----------------------------
    def robotMergeCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        ttl = os.path.join(DATA_DIR, OUT_TTL)

        cmd = (
            f"{ROBOT} merge --include-annotations true"
            f" --inputs '{os.path.join(DATA_DIR, 'components', '*.owl')}'"
            f" --output '{ttl}'"
        )
        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    def robotHermitExplainCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        ttl = os.path.join(DATA_DIR, OUT_TTL)
        explain_md = os.path.join(DATA_DIR, "hermit_inconsistency.md")

        cmd = (
            f"{ROBOT} explain --reasoner hermit --input '{ttl}' "
            f"-M inconsistency --explanation '{explain_md}'"
        )
        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    def isvalid(filename: str, ti=None):
        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        path = os.path.join(data_dir, filename)
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            s = f.read()
        if s.strip() != "No explanations found.":
            print(s)
            raise ValueError("Ontology inconsistent or explanation not empty")


    @task
    def mark_merge_successful(ti=None):
        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        ttl_path = os.path.join(data_dir, OUT_TTL)
        if not os.path.exists(ttl_path):
            raise FileNotFoundError(f"Expected TTL not found: {ttl_path}")

        Variable.set(LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME, data_dir)
        print(f"Set {LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME}={data_dir}")

    wait = EmptyOperator(task_id="wait_for_inputs")

    init = init_data_dir()
    staged = stage_components()

    init >> staged >> wait

    robot_merge = BashOperator(
        task_id="robot_merge_and_convert",
        bash_command=robotMergeCmdTemplate(),
    )

    robot_hermit_explain = BashOperator(
        task_id="robot_hermit_explain",
        bash_command=robotHermitExplainCmdTemplate(),
    )

    robot_hermit_valid = PythonOperator(
        task_id="robot_hermit_valid",
        python_callable=isvalid,
        op_kwargs={"filename": "hermit_inconsistency.md"},
    )

    done = mark_merge_successful()

    wait >> robot_merge
    robot_merge >> robot_hermit_explain >> robot_hermit_valid >> done


merge()
