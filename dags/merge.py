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

SUCCESFULL_RUN_VARIABLE_NAME = "matwerk_last_sucessfull_spreadsheet_run"
LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "matwerk_last_sucessfull_merge_run"


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

        data_dir = os.path.join(Variable.get("matwerk_sharedfs"), "runs", ctx["dag"].dag_id, run_id,)
        
        print("Creating directory for run:", data_dir)
        ti.xcom_push(key="datadir", value=data_dir)
        ti.xcom_push(key="run_id", value=run_id)
        ti.xcom_push(key="source_run_dir", value=Variable.get(SUCCESFULL_RUN_VARIABLE_NAME))


    # -----------------------------
    # ROBOT command templates
    # -----------------------------
    def robotMergeCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        SOURCE_DIR = "SOURCE_DIR"
        
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'
        XCOM_SOURCEDIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="source_run_dir") }}'
        
        ttl = os.path.join(DATA_DIR, OUT_TTL)

        cmd = (
            f"{ROBOT} merge --include-annotations true"
            f" --inputs '{os.path.join('SOURCE_DIR', '*.owl')}'"
            f" --output '{ttl}'"
        )
        cmd = cmd.replace(DATA_DIR, XCOM_DATADIR)
        cmd = cmd.replace(SOURCE_DIR, XCOM_SOURCEDIR)
        return cmd

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

    init >> wait

    robot_merge = BashOperator(task_id="robot_merge_and_convert", bash_command=robotMergeCmdTemplate(),)

    robot_hermit_explain = BashOperator(task_id="robot_hermit_explain", bash_command=robotHermitExplainCmdTemplate(),)

    robot_hermit_valid = PythonOperator(task_id="robot_hermit_valid", python_callable=isvalid, op_kwargs={"filename": "hermit_inconsistency.md"},)

    done = mark_merge_successful()

    wait >> robot_merge
    robot_merge >> robot_hermit_explain >> robot_hermit_valid >> done


merge()
