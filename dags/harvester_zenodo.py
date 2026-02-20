from __future__ import annotations
import os, sys

print('getcwd:      ', os.getcwd())
print('__file__:    ', __file__)
local_path = os.path.dirname(__file__)
print('adding local path', local_path)
sys.path.append(local_path)

from datetime import datetime
from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from scripts.zenodo import export_zenodo
from scripts import fetch_zenodo

DAG_ID = "harvester_zenodo"

LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "matwerk_last_successful_merge_run"
LAST_SUCCESSFUL_HARVESTER_ZENODO_RUN_VARIABLE_NAME = "matwerk_last_successful_harvester_zenodo_run"

OUT_TTL = "spreadsheets_asserted.ttl"
OUTPUT = "zenodo.ttl"

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    tags=["matwerk"],
)
def harvester_zenodo():

    @task()
    def init_data_dir(ti=None):
        ctx = get_current_context()
        sharedfs = Variable.get("matwerk_sharedfs")
        rid = ctx["dag_run"].run_id
        run_dir = os.path.join(sharedfs, "runs", ctx["dag"].dag_id, rid)
        os.makedirs(run_dir, exist_ok=True)
        ti.xcom_push(key="run_dir", value=run_dir)

    @task()
    def run_harvester(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")
        source_merge_dir = Variable.get(LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME)
        merge_ttl = os.path.join(source_merge_dir, OUT_TTL)

        # 1) Export Zenodo (writes zenodo.ttl)
        zenodo_ttl = os.path.join(run_dir, OUTPUT)
        export_zenodo.run(["--make-snapshots", "--out", zenodo_ttl])

        # 2) Harvest based on asserted TTL
        out_csv = os.path.join(run_dir, "datasets_urls.csv")
        out_harvested = os.path.join(run_dir, "harvested")
        os.makedirs(out_harvested, exist_ok=True)
        
        fetch_zenodo.run(
            data=merge_ttl,
            out_csv=out_csv,
            out_dir=out_harvested,
        )

        print("Harvester done. Output:", run_dir)

    @task()
    def mark_success(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")
        Variable.set(LAST_SUCCESSFUL_HARVESTER_ZENODO_RUN_VARIABLE_NAME, run_dir)
        print(f"Set {LAST_SUCCESSFUL_HARVESTER_ZENODO_RUN_VARIABLE_NAME}={run_dir}")

    init = init_data_dir()
    harvest = run_harvester()
    done = mark_success()

    trigger_reason_zenodo = TriggerDagRunOperator(
        task_id="trigger_reason_zenodo",
        trigger_dag_id="reason",
        wait_for_completion=True,
        conf={
            "artifact": "zenodo",
            "source_run_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "target_run_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "in_ttl": "zenodo.ttl",
        },
    )


    trigger_validation_zenodo = TriggerDagRunOperator(
        task_id="trigger_validation_zenodo",
        trigger_dag_id="validation_checks",
        wait_for_completion=True,
        conf={
            "artifact": "zenodo",

            "target_run_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",

            "asserted_source_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "asserted_ttl": "zenodo.ttl",
            
            "reason_source_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "inferences_ttl": "zenodo_inferences.ttl",
        },
    )

    init >> harvest >> done >> trigger_reason_zenodo >> trigger_validation_zenodo

harvester_zenodo()
