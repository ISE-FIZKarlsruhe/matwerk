from __future__ import annotations
import os, sys

print('getcwd:      ', os.getcwd())
print('__file__:    ', __file__)
local_path = os.path.dirname(__file__)
print('adding local path', local_path)
sys.path.append(local_path)

from datetime import datetime
from glob import glob
from common.utils import run_cmd, download_github_dir
import requests

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

DAG_ID = "harvester_endpoints"

SCRIPTS_REPO = "ISE-FIZKarlsruhe/matwerk"
SCRIPTS_REF = "main"

LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "matwerk_last_successful_merge_run"
LAST_SUCCESSFUL_HARVESTER_ENDPOINTS_RUN_VARIABLE_NAME = "matwerk_last_successful_harvester_endpoints_run"

MERGE_TTL_NAME = "spreadsheets_asserted.ttl"
OUTPUT = "dataset_stats.ttl"

SCRIPTS_REPO = "ISE-FIZKarlsruhe/matwerk"
SCRIPTS_REF = "main"
SCRIPTS_PATH = "scripts"

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
)
def harvester_endpoints():

    @task()
    def init_data_dir(ti=None):
        ctx = get_current_context()
        sharedfs = Variable.get("matwerk_sharedfs")
        rid = ctx["dag_run"].run_id
        run_dir = os.path.join(sharedfs, "runs", ctx["dag"].dag_id, rid)

        scripts_dir = os.path.join(run_dir, "scripts")
        os.makedirs(run_dir, exist_ok=True)
        os.makedirs(scripts_dir, exist_ok=True)

        ti.xcom_push(key="run_dir", value=run_dir)
        ti.xcom_push(key="scripts_dir", value=scripts_dir)

    @task()
    def fetchscripts(ti=None):
        scripts_dir = ti.xcom_pull(task_ids="init_data_dir", key="scripts_dir")
        download_github_dir(SCRIPTS_REPO, SCRIPTS_PATH, SCRIPTS_REF, scripts_dir)


    @task()
    def run_harvester(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")
        scripts_dir = ti.xcom_pull(task_ids="init_data_dir", key="scripts_dir")
        source_merge_dir = Variable.get(LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME)
        merge_ttl = os.path.join(source_merge_dir, MERGE_TTL_NAME)

        print("Using merge TTL from variable:", merge_ttl)
        
        # download MWO into this run
        mwo_local = os.path.join(run_dir, "ontology.owl")
        rr = requests.get(Variable.get("matwerk_ontology"), timeout=120)
        rr.raise_for_status()
        with open(mwo_local, "wb") as f:
            f.write(rr.content)
            
        # outputs in run_dir
        named_graphs_dir = os.path.join(run_dir, "named_graphs")
        os.makedirs(named_graphs_dir, exist_ok=True)
        state_json   = os.path.join(run_dir, "sparql_sources.json")
        summary_json = os.path.join(run_dir, "sparql_sources_list.json")
        stats_ttl    = os.path.join(run_dir, OUTPUT)

        run_cmd(["python", os.path.join(scripts_dir, "fetch_endpoints.py"),
                "--all-ttl", merge_ttl,
                "--mwo-owl", mwo_local,
                "--state-json", state_json,
                "--summary-json", summary_json,
                "--stats-ttl", stats_ttl,
                "--named-graphs-dir", named_graphs_dir,
                ],
                cwd=run_dir)

        print("Done. Outputs in:", run_dir)

    @task()
    def mark_success(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")
        Variable.set(LAST_SUCCESSFUL_HARVESTER_ENDPOINTS_RUN_VARIABLE_NAME, run_dir)
        print(f"Set {LAST_SUCCESSFUL_HARVESTER_ENDPOINTS_RUN_VARIABLE_NAME}={run_dir}")

    init = init_data_dir()
    scripts = fetchscripts()
    harvest = run_harvester()
    done = mark_success()

    trigger_reason_endpoints = TriggerDagRunOperator(
        task_id="trigger_reason_endpoints",
        trigger_dag_id="reason",
        wait_for_completion=True,
        conf={
            "artifact": "endpoints",
            "source_run_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "target_run_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "in_ttl": "dataset_stats.ttl",
        },
    )

    trigger_validation_endpoints = TriggerDagRunOperator(
        task_id="trigger_validation_endpoints",
        trigger_dag_id="validation_checks",
        wait_for_completion=True,
        conf={
            "artifact": "endpoints",

            "target_run_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",

            "asserted_source_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "asserted_ttl": "dataset_stats.ttl",

            "reason_source_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "inferences_ttl": "endpoints_inferences.ttl",
        },
    )

    init >> scripts >> harvest >> done >> trigger_reason_endpoints >> trigger_validation_endpoints



harvester_endpoints()
