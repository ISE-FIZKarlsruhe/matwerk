from __future__ import annotations
import os, sys

print('getcwd:      ', os.getcwd())
print('__file__:    ', __file__)
local_path = os.path.dirname(__file__)
print('adding local path', local_path)
sys.path.append(local_path)

from datetime import datetime
from common.utils import run_cmd, download_github_dir
from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException


DAG_ID = "harvester_zenodo"

LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "last_sucessfull_merge_run"
LAST_SUCCESSFUL_HARVESTER_ZENODO_RUN_VARIABLE_NAME = "last_sucessfull_harvester_zenodo_run"

OUT_TTL = "spreadsheets_asserted.ttl"
OUTPUT = "zenodo.ttl"

SCRIPTS_REPO = "ISE-FIZKarlsruhe/matwerk"
SCRIPTS_REF = "main"
SCRIPTS_PATH = "scripts"  # repo folder


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
)
def harvester_zenodo():

    @task()
    def init_data_dir(ti=None):
        ctx = get_current_context()
        sharedfs = Variable.get("sharedfs")

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
        merge_ttl = os.path.join(source_merge_dir, OUT_TTL)
        if not os.path.exists(merge_ttl) or os.path.getsize(merge_ttl) == 0:
            raise AirflowFailException(f"Missing/empty merge TTL: {merge_ttl}")

        print("Using merge TTL from variable:", merge_ttl)

        env = os.environ.copy()
        env["PYTHONPATH"] = run_dir + (":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")

        # 1) Export Zenodo (writes zenodo.ttl)
        zenodo_ttl = os.path.join(run_dir, OUTPUT)
        run_cmd(
            ["python", "-m", "scripts.zenodo.export_zenodo", "--make-snapshots", "--out", zenodo_ttl],
            cwd=run_dir,
            env=env 
        )
        if not os.path.exists(zenodo_ttl) or os.path.getsize(zenodo_ttl) == 0:
            raise AirflowFailException(f"{OUTPUT} missing/empty: {zenodo_ttl}")

        # 2) Harvest based on asserted TTL
        out_csv = os.path.join(run_dir, "datasets_urls.csv")
        out_harvested = os.path.join(run_dir, "harvested")

        run_cmd(
            ["python", os.path.join(scripts_dir, "fetch_zenodo.py"), "--data", merge_ttl, "--out-csv", out_csv, "--out-dir", out_harvested],
            cwd=run_dir,
            env=env,
        )

        print("Harvester done. Output:", run_dir)

    @task()
    def mark_success(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")
        Variable.set(LAST_SUCCESSFUL_HARVESTER_ZENODO_RUN_VARIABLE_NAME, run_dir)
        print(f"Set {LAST_SUCCESSFUL_HARVESTER_ZENODO_RUN_VARIABLE_NAME}={run_dir}")

    init = init_data_dir()
    scripts = fetchscripts()
    harvest = run_harvester()
    done = mark_success()

    init >> scripts >> harvest >> done


harvester_zenodo()
