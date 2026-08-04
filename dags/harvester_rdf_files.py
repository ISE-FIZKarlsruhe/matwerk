from __future__ import annotations
import os, sys

print('getcwd:      ', os.getcwd())
print('__file__:    ', __file__)
local_path = os.path.dirname(__file__)
print('adding local path', local_path)
sys.path.append(local_path)

import json
from datetime import datetime
from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from rdf_harvester import run as rdf_run

DAG_ID = "harvester_rdf_files"

LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "matwerk_last_successful_merge_run"
LAST_SUCCESSFUL_RUN_VARIABLE_NAME = "matwerk_last_successful_harvester_rdf_files_run"

MERGE_TTL = "spreadsheets_asserted.ttl"   # merged KG: attach graphs to known entities
OUTPUT = "rdf_files.ttl"                  # per-named-graph TriG published to Virtuoso

# The registration tab of the Google Sheet, published to the web as TSV — the
# same mechanism dags/spreadsheets.py uses. A curator registers a GitHub repo or
# a Zenodo record (optionally naming the files); everything else is automatic.
#   matwerk_rdf_files_gid        — gid of the registration tab  [required]
#   matwerk_rdf_files_publish_id — publish id of the sheet holding that tab
#                                  (defaults to the sheet process_spreadsheets reads)
DEFAULT_PUBLISH_ID = (
    "2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE"
)


def _opt_var(name: str, default=None):
    try:
        return Variable.get(name)
    except Exception:
        return default


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["matwerk"],
)
def harvester_rdf_files():

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

        gid = _opt_var("matwerk_rdf_files_gid", "")
        if not gid:
            raise AirflowFailException(
                "matwerk_rdf_files_gid is not set — create the registration tab, "
                "publish it to the web as TSV, and set the Variable to its gid."
            )
        publish_id = _opt_var("matwerk_rdf_files_publish_id", DEFAULT_PUBLISH_ID)

        # optional: attach harvested graphs to entities the KG already knows
        try:
            kg_ttl = os.path.join(Variable.get(LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME), MERGE_TTL)
            if not os.path.exists(kg_ttl):
                kg_ttl = None
        except Exception:
            kg_ttl = None

        # change-detection state persists across runs (outside the per-run dir)
        state_dir = os.path.join(Variable.get("matwerk_sharedfs"), "state", DAG_ID)
        os.makedirs(state_dir, exist_ok=True)

        summary = rdf_run.run(
            out_dir=run_dir,
            publish_id=publish_id,
            gid=gid,
            kg_ttl=kg_ttl,
            github_token=_opt_var("matwerk_github_token", "") or None,
            zenodo_token=_opt_var("matwerk_zenodo_token", "") or None,
            state_path=os.path.join(state_dir, "rdf_files_state.json"),
            out_name=OUTPUT,
            # matwerk_mwo_version selects the vocabulary written (see common/mwo_terms.py).
            # MWO 3.0.2 deprecates the dataset/publication/software classes 3.0.1 uses,
            # so the version has to be explicit rather than hard-coded in each DAG.
            mwo_version=_opt_var("matwerk_mwo_version", "") or None,
        )
        print("Harvester done:", json.dumps({k: v for k, v in summary.items() if k != "results"}))

    @task()
    def mark_success(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")
        Variable.set(LAST_SUCCESSFUL_RUN_VARIABLE_NAME, run_dir)
        print(f"Set {LAST_SUCCESSFUL_RUN_VARIABLE_NAME}={run_dir}")

    init_data_dir() >> run_harvester() >> mark_success()


harvester_rdf_files()
