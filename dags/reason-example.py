from __future__ import annotations

import os

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator


DAG_ID = "reason-example"
LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "last_sucessfull_merge_run"
LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME = "last_sucessfull_reason_run"

OUT_TTL = "spreadsheets_asserted.ttl"
OUT_REASONED_TTL = "spreadsheets_inferences.owl"

@dag(
    schedule=None,
    catchup=False,
    dag_id=DAG_ID,
)
def reason():

    @task
    def init_data_dir(ti=None):
        ctx = get_current_context()
        run_id = ctx["dag_run"].run_id

        sharedfs = Variable.get("sharedfs")

        run_dir = os.path.join(sharedfs, "runs", ctx["dag"].dag_id, run_id)
        os.makedirs(run_dir, exist_ok=True)
        ti.xcom_push(key="datadir", value=run_dir)

    def reasonCmdTemplate() -> str:
        REASONER = "{{ var.value.lletinatorcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        source_run_dir = Variable.get(LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME)

        in_ttl = os.path.join(source_run_dir, OUT_TTL)
        out_ttl = os.path.join(DATA_DIR, OUT_REASONED_TTL)

        cmd = (f"{REASONER} --input '{in_ttl}' --output {in_ttl}")

        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    reason = BashOperator(
        task_id="robot_reason",
        bash_command=reasonCmdTemplate()
    )

    init_data_dir() >> reason 

reason()
