from __future__ import annotations

import os

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator


DAG_ID = "reason-spreadsheets"
LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "last_sucessfull_merge_run"
LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME = "last_sucessfull_reason_run"

IN_FILE = "spreadsheets_asserted.ttl"
OUT_FILE = "spreadsheets_inferences.ttl"

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
        REASONER = "{{ var.value.sunletcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        source_run_dir = "{{ var.value." + LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME + " }}"

        in_path = os.path.join(source_run_dir, IN_FILE)
        out_path = os.path.join(DATA_DIR, OUT_FILE)

        # the reasoner will not merge the inferences with in original ontology, 
        # it just creates an rdf/owl file containing the inferred axioms.
        # (we later want to put the inferences in a separate named graph)

        cmd = (f"{REASONER} --input '{in_path}' --output {out_path}")

        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    reason = BashOperator(
        task_id="sunlet_reasoning",
        bash_command=reasonCmdTemplate()
    )

    @task
    def mark_reason_success(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        out_path = os.path.join(run_dir, OUT_FILE)

        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise AirflowFailException(f"Reasoned TTL missing/empty: {out_path}")

        Variable.set(LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME, run_dir)
        print(f"Set {LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME}={run_dir}")


    init_data_dir() >> reason >> mark_reason_success()

reason()
