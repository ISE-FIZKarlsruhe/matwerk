from __future__ import annotations

import os

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator


DAG_ID = "reason-spreadsheets"
LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "matwerk_last_successful_merge_run"
LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME = "matwerk_last_successful_reason_run"

IN_FILE = "spreadsheets_asserted.ttl"
IN_FILTERED = "spreadsheets_asserted-filtered.ttl"

OUT_OWL = "spreadsheets_inferences.owl"
OUT_TTL = "spreadsheets_inferences.ttl"



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

        sharedfs = Variable.get("matwerk_sharedfs")
        run_dir = os.path.join(sharedfs, "runs", ctx["dag"].dag_id, run_id)
        os.makedirs(run_dir, exist_ok=True)
        ti.xcom_push(key="datadir", value=run_dir)

    def sunletReasonCmdTemplate() -> str:
        REASONER = "{{ var.value.sunletcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        # read last merge run dir from Airflow variable
        #source_run_dir = "{{ var.value." + LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME + " }}"

        #in_path = os.path.join(source_run_dir, IN_FILE)
        in_path = os.path.join(DATA_DIR, IN_FILTERED)
        out_path = os.path.join(DATA_DIR, OUT_OWL)

        # sunlet emits OWL/RDF (often RDF/XML). Keep it as .owl.
        cmd = f"{REASONER} --input '{in_path}' --output '{out_path}'"
        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    def robotConvertCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        in_owl = os.path.join(DATA_DIR, OUT_OWL)
        out_ttl = os.path.join(DATA_DIR, OUT_TTL)

        # Convert whatever OWL syntax sunlet wrote into Turtle
        # ROBOT auto-detects input format from content/extension.
        cmd = f"{ROBOT} convert --input '{in_owl}' --output '{out_ttl}'"
        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    sunlet_reasoning = BashOperator(
        task_id="sunlet_reasoning",
        bash_command=sunletReasonCmdTemplate(),
    )

    robot_convert_to_ttl = BashOperator(
        task_id="robot_convert_to_ttl",
        bash_command=robotConvertCmdTemplate(),
    )

    @task
    def mark_reason_success(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")

        out_owl_path = os.path.join(run_dir, OUT_OWL)
        out_ttl_path = os.path.join(run_dir, OUT_TTL)

        if not os.path.exists(out_owl_path) or os.path.getsize(out_owl_path) == 0:
            raise AirflowFailException(f"Reasoner output missing/empty: {out_owl_path}")

        if not os.path.exists(out_ttl_path) or os.path.getsize(out_ttl_path) == 0:
            raise AirflowFailException(f"Converted TTL missing/empty: {out_ttl_path}")

        # Optional: cheap sanity check to catch RDF/XML accidentally written to .ttl
        with open(out_ttl_path, "rb") as f:
            head = f.read(64).lstrip()
        if head.startswith(b"<?xml") or head.startswith(b"<rdf:RDF"):
            raise AirflowFailException(f"Output {out_ttl_path} still looks like RDF/XML, expected Turtle")

        Variable.set(LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME, run_dir)
        print(f"Set {LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME}={run_dir}")

    def preFilterCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        source_run_dir = "{{ var.value." + LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME + " }}"

        in_owl = os.path.join(source_run_dir, IN_FILE)
        filtered = os.path.join(DATA_DIR, IN_FILTERED)

        # ROBOT auto-detects input format from content/extension.
        cmd = f"{ROBOT} remove --input '{in_owl}' --term http://purl.obolibrary.org/obo/RO_0000057 --axioms SubPropertyChainOf  remove --term http://purl.obolibrary.org/obo/BFO_0000118 --term http://purl.obolibrary.org/obo/BFO_0000181 --term http://purl.obolibrary.org/obo/BFO_0000138 --term http://purl.obolibrary.org/obo/BFO_0000136 --output '{filtered}'"
        return cmd.replace(DATA_DIR, XCOM_DATADIR)


    pre_filter = BashOperator(
        task_id="pre_filter",
        bash_command=preFilterCmdTemplate(),
    )

    init_data_dir() >> pre_filter >> sunlet_reasoning >> robot_convert_to_ttl >> mark_reason_success()


reason()