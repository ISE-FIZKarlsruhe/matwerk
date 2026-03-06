from __future__ import annotations

import os

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator


DAG_ID = "reason_openllet_new"
LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "matwerk_last_successful_merge_run"
LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME = "matwerk_last_successful_reason_run"

# Defaults remain spreadsheets
DEFAULT_ARTIFACT = "spreadsheets"
DEFAULT_IN_TTL = "spreadsheets_asserted.ttl"


@dag(
    schedule=None,
    catchup=False,
    dag_id=DAG_ID,
    tags=["matwerk"],
)
def reason():

    @task
    def init_data_dir(ti=None):
        ctx = get_current_context()
        run_id = ctx["dag_run"].run_id

        conf = (ctx["dag_run"].conf or {})
        artifact = conf.get("artifact", DEFAULT_ARTIFACT)
        in_ttl = conf.get("in_ttl", DEFAULT_IN_TTL)
        source_run_dir = conf.get("source_run_dir")
        target_run_dir = conf.get("target_run_dir")

        if target_run_dir:
            run_dir = target_run_dir
            os.makedirs(run_dir, exist_ok=True)
        else:
            sharedfs = Variable.get("matwerk_sharedfs")
            run_dir = os.path.join(sharedfs, "runs", ctx["dag"].dag_id, run_id)
            os.makedirs(run_dir, exist_ok=True)

        in_filtered = f"{artifact}-filtered.ttl"
        out_owl = f"{artifact}_inferences.owl"
        out_ttl = f"{artifact}_inferences.ttl"

        ti.xcom_push(key="datadir", value=run_dir)
        ti.xcom_push(key="artifact", value=artifact)
        ti.xcom_push(key="in_ttl", value=in_ttl)
        ti.xcom_push(key="source_run_dir", value=source_run_dir or "")
        ti.xcom_push(key="in_filtered", value=in_filtered)
        ti.xcom_push(key="out_owl", value=out_owl)
        ti.xcom_push(key="out_ttl", value=out_ttl)


    def openlletNewReasonCmdTemplate() -> str:
        REASONER = "{{ var.value.openlletnewcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        in_filtered = '{{ ti.xcom_pull(task_ids="init_data_dir", key="in_filtered") }}'
        out_owl = '{{ ti.xcom_pull(task_ids="init_data_dir", key="out_owl") }}'

        in_path = os.path.join(DATA_DIR, in_filtered)
        out_path = os.path.join(DATA_DIR, out_owl)

        cmd = f"{REASONER} extract -s \"PropertyAssertion SubPropertyOf InverseProperties SubClassOf ClassAssertion\" '{in_path}' > '{out_path}'"
        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    def robotConvertCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        out_owl = '{{ ti.xcom_pull(task_ids="init_data_dir", key="out_owl") }}'
        out_ttl = '{{ ti.xcom_pull(task_ids="init_data_dir", key="out_ttl") }}'

        in_owl = os.path.join(DATA_DIR, out_owl)
        out_ttl_path = os.path.join(DATA_DIR, out_ttl)

        cmd = f"{ROBOT} convert --input '{in_owl}' --output '{out_ttl_path}'"
        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    sunlet_reasoning = BashOperator(
        task_id="sunlet_reasoning",
        bash_command=openlletNewReasonCmdTemplate(),
    )

    robot_convert_to_ttl = BashOperator(
        task_id="robot_convert_to_ttl",
        bash_command=robotConvertCmdTemplate(),
    )

    @task
    def mark_reason_success(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        artifact = ti.xcom_pull(task_ids="init_data_dir", key="artifact")

        out_owl = ti.xcom_pull(task_ids="init_data_dir", key="out_owl")
        out_ttl = ti.xcom_pull(task_ids="init_data_dir", key="out_ttl")

        out_owl_path = os.path.join(run_dir, out_owl)
        out_ttl_path = os.path.join(run_dir, out_ttl)

        if not os.path.exists(out_owl_path) or os.path.getsize(out_owl_path) == 0:
            raise AirflowFailException(f"Reasoner output missing/empty: {out_owl_path}")

        if not os.path.exists(out_ttl_path) or os.path.getsize(out_ttl_path) == 0:
            raise AirflowFailException(f"Converted TTL missing/empty: {out_ttl_path}")

        with open(out_ttl_path, "rb") as f:
            head = f.read(64).lstrip()
        if head.startswith(b"<?xml") or head.startswith(b"<rdf:RDF"):
            raise AirflowFailException(f"Output {out_ttl_path} still looks like RDF/XML, expected Turtle")

        if artifact == DEFAULT_ARTIFACT:
            Variable.set(LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME, run_dir)
            print(f"Set {LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME}={run_dir}")

        Variable.set(f"{LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME}__{artifact}", run_dir)
        print(f"Set {LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME}__{artifact}={run_dir}")


    def preFilterCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        source_run_dir = '{{ (ti.xcom_pull(task_ids="init_data_dir", key="source_run_dir") or var.value.' + LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME + ') }}'

        in_ttl = '{{ ti.xcom_pull(task_ids="init_data_dir", key="in_ttl") }}'
        in_filtered = '{{ ti.xcom_pull(task_ids="init_data_dir", key="in_filtered") }}'

        in_path = f"{source_run_dir}/{in_ttl}"
        filtered = os.path.join(DATA_DIR, in_filtered)

        cmd = (
            f"{ROBOT} remove --input '{in_path}' "
            f"--term http://purl.obolibrary.org/obo/RO_0000057 --axioms SubPropertyChainOf "
            f"remove --term http://purl.obolibrary.org/obo/BFO_0000118 "
            f"--term http://purl.obolibrary.org/obo/BFO_0000181 "
            f"--term http://purl.obolibrary.org/obo/BFO_0000138 "
            f"--term http://purl.obolibrary.org/obo/BFO_0000136 "
            f"--output '{filtered}'"
        )
        return cmd.replace(DATA_DIR, XCOM_DATADIR)


    pre_filter = BashOperator(
        task_id="pre_filter",
        bash_command=preFilterCmdTemplate(),
    )

    init_data_dir() >> pre_filter >> sunlet_reasoning >> robot_convert_to_ttl >> mark_reason_success()


reason()
