"""
Reasoning DAG built around the Konclude reasoner via the Koncludix wrapper.
"""
from __future__ import annotations

import os
import sys

import requests

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator

# Make the `common.koncludix` module importable when the DAG runs inside Airflow.
_DAG_DIR = os.path.dirname(__file__)
if _DAG_DIR not in sys.path:
    sys.path.append(_DAG_DIR)


DAG_ID = "reason_koncludix"
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
        in_expanded = f"{artifact}-expanded.ttl"
        out_ttl = f"{artifact}_inferences.ttl"

        ti.xcom_push(key="datadir", value=run_dir)
        ti.xcom_push(key="artifact", value=artifact)
        ti.xcom_push(key="in_ttl", value=in_ttl)
        ti.xcom_push(key="source_run_dir", value=source_run_dir or "")
        ti.xcom_push(key="in_filtered", value=in_filtered)
        ti.xcom_push(key="in_expanded", value=in_expanded)
        ti.xcom_push(key="out_ttl", value=out_ttl)

    @task
    def retrieve_nfdicore_extension(ti=None):
        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        print("Working in datadir ", data_dir)
        out_path = os.path.join(data_dir, "nfdicore-extension.owl")
        nfdicore_ext_url = Variable.get("nfdicore_extension")
        r = requests.get(nfdicore_ext_url, timeout=60)
        r.raise_for_status()
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(r.text)
        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise RuntimeError(f"nfdicore-extension.owl not written: {out_path}")

    # ---------------------------------------------------------------------
    # ROBOT pre-filter (unchanged from the previous DAG)
    # ---------------------------------------------------------------------
    def preFilterCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        source_run_dir = (
            '{{ (ti.xcom_pull(task_ids="init_data_dir", key="source_run_dir") or var.value.'
            + LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME + ') }}'
        )
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

    # ---------------------------------------------------------------------
    # ROBOT merge + expand (unchanged): produces the input TTL for Konclude
    # ---------------------------------------------------------------------
    def mergeExpandCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        in_filtered = '{{ ti.xcom_pull(task_ids="init_data_dir", key="in_filtered") }}'
        in_expanded = '{{ ti.xcom_pull(task_ids="init_data_dir", key="in_expanded") }}'

        in_path = os.path.join(DATA_DIR, in_filtered)
        ext_path = os.path.join(DATA_DIR, "nfdicore-extension.owl")
        out_path = os.path.join(DATA_DIR, in_expanded)

        cmd = (
            f"{ROBOT} merge"
            f" --input '{in_path}'"
            f" --input '{ext_path}'"
            f" expand"
            f" --annotate-expansion-axioms true"
            f" --output '{out_path}'"
        )
        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    pre_filter = BashOperator(
        task_id="pre_filter",
        bash_command=preFilterCmdTemplate(),
    )
    merge_expand = BashOperator(
        task_id="merge_expand",
        bash_command=mergeExpandCmdTemplate(),
    )

    @task
    def reasoning(ti=None):
        """
        Run Konclude on the merged/expanded asserted graph via the Koncludix
        wrapper. The wrapper drives Konclude through a small SPARQL job set
        (classes, object/datatype properties, sub-property hierarchies, class
        assertions) and recombines the XML results into a single inferences
        Turtle file.
        """
        from common.koncludix import koncludix

        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        in_expanded = ti.xcom_pull(task_ids="init_data_dir", key="in_expanded")
        out_ttl = ti.xcom_pull(task_ids="init_data_dir", key="out_ttl")

        BINARY = Variable.get("koncludebin")
        print("Working in datadir ", data_dir)

        input_file = os.path.join(data_dir, in_expanded)
        output_file = os.path.join(data_dir, out_ttl)
        tmpfolder = os.path.join(data_dir, "koncludix")
        os.makedirs(tmpfolder, exist_ok=True)

        # NOTE: signature is koncludix(binary, input_file, output_file, work_dir)
        koncludix(BINARY, input_file, output_file, tmpfolder)

        if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
            raise AirflowFailException(f"Koncludix produced no output at {output_file}")

    @task
    def mark_reason_success(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        artifact = ti.xcom_pull(task_ids="init_data_dir", key="artifact")
        out_ttl = ti.xcom_pull(task_ids="init_data_dir", key="out_ttl")
        out_ttl_path = os.path.join(run_dir, out_ttl)

        if not os.path.exists(out_ttl_path) or os.path.getsize(out_ttl_path) == 0:
            raise AirflowFailException(f"Inferences TTL missing/empty: {out_ttl_path}")

        with open(out_ttl_path, "rb") as f:
            head = f.read(64).lstrip()
        if head.startswith(b"<?xml") or head.startswith(b"<rdf:RDF"):
            raise AirflowFailException(f"Output {out_ttl_path} looks like RDF/XML, expected Turtle")

        if artifact == DEFAULT_ARTIFACT:
            Variable.set(LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME, run_dir)
            print(f"Set {LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME}={run_dir}")
        Variable.set(f"{LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME}__{artifact}", run_dir)
        print(f"Set {LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME}__{artifact}={run_dir}")

    init = init_data_dir()
    retrieve_ext = retrieve_nfdicore_extension()
    reason_task = reasoning()
    done = mark_reason_success()

    init >> pre_filter
    init >> retrieve_ext
    [pre_filter, retrieve_ext] >> merge_expand >> reason_task >> done


reason()
