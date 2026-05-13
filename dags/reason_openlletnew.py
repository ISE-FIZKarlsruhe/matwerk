from __future__ import annotations

import os
import requests

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator

# Zenodo per-graph reasoning is disabled for now.
# import sys
# from airflow.providers.standard.operators.python import BranchPythonOperator
# _DAG_DIR = os.path.dirname(__file__)
# if _DAG_DIR not in sys.path:
#     sys.path.append(_DAG_DIR)


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

        in_expanded = f"{artifact}-expanded.ttl"
        ti.xcom_push(key="in_expanded", value=in_expanded)

    # def _branch_by_artifact(**kwargs):
    #     """
    #     Route zenodo artifacts through the per-graph reasoner so an inconsistent
    #     named graph cannot poison the whole reasoning step. Other artifacts use
    #     the existing flat ROBOT+Openllet chain unchanged.
    #     """
    #     ti = kwargs["ti"]
    #     artifact = ti.xcom_pull(task_ids="init_data_dir", key="artifact")
    #     if artifact == "zenodo":
    #         return "reason_zenodo_per_graph"
    #     return "pre_filter"

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

    def openlletNewReasonCmdTemplate() -> str:
        REASONER = "{{ var.value.openlletnewcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        in_expanded = '{{ ti.xcom_pull(task_ids="init_data_dir", key="in_expanded") }}'
        out_owl = '{{ ti.xcom_pull(task_ids="init_data_dir", key="out_owl") }}'

        in_path = os.path.join(DATA_DIR, in_expanded)
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

    merge_expand = BashOperator(
        task_id="merge_expand",
        bash_command=mergeExpandCmdTemplate(),
    )

    sunlet_reasoning = BashOperator(
        task_id="sunlet_reasoning",
        bash_command=openlletNewReasonCmdTemplate(),
    )

    robot_convert_to_ttl = BashOperator(
        task_id="robot_convert_to_ttl",
        bash_command=robotConvertCmdTemplate(),
    )

    # ---------------------------------------------------------------------
    # Zenodo per-graph reasoning is disabled for now.
    # ---------------------------------------------------------------------
    # @task
    # def reason_zenodo_per_graph(ti=None):
    #     """
    #     Per-named-graph reasoning for zenodo TriG inputs. Splits the input into
    #     one TTL per named graph, runs ROBOT pre_filter+merge+expand and then
    #     Openllet consistency+extract on each, and recombines. An inconsistent
    #     named graph is kept (no inferences emitted for it) and reported in the
    #     validation manifest + as rdfs:comment "INCONSISTENT" in the per-graph
    #     TriG output.
    #     """
    #     from common.per_graph_reasoner import ReasonConfig, reason_artifact
    #
    #     run_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
    #     artifact = ti.xcom_pull(task_ids="init_data_dir", key="artifact")
    #     in_ttl_name = ti.xcom_pull(task_ids="init_data_dir", key="in_ttl")
    #     source_run_dir = ti.xcom_pull(task_ids="init_data_dir", key="source_run_dir") or ""
    #
    #     if not source_run_dir:
    #         source_run_dir = Variable.get(LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME)
    #
    #     in_path = os.path.join(source_run_dir, in_ttl_name)
    #     ext_path = os.path.join(run_dir, "nfdicore-extension.owl")
    #
    #     cfg = ReasonConfig(
    #         in_path=in_path,
    #         artifact=artifact,
    #         extension_owl_path=ext_path,
    #         out_dir=run_dir,
    #         robot_cmd=Variable.get("robotcmd"),
    #         openllet_cmd=Variable.get("openlletnewcmd"),
    #         pre_filter_terms=[
    #             "http://purl.obolibrary.org/obo/RO_0000057",
    #             "http://purl.obolibrary.org/obo/BFO_0000118",
    #             "http://purl.obolibrary.org/obo/BFO_0000181",
    #             "http://purl.obolibrary.org/obo/BFO_0000138",
    #             "http://purl.obolibrary.org/obo/BFO_0000136",
    #         ],
    #     )
    #     manifest = reason_artifact(cfg)
    #
    #     out_owl = ti.xcom_pull(task_ids="init_data_dir", key="out_owl")
    #     out_ttl = ti.xcom_pull(task_ids="init_data_dir", key="out_ttl")
    #     out_owl_path = os.path.join(run_dir, out_owl)
    #     out_ttl_path = os.path.join(run_dir, out_ttl)
    #
    #     import subprocess
    #     if os.path.exists(out_ttl_path) and os.path.getsize(out_ttl_path) > 0:
    #         cmd = [*Variable.get("robotcmd").split(), "convert",
    #                "--input", out_ttl_path, "--output", out_owl_path]
    #         print(f"[CMD] {' '.join(cmd)}")
    #         rc = subprocess.run(cmd).returncode
    #         if rc != 0:
    #             with open(out_owl_path, "w", encoding="utf-8") as f:
    #                 f.write("<?xml version=\"1.0\"?>\n<!-- robot convert failed; see validation_report.txt -->\n")
    #     else:
    #         with open(out_owl_path, "w", encoding="utf-8") as f:
    #             f.write("<?xml version=\"1.0\"?>\n<!-- no inferences produced; see validation_report.txt -->\n")
    #
    #     n_total = manifest.get("counts", {}).get("graphs_total", 0)
    #     n_bad = manifest.get("counts", {}).get("graphs_inconsistent", 0)
    #     print(f"[INFO] zenodo reasoning summary: {n_bad}/{n_total} inconsistent graphs")
    #     if n_total == 0:
    #         raise AirflowFailException("No named graphs found in zenodo input — wrong format?")

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

    init = init_data_dir()
    retrieve_ext = retrieve_nfdicore_extension()
    done = mark_reason_success()

    init >> pre_filter
    init >> retrieve_ext
    [pre_filter, retrieve_ext] >> merge_expand >> sunlet_reasoning >> robot_convert_to_ttl >> done

    # ---------------------------------------------------------------------
    # Zenodo per-graph reasoning wiring — disabled for now.
    # branch = BranchPythonOperator(
    #     task_id="branch_by_artifact",
    #     python_callable=_branch_by_artifact,
    # )
    # zenodo_reason = reason_zenodo_per_graph()
    # init >> branch
    # branch >> pre_filter
    # branch >> zenodo_reason
    # retrieve_ext >> zenodo_reason >> done
    # (also switch mark_reason_success back to
    #  @task(trigger_rule="none_failed_min_one_success") so the skipped branch
    #  doesn't block it.)
    # ---------------------------------------------------------------------


reason()
