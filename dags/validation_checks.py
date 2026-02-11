from __future__ import annotations

import os
import shutil
from glob import glob

import requests

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

DAG_ID = "validation_checks"

MERGED_FOR_VALIDATION_TTL = "spreadsheets_merged_for_validation.ttl"

LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "matwerk_last_sucessfull_merge_run"
LAST_SUCCESSFUL_VALIDATED_RUN_VARIABLE_NAME = "matwerk_last_sucessfull_validated_run"
LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME = "matwerk_last_sucessfull_reason_run"
# Inputs produced by other DAGs
ASSERTED_TTL = "spreadsheets_asserted.ttl" 
INFERENCES_FILE = "spreadsheets_inferences.ttl"

@dag(
    schedule=None,
    catchup=False,
    dag_id=DAG_ID,
)
def validation_checks():

    @task
    def init_data_dir(ti=None):
        ctx = get_current_context()
        run_id = ctx["dag_run"].run_id

        data_dir = os.path.join(Variable.get("matwerk_sharedfs"), "runs", ctx["dag"].dag_id, run_id,)
        os.makedirs(data_dir, exist_ok=True)

        # Where we keep downloaded shapes and validation outputs
        os.makedirs(os.path.join(data_dir, "_shapes"), exist_ok=True)
        os.makedirs(os.path.join(data_dir, "validation", "robot_verify"), exist_ok=True)
        os.makedirs(os.path.join(data_dir, "validation", "shacl"), exist_ok=True)

        ti.xcom_push(key="datadir", value=data_dir)
        print("Validation run dir:", data_dir)

    @task
    def pull_merge_reason_output(ti=None):

        # pull merge output
        src_dir = Variable.get(LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME)
        src_ttl = os.path.join(src_dir, ASSERTED_TTL)

        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        dst_ttl = os.path.join(data_dir, ASSERTED_TTL)
        shutil.copyfile(src_ttl, dst_ttl)

        # Keep track of original merge run id (useful later)
        source_run_id = os.path.basename(src_dir.rstrip("/"))
        ti.xcom_push(key="source_merge_dir", value=src_dir)
        ti.xcom_push(key="source_merge_run_id", value=source_run_id)

        print("Copied merge TTL:", src_ttl, "->", dst_ttl)
        print("Source merge run_id:", source_run_id)

        # pull reason output
        src_dir = Variable.get(LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME)
        src_inf = os.path.join(src_dir, INFERENCES_FILE)

        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        dst_inf = os.path.join(data_dir, INFERENCES_FILE)
        shutil.copyfile(src_inf, dst_inf)

        source_run_id = os.path.basename(src_dir.rstrip("/"))
        ti.xcom_push(key="source_reason_dir", value=src_dir)
        ti.xcom_push(key="source_reason_run_id", value=source_run_id)

        print("Copied reason output:", src_inf, "->", dst_inf)
        print("Source reason run_id:", source_run_id)

    # -----------------------------
    # Fetch SHACL + SPARQL shapes
    # -----------------------------
    @task
    def fetch_shapes(ti=None):
        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        shapes_dir = os.path.join(data_dir, "_shapes")

        api = "https://api.github.com/repos/ISE-FIZKarlsruhe/matwerk/contents/shapes?ref=main"
        items = requests.get(api).json()

        for it in items:
            if it.get("type") != "file":
                continue
            name = it.get("name", "")
            dl = it.get("download_url", "")
            if not name or not dl:
                continue
            out = os.path.join(shapes_dir, name)
            r = requests.get(dl)
            r.raise_for_status()
            with open(out, "wb") as f:
                f.write(r.content)

        shape_files = sorted([os.path.basename(p) for p in glob(os.path.join(shapes_dir, "*.ttl"))])
        sparql_files = sorted([os.path.basename(p) for p in glob(os.path.join(shapes_dir, "*.sparql"))])

        print("Downloaded SHACL shapes:", shape_files)
        print("Downloaded SPARQL queries:", sparql_files)

        ti.xcom_push(key="shape_files", value=shape_files)
        ti.xcom_push(key="sparql_files", value=sparql_files)

    # -----------------------------
    # ROBOT merge asserted + inferences
    # -----------------------------
    def robotMergeForValidationCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        asserted = os.path.join(DATA_DIR, ASSERTED_TTL)
        inferences = os.path.join(DATA_DIR, INFERENCES_FILE)
        merged = os.path.join(DATA_DIR, MERGED_FOR_VALIDATION_TTL)

        cmd = (
            f"{ROBOT} merge --include-annotations true "
            f"--input '{asserted}' --input '{inferences}' "
            f"--output '{merged}'"
        )
        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    # -----------------------------
    # ROBOT verify (SPARQL) + save outputs
    # -----------------------------
    def robotVerifySparqlCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        merged_ttl = os.path.join(DATA_DIR, MERGED_FOR_VALIDATION_TTL)
        out_dir = os.path.join(DATA_DIR, "validation", "robot_verify")

        # One log per query file
        cmd = (
            f"mkdir -p '{out_dir}'\n"
            
            "{% for q in ti.xcom_pull(task_ids='fetch_shapes', key='sparql_files') %}\n"
            f"LOG='{os.path.join(out_dir, '{{ q }}')}.log'\n"
            
            f"{ROBOT} verify --input \"{merged_ttl}\" "
            f"--queries '{os.path.join(DATA_DIR, '_shapes', '{{ q }}')}' -vvv "
            f"> \"$LOG\" 2>&1 || (tail -n 120 \"$LOG\"; exit 1)\n"
            
            "{% endfor %}\n"
        )

        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    # -----------------------------
    # SHACL validate + save outputs
    # -----------------------------
    @task
    def shacl_validate(ti=None):
        from pyshacl import validate

        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        shape_files = ti.xcom_pull(task_ids="fetch_shapes", key="shape_files")

        merged_ttl = os.path.join(data_dir, MERGED_FOR_VALIDATION_TTL)
        out_dir = os.path.join(data_dir, "validation", "shacl")
        os.makedirs(out_dir, exist_ok=True)

        any_bad = False
        summary_lines: list[str] = []

        for shp_name in shape_files:
            shp = os.path.join(data_dir, "_shapes", shp_name)
            print("Running SHACL:", shp)

            conforms, _, results_text = validate(
                data_graph=merged_ttl,
                shacl_graph=shp,
                data_graph_format="turtle",
                shacl_graph_format="turtle",
            )

            out_path = os.path.join(out_dir, f"{shp_name}.txt")
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(results_text or "")

            summary_lines.append(f"{shp_name}: {'OK' if conforms else 'FAIL'}  (details: {out_path})")

            if not conforms:
                print(f"---- SHACL report for {shp_name} ----")
                print("\n".join((results_text or "").splitlines()))
                any_bad = True

        summary_path = os.path.join(out_dir, "summary.txt")
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("\n".join(summary_lines) + "\n")

        print("\n".join(summary_lines))
        print("SHACL summary written to:", summary_path)

        if any_bad:
            raise AirflowFailException("SHACL validation failed (see saved results + logs)")

    @task
    def mark_validated_successful(ti=None):
        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")

        Variable.set(LAST_SUCCESSFUL_VALIDATED_RUN_VARIABLE_NAME, data_dir)
        print(f"Set {LAST_SUCCESSFUL_VALIDATED_RUN_VARIABLE_NAME}={data_dir}")

    # -----------------------------
    # Wiring
    # -----------------------------
    init = init_data_dir()
    pull_asserted_inferences = pull_merge_reason_output()
    shapes = fetch_shapes()

    robot_merge_for_validation = BashOperator(task_id="robot_merge_for_validation", bash_command=robotMergeForValidationCmdTemplate(),)

    robot_verify = BashOperator(task_id="robot_verify_sparql", bash_command=robotVerifySparqlCmdTemplate(),)

    shacl = shacl_validate()
    done = mark_validated_successful()

    init >> [pull_asserted_inferences, shapes]
    pull_asserted_inferences >> robot_merge_for_validation
    
    robot_merge_for_validation >> [robot_verify, shacl]
    shapes >> [robot_verify, shacl]

    [robot_verify, shacl] >> done


validation_checks()
