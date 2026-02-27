# dags/harvester_pmd.py
from __future__ import annotations

import os
import sys
import requests
from datetime import datetime

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


print("getcwd:      ", os.getcwd())
print("__file__:    ", __file__)
local_path = os.path.dirname(__file__)
print("adding local path", local_path)
sys.path.append(local_path)

from pmd_harvester import harvest as pmd_harvest
from pmd_harvester import template_builder as pmd_templates


DAG_ID = "harvester_pmd"

LAST_SUCCESSFUL_HARVESTER_PMD_RUN_VARIABLE_NAME = "matwerk_last_successful_harvester_pmd_run"

# outputs within run_dir
HARVEST_DIRNAME = "pmd_harvest"
TEMPLATES_DIRNAME = "robot_templates"
MODULES_DIRNAME = "modules"
OUT_TTL = "pmd_asserted.ttl"


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["matwerk"],
)
def harvester_pmd():

    @task()
    def init_data_dir(ti=None):
        ctx = get_current_context()
        sharedfs = Variable.get("matwerk_sharedfs")
        rid = ctx["dag_run"].run_id
        run_dir = os.path.join(sharedfs, "runs", ctx["dag"].dag_id, rid)
        os.makedirs(run_dir, exist_ok=True)
        ti.xcom_push(key="run_dir", value=run_dir)
        print("Run dir:", run_dir)

    @task()
    def retrieve_ontology(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")
        out_path = os.path.join(run_dir, "ontology.owl")

        ontology_url = Variable.get("matwerk_ontology")
        r = requests.get(ontology_url, timeout=60)
        r.raise_for_status()

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(r.text)

        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise RuntimeError(f"ontology.owl not written: {out_path}")

        print("Wrote ontology:", out_path)

    @task()
    def run_harvest_and_build_templates(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")

        harvest_dir = os.path.join(run_dir, HARVEST_DIRNAME)
        templates_dir = os.path.join(run_dir, TEMPLATES_DIRNAME)

        os.makedirs(harvest_dir, exist_ok=True)
        os.makedirs(templates_dir, exist_ok=True)

        # 1) Harvest -> creates metadata.jsonl + full_metadata.csv in harvest_dir
        pmd_harvest.main([
            "--out-dir", harvest_dir,
            "--download-files",
        ])

        in_csv = os.path.join(harvest_dir, "full_metadata.csv")
        if not os.path.exists(in_csv) or os.path.getsize(in_csv) == 0:
            raise RuntimeError(f"Expected full_metadata.csv not found/written: {in_csv}")

        # 2) Build TSV templates
        pmd_templates.main([
            "--in-csv", in_csv,
            "--out-dir", templates_dir,
            # optionally: "--base-iri", "https://....", "--ns", "..."
        ])

        print("Templates built:", templates_dir)

    def isvalid(filename: str, ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")
        path = os.path.join(run_dir, filename)
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            s = f.read()
        if s.strip() != "No explanations found.":
            print(s)
            raise ValueError("Ontology inconsistent or explanation not empty")

    def robotTemplateCmd(inputs: list[str], template_name: str) -> str:
        """
        Creates modules/<template_name>.owl from robot_templates/<template_name>.tsv and runs robot explain.
        inputs: list of prior module basenames (without extension). Use ["ontology"] for base.
        """
        ROBOT = "{{ var.value.robotcmd }}"
        RUN_DIR = "RUN_DIR"

        XCOM_RUN_DIR = "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}"

        tmpl = os.path.join(RUN_DIR, TEMPLATES_DIRNAME, f"{template_name}.tsv")
        out_owl = os.path.join(RUN_DIR, MODULES_DIRNAME, f"{template_name}.owl")
        out_md = os.path.join(RUN_DIR, MODULES_DIRNAME, f"{template_name}.md")

        insert = ""
        for i in inputs:
            insert += f" -i '{os.path.join(RUN_DIR, MODULES_DIRNAME, i + '.owl')}'"

        cmd = (
            f"{ROBOT} merge --include-annotations true {insert} "
            f"template --merge-before --template '{tmpl}' "
            f"--output '{out_owl}' -vvv"
        )
        cmd += " && "
        cmd += (
            f"{ROBOT} explain --reasoner hermit --input '{out_owl}' "
            f"-M inconsistency --explanation '{out_md}'"
        )

        return cmd.replace(RUN_DIR, XCOM_RUN_DIR)

    def robotMergeCmd() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        RUN_DIR = "RUN_DIR"
        XCOM_RUN_DIR = "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}"

        out_ttl = os.path.join(RUN_DIR, OUT_TTL)
        cmd = (
            f"{ROBOT} merge --include-annotations true "
            f" --inputs '{os.path.join(RUN_DIR, MODULES_DIRNAME, '*.owl')}' "
            f" --output '{out_ttl}'"
        )
        return cmd.replace(RUN_DIR, XCOM_RUN_DIR)

    def robotMergedExplainCmd() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        RUN_DIR = "RUN_DIR"
        XCOM_RUN_DIR = "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}"

        out_ttl = os.path.join(RUN_DIR, OUT_TTL)
        explain_md = os.path.join(RUN_DIR, "pmd_merged_inconsistency.md")
        cmd = (
            f"{ROBOT} explain --reasoner hermit --input '{out_ttl}' "
            f"-M inconsistency --explanation '{explain_md}'"
        )
        return cmd.replace(RUN_DIR, XCOM_RUN_DIR)

    @task()
    def mark_success(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")
        ttl_path = os.path.join(run_dir, OUT_TTL)
        if not os.path.exists(ttl_path) or os.path.getsize(ttl_path) == 0:
            raise FileNotFoundError(f"Expected TTL not found: {ttl_path}")

        Variable.set(LAST_SUCCESSFUL_HARVESTER_PMD_RUN_VARIABLE_NAME, run_dir)
        print(f"Set {LAST_SUCCESSFUL_HARVESTER_PMD_RUN_VARIABLE_NAME}={run_dir}")

    # ---- graph ----
    init = init_data_dir()
    ont = retrieve_ontology()
    build = run_harvest_and_build_templates()

    mkdir_modules = BashOperator(
        task_id="mkdir_modules",
        bash_command="mkdir -p '{{ ti.xcom_pull(task_ids=\"init_data_dir\", key=\"run_dir\") }}/modules'",
    )

    wait = EmptyOperator(task_id="wait_for_inputs")

    init >> ont >> wait
    init >> build >> wait
    wait >> mkdir_modules

    copy_ontology = BashOperator(
        task_id="copy_ontology_into_modules",
        bash_command=(
            "cp '{{ ti.xcom_pull(task_ids=\"init_data_dir\", key=\"run_dir\") }}/ontology.owl' "
            "'{{ ti.xcom_pull(task_ids=\"init_data_dir\", key=\"run_dir\") }}/modules/ontology.owl'"
        ),
    )

    mkdir_modules >> copy_ontology

    module_order = [
        ("emails", ["ontology"]),
        ("websites", ["ontology"]),
        ("written_names", ["ontology"]),
        ("given_names", ["ontology"]),
        ("family_names", ["ontology"]),
        ("persons", ["ontology", "emails", "written_names", "given_names", "family_names"]),
        ("organizations", ["ontology"]),
        ("agent", ["ontology", "written_names"]),
        ("role", ["ontology", "agent", "emails", "websites", "written_names"]),
        ("process", ["ontology", "agent", "role"]),
        ("dates", ["ontology"]),
        ("license", ["ontology"]),
        ("titles", ["ontology"]),
        ("datasets", ["ontology", "titles", "license", "process", "persons", "organizations"]),
    ]

    prev = copy_ontology
    last_valid = None

    for name, inputs in module_order:
        robot = BashOperator(
            task_id=f"robot_{name}",
            bash_command=robotTemplateCmd(inputs, name),
        )
        valid = PythonOperator(
            task_id=f"robot_{name}_valid",
            python_callable=isvalid,
            op_kwargs={"filename": f"{name}.md"},
        )
        prev >> robot >> valid
        prev = valid
        last_valid = valid

    # 2) Merge all modules => asserted ttl + explain
    robot_merge = BashOperator(
        task_id="robot_merge_modules_to_ttl",
        bash_command=robotMergeCmd(),
    )

    robot_merge_explain = BashOperator(
        task_id="robot_merge_explain",
        bash_command=robotMergedExplainCmd(),
    )

    robot_merge_valid = PythonOperator(
        task_id="robot_merge_valid",
        python_callable=isvalid,
        op_kwargs={"filename": "pmd_merged_inconsistency.md"},
    )

    done = mark_success()

    last_valid >> robot_merge >> robot_merge_explain >> robot_merge_valid >> done

    # 3) Trigger reason + validation (same style as zenodo)
    trigger_reason_pmd = TriggerDagRunOperator(
        task_id="trigger_reason_pmd",
        trigger_dag_id="reason",
        wait_for_completion=True,
        conf={
            "artifact": "pmd",
            "source_run_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "target_run_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "in_ttl": OUT_TTL,
        },
    )

    trigger_validation_pmd = TriggerDagRunOperator(
        task_id="trigger_validation_pmd",
        trigger_dag_id="validation_checks",
        wait_for_completion=True,
        conf={
            "artifact": "pmd",
            "target_run_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "asserted_source_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "asserted_ttl": OUT_TTL,
            "reason_source_dir": "{{ ti.xcom_pull(task_ids='init_data_dir', key='run_dir') }}",
            "inferences_ttl": "pmd_inferences.ttl",
        },
    )

    done >> trigger_reason_pmd >> trigger_validation_pmd


harvester_pmd()