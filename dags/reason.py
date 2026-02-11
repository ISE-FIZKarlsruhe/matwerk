from __future__ import annotations

import os

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator


DAG_ID = "reason"
LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "matwerk_last_sucessfull_merge_run"
LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME = "matwerk_last_sucessfull_reason_run"

OUT_TTL = "spreadsheets_asserted.ttl"
OUT_REASONED_TTL = "spreadsheets_reasoned.ttl"

# Default reasoning config requested:
DEFAULT_REASONER = "elk" #"hermit"
DEFAULT_AXIOM_GENERATORS = ["SubClass", "ClassAssertion"]
# DEFAULT_AXIOM_GENERATORS = ["SubClass", "EquivalentClass"]
# DEFAULT_AXIOM_GENERATORS = ["SubClass", "ClassAssertion", "PropertyAssertion"]
# DEFAULT_AXIOM_GENERATORS = [
#     "SubClass", "EquivalentClass", "DisjointClasses",
#     "ClassAssertion", "PropertyAssertion",
#     "ObjectPropertyDomain", "ObjectPropertyRange",
# ]

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

    def robotReasonCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        source_run_dir = Variable.get(LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME)

        in_ttl = os.path.join(source_run_dir, OUT_TTL)
        out_ttl = os.path.join(DATA_DIR, OUT_REASONED_TTL)

        reasoner = DEFAULT_REASONER
        axiom_generators = DEFAULT_AXIOM_GENERATORS
        include_indirect = "false"
        exclude_tautologies = "false"
        equivalent_classes_allowed = "all"
        annotate_inferred_axioms = "false"
        exclude_duplicate_axioms = "false"
        remove_redundant_subclass_axioms = "true"

        axiom_part = ""
        if axiom_generators:
            axiom_part = f' --axiom-generators "{" ".join(axiom_generators)}"'

        base_reason = (
            f"{ROBOT} reason"
            f" --input '{in_ttl}'"
            f" --reasoner {reasoner}"
            f" --include-indirect {include_indirect}"
            f" --exclude-tautologies {exclude_tautologies}"
            f" --equivalent-classes-allowed {equivalent_classes_allowed}"
            f" --annotate-inferred-axioms {annotate_inferred_axioms}"
            f" --exclude-duplicate-axioms {exclude_duplicate_axioms}"
            f" --remove-redundant-subclass-axioms {remove_redundant_subclass_axioms}"
            f"{axiom_part}"
            f" -vvv"
        )

        cmd = base_reason + f" --output '{out_ttl}'"

        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    @task
    def mark_reason_success(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        out_path = os.path.join(run_dir, OUT_REASONED_TTL)

        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise AirflowFailException(f"Reasoned TTL missing/empty: {out_path}")

        Variable.set(LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME, run_dir)
        print(f"Set {LAST_SUCCESSFUL_REASON_RUN_VARIABLE_NAME}={run_dir}")

    init = init_data_dir()

    robot_reason = BashOperator(
        task_id="robot_reason",
        bash_command=robotReasonCmdTemplate(),
    )

    done = mark_reason_success()

    init >> robot_reason >> done


reason()
