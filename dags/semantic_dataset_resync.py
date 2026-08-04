"""Full re-sync of the registered RDF datasets (GitHub releases / Zenodo records).

``harvester_rdf_files`` runs weekly and is *incremental*: a file whose content hash
is unchanged is left alone, so an unchanged release costs nothing. That is the right
default, but it means a graph is never repaired if something happened to it after
publication — a partial Virtuoso load, a manual DROP, or a change to the metadata
this pipeline writes that should be re-applied to graphs harvested earlier.

This DAG is the periodic repair pass: monthly it re-harvests **every** registration
from scratch, ignoring the change-detection state, and republishes each named graph.
It shares all of its logic with the weekly harvester — same registration tab, same
artifact selection, same graph IRIs — so a re-sync can only reproduce or repair what
the weekly run would have written, never diverge from it.

Airflow Variables (identical to harvester_rdf_files):
  matwerk_rdf_files_gid         — gid of the registration tab  [required]
  matwerk_rdf_files_publish_id  — publish id, if that tab is in another spreadsheet
  matwerk_github_token          — recommended (GitHub allows 60 requests/hour without one)
  matwerk_zenodo_token          — only needed for restricted records
  matwerk_mwo_version           — ontology version for the classes written (default 3.0.1)
"""

from __future__ import annotations

import os
import sys

print('getcwd:      ', os.getcwd())
print('__file__:    ', __file__)
local_path = os.path.dirname(__file__)
print('adding local path', local_path)
sys.path.append(local_path)

import json
from datetime import datetime

from airflow.exceptions import AirflowFailException
from airflow.sdk import Variable, dag, get_current_context, task

from rdf_harvester import run as rdf_run

DAG_ID = "semantic_dataset_resync"

LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME = "matwerk_last_successful_merge_run"
# Deliberately the SAME variable the weekly harvester sets: publish_to_virtuoso reads
# it, so a re-sync republishes through exactly the same path as a normal run.
LAST_SUCCESSFUL_RUN_VARIABLE_NAME = "matwerk_last_successful_harvester_rdf_files_run"

MERGE_TTL = "spreadsheets_asserted.ttl"
OUTPUT = "rdf_files.ttl"
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
    schedule="@monthly",
    catchup=False,
    tags=["matwerk"],
)
def semantic_dataset_resync():

    @task()
    def init_data_dir(ti=None):
        ctx = get_current_context()
        sharedfs = Variable.get("matwerk_sharedfs")
        run_dir = os.path.join(sharedfs, "runs", ctx["dag"].dag_id, ctx["dag_run"].run_id)
        os.makedirs(run_dir, exist_ok=True)
        ti.xcom_push(key="run_dir", value=run_dir)

    @task()
    def resync_all(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")

        gid = _opt_var("matwerk_rdf_files_gid", "")
        if not gid:
            raise AirflowFailException(
                "matwerk_rdf_files_gid is not set — create the registration tab, "
                "publish it to the web as TSV, and set the Variable to its gid."
            )

        try:
            kg_ttl = os.path.join(Variable.get(LAST_SUCCESSFUL_MERGE_RUN_VARIABLE_NAME), MERGE_TTL)
            if not os.path.exists(kg_ttl):
                kg_ttl = None
        except Exception:
            kg_ttl = None

        # A run-local state file (rather than the shared one the weekly DAG keeps) is
        # what makes this a FULL re-sync: with no prior state every file counts as new
        # and every named graph is rewritten.
        summary = rdf_run.run(
            out_dir=run_dir,
            publish_id=_opt_var("matwerk_rdf_files_publish_id", DEFAULT_PUBLISH_ID),
            gid=gid,
            kg_ttl=kg_ttl,
            github_token=_opt_var("matwerk_github_token", "") or None,
            zenodo_token=_opt_var("matwerk_zenodo_token", "") or None,
            state_path=os.path.join(run_dir, "resync_state.json"),
            out_name=OUTPUT,
            mwo_version=_opt_var("matwerk_mwo_version", "") or None,
        )
        print("Re-sync done:", json.dumps({k: v for k, v in summary.items() if k != "results"}))

        # Keep the shared incremental state in step with what was just published, so
        # the next weekly run does not re-report every graph as "changed".
        src = os.path.join(run_dir, "resync_state.json")
        if os.path.exists(src):
            state_dir = os.path.join(Variable.get("matwerk_sharedfs"), "state", "harvester_rdf_files")
            os.makedirs(state_dir, exist_ok=True)
            with open(src, encoding="utf-8") as fh:
                state = fh.read()
            with open(os.path.join(state_dir, "rdf_files_state.json"), "w", encoding="utf-8") as fh:
                fh.write(state)

    @task()
    def mark_success(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="run_dir")
        Variable.set(LAST_SUCCESSFUL_RUN_VARIABLE_NAME, run_dir)
        print(f"Set {LAST_SUCCESSFUL_RUN_VARIABLE_NAME}={run_dir}")

    init_data_dir() >> resync_all() >> mark_success()


semantic_dataset_resync()
