from __future__ import annotations

import json
import os
from datetime import datetime

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException

from common.virtuoso import upload_ttl_file


DAG_ID = "publish_to_virtuoso"
GRAPH_ROOT = "https://purls.helmholtz-metadaten.de/msekg"

# Sources to publish (edit freely)
PUBLISH_SOURCES = [
    ("merge", "last_sucessfull_merge_run", "spreadsheets_asserted.ttl"),
    ("reason", "last_sucessfull_reason_run", "spreadsheets_reasoned.ttl"),
    # later add harvesters
]



def build_graph_uri(graph_root: str, stage: str, run_id_safe: str) -> str:
    return f"{graph_root.rstrip('/')}/{stage}/{run_id_safe}"


@dag(
    schedule=None,
    catchup=False,
    dag_id=DAG_ID,
)
def publish_to_virtuoso():

    @task
    def init_publish_dir(ti=None):
        ctx = get_current_context()
        sharedfs = Variable.get("sharedfs")
        if not sharedfs or not os.path.isdir(sharedfs):
            raise AirflowFailException(f"sharedfs missing/not a dir: {sharedfs}")

        rid = ctx["dag_run"].run_id
        run_dir = os.path.join(sharedfs, "runs", ctx["dag"].dag_id, rid)
        os.makedirs(run_dir, exist_ok=True)
        ti.xcom_push(key="datadir", value=run_dir)

    @task
    def publish_all(ti=None):
        run_dir = ti.xcom_pull(task_ids="init_publish_dir", key="datadir")

        report = {
            "dag_id": DAG_ID,
            "timestamp_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "graph_root": GRAPH_ROOT,
            "results": [],
        }

        failures: list[str] = []

        for stage, var_name, ttl_name in PUBLISH_SOURCES:
            entry = {
                "stage": stage,
                "variable": var_name,
                "ttl_name": ttl_name,
                "status": "skipped",
                "reason": None,
                "source_dir": None,
                "ttl_path": None,
                "graph": None,
                "error": None,
            }

            # Variable may not exist; skip cleanly
            try:
                source_dir = Variable.get(var_name)
            except Exception as e:
                entry["status"] = "skipped"
                entry["reason"] = f"variable not found: {e}"
                report["results"].append(entry)
                continue

            entry["source_dir"] = source_dir

            # Validate source dir
            if not source_dir or not os.path.isdir(source_dir):
                entry["status"] = "failed"
                entry["error"] = f"source_dir missing/not a dir: {source_dir}"
                failures.append(f"{stage}:bad_source_dir")
                report["results"].append(entry)
                continue

            run_id_safe = os.path.basename(source_dir.rstrip("/"))
            ttl_path = os.path.join(source_dir, ttl_name)

            entry["ttl_path"] = ttl_path
            entry["graph"] = build_graph_uri(GRAPH_ROOT, stage, run_id_safe)

            if not os.path.exists(ttl_path) or os.path.getsize(ttl_path) == 0:
                entry["status"] = "failed"
                entry["error"] = f"ttl missing/empty: {ttl_path}"
                failures.append(f"{stage}:missing_ttl")
                report["results"].append(entry)
                continue

            try:
                print(f"[INFO] Publishing stage={stage}")
                print(f"[INFO] ttl_path={ttl_path}")
                print(f"[INFO] graph={entry['graph']}")

                upload_ttl_file(graph=entry["graph"], ttl_path=ttl_path, delete_first=True)

                entry["status"] = "published"
            except Exception as e:
                entry["status"] = "failed"
                entry["error"] = repr(e)
                failures.append(f"{stage}:upload_failed")

            report["results"].append(entry)

        json_path = os.path.join(run_dir, "publish_report.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, sort_keys=True)
        print(f"[INFO] Wrote report: {json_path}")

        if failures:
            raise AirflowFailException(
                "One or more publishes failed: " + ", ".join(failures)
            )

    init_publish_dir() >> publish_all()


publish_to_virtuoso()
