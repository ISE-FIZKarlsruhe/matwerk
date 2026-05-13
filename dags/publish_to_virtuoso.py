from __future__ import annotations
import os, sys

print('getcwd:      ', os.getcwd())
print('__file__:    ', __file__)
local_path = os.path.dirname(__file__)
print('adding local path', local_path)
sys.path.append(local_path)


import json
from datetime import datetime
from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from common.virtuoso import upload_ttl_file, publish_trig_per_graph
from common.graph_metadata import (
    GraphPublishFacts,
    build_metadata_ttl,
    utc_now_iso_seconds,
    compute_rdf_stats,
)
import socket



DAG_ID = "publish_to_virtuoso"
GRAPH_ROOT = "https://nfdi.fiz-karlsruhe.de/matwerk"

# Sources published as a single Virtuoso graph (one TTL -> one graph IRI).
PUBLISH_SOURCES = [
    ("spreadsheets_assertions", "matwerk_last_successful_merge_run", "spreadsheets_asserted.ttl"),
    ("spreadsheets_inferences", "matwerk_last_successful_reason_run", "spreadsheets_inferences.ttl"),
    ("spreadsheets_validated", "matwerk_last_successful_validated_run", "spreadsheets_merged_for_validation.ttl"),
    ("endpoints_validated", "matwerk_last_successful_harvester_endpoints_run", "endpoints_merged_for_validation.ttl"),
    #("pmd_validated", "matwerk_last_successful_harvester_pmd_run", "pmd_merged_for_validation.ttl"),
    # zenodo_validated removed: zenodo is published per-named-graph below so
    # an inconsistent record is isolated in its own Virtuoso graph instead of
    # being merged into a single graph.
]

# Sources published per-named-graph from a TriG file. Each named graph in the
# TriG becomes its own Virtuoso graph (graph IRI = named-graph IRI). The TriG's
# default graph (carrying the harvester's BFO/MWO provenance about each named
# graph) is uploaded to `default_graph_iri` so it remains queryable.
#
# Zenodo is published straight from the harvester's `zenodo.ttl` (no reasoning
# is run for zenodo at the moment — see the disabled triggers in
# `dags/harvester_zenodo.py` and the zenodo branch in `dags/reason_openlletnew.py`).
PUBLISH_TRIG_SOURCES = [
    {
        "stage": "zenodo_per_graph",
        "variable": "matwerk_last_successful_harvester_zenodo_run",
        "trig_name": "zenodo.ttl",
        "default_graph_iri": f"{GRAPH_ROOT}/zenodo_metadata",
    },
]


@dag(
    schedule=None,
    catchup=False,
    dag_id=DAG_ID,
    tags=["matwerk"],
)
def publish_to_virtuoso():

    @task
    def init_publish_dir(ti=None):
        ctx = get_current_context()
        sharedfs = Variable.get("matwerk_sharedfs")
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

            ttl_path = os.path.join(source_dir, ttl_name)

            entry["ttl_path"] = ttl_path
            entry["graph"] = f"{GRAPH_ROOT}/{stage}"

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

                data_graph = f"{GRAPH_ROOT}/{stage}"
                
                started_iso = utc_now_iso_seconds()
                # Upload DATA graph (clears data graph)
                upload_ttl_file(graph=data_graph, ttl_path=ttl_path, delete_first=True)
                ended_iso = utc_now_iso_seconds()

                # Collect “more data” from Airflow context
                ctx = get_current_context()
                task = ctx.get("task")
                ti = ctx.get("ti")

                operator = task.__class__.__name__ if task else None
                log_url = getattr(ti, "log_url", None)
                hostname = socket.gethostname()

                facts = GraphPublishFacts(
                    graph_root=data_graph,
                    stage=stage,
                    dag_id=DAG_ID,
                    run_id=ctx["dag_run"].run_id,
                    data_graph_uri=data_graph,
                    ttl_path=ttl_path,
                    started_at=started_iso,
                    ended_at=ended_iso,
                    task_id=getattr(task, "task_id", None),
                    operator=operator,
                    log_url=log_url,
                    hostname=hostname,
                    stats=compute_rdf_stats(ttl_path),
                )

                meta_ttl = build_metadata_ttl(facts)
                meta_path = os.path.join(run_dir, f"{stage}__metadata.ttl")
                with open(meta_path, "w", encoding="utf-8") as f:
                    f.write(meta_ttl)

                # Upload METADATA graph (separate graph; clear then load)
                upload_ttl_file(graph=data_graph, ttl_path=meta_path, delete_first=False)

                entry["status"] = "published"
            except Exception as e:
                entry["status"] = "failed"
                entry["error"] = repr(e)
                failures.append(f"{stage}:upload_failed")

            report["results"].append(entry)

        # ----- Per-graph TriG sources (zenodo) -----
        for src in PUBLISH_TRIG_SOURCES:
            stage = src["stage"]
            var_name = src["variable"]
            trig_name = src["trig_name"]
            default_graph_iri = src.get("default_graph_iri")

            entry = {
                "stage": stage,
                "variable": var_name,
                "ttl_name": trig_name,
                "default_graph_iri": default_graph_iri,
                "status": "skipped",
                "reason": None,
                "source_dir": None,
                "ttl_path": None,
                "mode": "trig_per_graph",
                "graphs": [],
                "error": None,
            }

            try:
                source_dir = Variable.get(var_name)
            except Exception as e:
                entry["reason"] = f"variable not found: {e}"
                report["results"].append(entry)
                continue
            entry["source_dir"] = source_dir

            if not source_dir or not os.path.isdir(source_dir):
                entry["status"] = "failed"
                entry["error"] = f"source_dir missing/not a dir: {source_dir}"
                failures.append(f"{stage}:bad_source_dir")
                report["results"].append(entry)
                continue

            trig_path = os.path.join(source_dir, trig_name)
            entry["ttl_path"] = trig_path

            if not os.path.exists(trig_path) or os.path.getsize(trig_path) == 0:
                entry["status"] = "failed"
                entry["error"] = f"trig missing/empty: {trig_path}"
                failures.append(f"{stage}:missing_trig")
                report["results"].append(entry)
                continue

            try:
                print(f"[INFO] Per-graph publishing stage={stage}")
                print(f"[INFO] trig_path={trig_path}")
                if default_graph_iri:
                    print(f"[INFO] default_graph_iri={default_graph_iri}")

                workdir = os.path.join(run_dir, f"{stage}__per_graph")
                pg_report = publish_trig_per_graph(
                    trig_path,
                    workdir=workdir,
                    delete_first=True,
                    default_graph_iri=default_graph_iri,
                )
                entry["graphs"] = pg_report.get("graphs", [])

                pub = sum(1 for g in entry["graphs"] if g.get("status") == "published")
                fail = sum(1 for g in entry["graphs"] if g.get("status") == "failed")
                entry["published_count"] = pub
                entry["failed_count"] = fail

                if fail > 0:
                    entry["status"] = "partial"
                    failures.append(f"{stage}:{fail}_graph_uploads_failed")
                else:
                    entry["status"] = "published"
            except Exception as e:
                entry["status"] = "failed"
                entry["error"] = repr(e)
                failures.append(f"{stage}:trig_publish_failed")

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
