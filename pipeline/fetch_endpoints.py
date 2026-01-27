# pipeline/fetch_endpoints.py
from __future__ import annotations

import argparse
import os
from pathlib import Path

from pipeline.common import ensure_dir, run_cmd, assert_nonempty_file


def main() -> None:
    ap = argparse.ArgumentParser(description="Run scripts/fetch_endpoints.py with run-dir scoped env vars.")
    ap.add_argument("--in-ttl", required=True, help="Input all_NotReasoned.ttl path (in run dir).")
    ap.add_argument("--run-dir", required=True, help="Run directory root (e.g., /workspace/runs/<run_id>).")
    ap.add_argument("--script", default="scripts/fetch_endpoints.py", help="Script path.")
    ap.add_argument("--workdir", default="/app", help="Repo working directory inside the image.")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    in_ttl = Path(args.in_ttl)

    out_dir = run_dir / "data" / "sparql_endpoints"
    ensure_dir(out_dir)

    env = {
        # Make the script read/write strictly within this run
        "ALL_TTL": str(in_ttl),  # it updates this file (adds HAS_GRAPH_PRED triples, stats merge)
        "STATE_JSON": str(out_dir / "sparql_sources.json"),
        "SUMMARY_JSON": str(out_dir / "sparql_sources_list.json"),
        "STATS_TTL": str(out_dir / "dataset_stats.ttl"),
        "NAMED_GRAPHS_DIR": str(out_dir / "named_graphs"),
        "MWO_OWL_PATH": "ontology/mwo-full.owl",
        # Optional tuning:
        # "REQUEST_TIMEOUT": "30",
    }

    run_cmd(["python", args.script], cwd=args.workdir, env=env, check=True)

    # Verify the expected outputs exist
    assert_nonempty_file(in_ttl, "Updated ALL_TTL")
    assert_nonempty_file(out_dir / "dataset_stats.ttl", "STATS_TTL")
    assert_nonempty_file(out_dir / "sparql_sources_list.json", "SUMMARY_JSON")


if __name__ == "__main__":
    main()
