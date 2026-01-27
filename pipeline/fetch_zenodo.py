# pipeline/fetch_zenodo.py
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import ensure_dir, run_cmd, assert_nonempty_file


def main() -> None:
    ap = argparse.ArgumentParser(description="Run scripts/fetch_zenodo.py scoped to run-dir outputs.")
    ap.add_argument("--data", help="Input KG path to query (e.g., /workspace/runs/<id>/data/all.ttl).")
    ap.add_argument("--run-dir", required=True, help="Run directory root.")
    ap.add_argument("--script", default="scripts/fetch_zenodo.py", help="Script path.")
    ap.add_argument("--limit", type=int, default=-1, help="Optional limit.")
    ap.add_argument("--dry-run", action="store_true", help="Do not harvest, only print actions.")
    ap.add_argument("--workdir", default="/app", help="Repo working directory inside the image.")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    
    data_path = Path(args.data) if args.data else None



    zen_dir = run_dir / "data" / "zenodo"
    ensure_dir(zen_dir)
    ensure_dir(zen_dir / "harvested")

    out_csv = zen_dir / "datasets_urls.csv"

    cmd = [
        "python",
        args.script,
        "--data",
        str(data_path),
        "--out-csv",
        str(out_csv),
        "--out-dir",
        str(zen_dir / "harvested"),
    ]
    if args.limit and args.limit > 0:
        cmd += ["--limit", str(args.limit)]
    if args.dry_run:
        cmd += ["--dry-run"]

    run_cmd(cmd, cwd=args.workdir, check=True)

    assert_nonempty_file(out_csv, "Zenodo CSV")
    # harvested TTLs may be zero if no Zenodo links found; do not assert non-empty directory.


if __name__ == "__main__":
    main()
