# pipeline/export_zenodo.py
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import ensure_dir, run_cmd, assert_nonempty_file


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run scripts.zenodo.export_zenodo and write outputs into the run-dir workspace."
    )
    ap.add_argument("--run-dir", required=True, help="Run directory root, e.g. /workspace/runs/<run_id>")
    ap.add_argument("--out", default=None, help="Explicit output path. Default: <run-dir>/data/zenodo/zenodo.ttl")
    ap.add_argument("--make-snapshots", action="store_true", help="Pass --make-snapshots to the exporter.")
    ap.add_argument("--workdir", default="/app", help="Repo working directory inside the image.")
    ap.add_argument("--python-exec", default="python", help="Python executable to use inside the container.")
    ap.add_argument("--extra-arg", action="append", default=[], help="Extra args to pass (repeatable).")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    zen_dir = run_dir / "data" / "zenodo"
    ensure_dir(zen_dir)

    out_path = Path(args.out) if args.out else (zen_dir / "zenodo.ttl")
    ensure_dir(out_path.parent)

    cmd = [args.python_exec, "-m", "scripts.zenodo.export_zenodo", "--out", str(out_path)]
    if args.make_snapshots:
        cmd.append("--make-snapshots")
    if args.extra_arg:
        cmd.extend(args.extra_arg)

    run_cmd(cmd, cwd=args.workdir, check=True)
    assert_nonempty_file(out_path, "Zenodo export TTL")


if __name__ == "__main__":
    main()
