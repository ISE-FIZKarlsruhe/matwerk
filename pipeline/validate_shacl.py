# pipeline/validate_shacl.py
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import ensure_dir, run_cmd, atomic_write_text


def main() -> None:
    ap = argparse.ArgumentParser(description="Run pyshacl for one shape against a dataset.")
    ap.add_argument("--shape", required=True, help="Shape TTL path.")
    ap.add_argument("--data", required=True, help="Data TTL path.")
    ap.add_argument("--out", required=True, help="Output markdown/text report path.")
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Fail (non-zero) when pyshacl returns non-zero. Default is soft-fail (write output and exit 0).",
    )
    ap.add_argument("--workdir", default="/app", help="Repo working directory inside the image.")
    args = ap.parse_args()

    out_path = Path(args.out)
    ensure_dir(out_path.parent)

    # pyshacl writes to stdout; capture it and write deterministically to file.
    res = run_cmd(
        ["python", "-m", "pyshacl", "-s", str(args.shape), str(args.data)],
        cwd=args.workdir,
        check=False,
        capture=True,
    )

    atomic_write_text(out_path, res.stdout + ("\n\n[stderr]\n" + res.stderr if res.stderr else ""))

    if res.returncode != 0 and args.strict:
        raise SystemExit(res.returncode)

    # soft-fail default: always exit 0 so the DAG can continue,
    # while still preserving the failure output in the report.


if __name__ == "__main__":
    main()
