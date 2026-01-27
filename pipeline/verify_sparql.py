# pipeline/verify_sparql.py
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import ensure_dir, run_cmd, atomic_write_text


def main() -> None:
    ap = argparse.ArgumentParser(description="Run ROBOT verify with SPARQL queries.")
    ap.add_argument("--data", required=True, help="Input TTL path.")
    ap.add_argument("--queries", required=True, help="SPARQL queries file (robot verify format).")
    ap.add_argument("--out-dir", required=True, help="Output directory.")
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Fail the task when robot verify returns non-zero. Default is soft-fail.",
    )
    ap.add_argument("--robot-bin", default="robot", help="ROBOT executable.")
    ap.add_argument("--workdir", default="/app", help="Repo working directory inside the image.")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    ensure_dir(out_dir)

    # Capture output to a deterministic report file.
    report_path = out_dir / "verify1.md"
    res = run_cmd(
        [
            args.robot_bin,
            "verify",
            "--input",
            str(args.data),
            "--queries",
            str(args.queries),
            "--output-dir",
            str(out_dir),
            "-vvv",
        ],
        cwd=args.workdir,
        check=False,
        capture=True,
    )

    atomic_write_text(report_path, res.stdout + ("\n\n[stderr]\n" + res.stderr if res.stderr else ""))

    if res.returncode != 0 and args.strict:
        raise SystemExit(res.returncode)


if __name__ == "__main__":
    main()
