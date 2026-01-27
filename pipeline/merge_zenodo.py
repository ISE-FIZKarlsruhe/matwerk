# pipeline/merge_zenodo.py
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from pipeline.common import assert_nonempty_file, ensure_dir, run_cmd


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge base not-reasoned TTL with Zenodo TTL -> all_NotReasoned.ttl")
    ap.add_argument("--base-ttl", required=True, help="Base TTL, e.g. .../all_NotReasoned.base.ttl")
    ap.add_argument("--zenodo-ttl", required=True, help="Zenodo TTL, e.g. .../data/zenodo/zenodo.ttl")
    ap.add_argument("--out-ttl", required=True, help="Output TTL, e.g. .../all_NotReasoned.ttl")
    ap.add_argument("--robot-bin", default="robot")
    ap.add_argument("--workdir", default="/app")
    args = ap.parse_args()

    base_ttl = Path(args.base_ttl)
    zenodo_ttl = Path(args.zenodo_ttl)
    out_ttl = Path(args.out_ttl)
    ensure_dir(out_ttl.parent)

    assert_nonempty_file(base_ttl, "base TTL")

    # If Zenodo missing/empty, just copy base -> out (no-op merge)
    if not zenodo_ttl.exists() or zenodo_ttl.stat().st_size == 0:
        print(f"[merge_zenodo] Zenodo TTL missing/empty, writing out base TTL: {zenodo_ttl}")
        out_ttl.write_bytes(base_ttl.read_bytes())
        assert_nonempty_file(out_ttl, "all_NotReasoned.ttl (copied base)")
        return

    try:
        run_cmd(
            [
                args.robot_bin,
                "merge",
                "--include-annotations",
                "true",
                "-i",
                str(base_ttl),
                "-i",
                str(zenodo_ttl),
                "--output",
                str(out_ttl),
            ],
            cwd=args.workdir,
            check=True,
        )
    except subprocess.CalledProcessError:
        # fallback: convert Zenodo -> OWL and retry
        zenodo_owl = zenodo_ttl.with_suffix(".owl")
        run_cmd([args.robot_bin, "convert", "-i", str(zenodo_ttl), "-o", str(zenodo_owl)], cwd=args.workdir, check=True)
        assert_nonempty_file(zenodo_owl, "zenodo.owl (converted)")

        run_cmd(
            [
                args.robot_bin,
                "merge",
                "--include-annotations",
                "true",
                "-i",
                str(base_ttl),
                "-i",
                str(zenodo_owl),
                "--output",
                str(out_ttl),
            ],
            cwd=args.workdir,
            check=True,
        )

    assert_nonempty_file(out_ttl, "all_NotReasoned.ttl")


if __name__ == "__main__":
    main()
