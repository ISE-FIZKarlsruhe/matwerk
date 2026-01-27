# pipeline/docs_widoco.py
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import ensure_dir, run_cmd


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate Widoco documentation.")
    ap.add_argument("--in-ttl", required=True, help="Input TTL path (all.ttl).")
    ap.add_argument("--out-dir", required=True, help="Output docs directory.")
    ap.add_argument(
        "--widoco-jar",
        default="widoco-1.4.24-jar-with-dependencies_JDK-11.jar",
        help="Widoco jar path (relative to workdir or absolute).",
    )
    ap.add_argument("--workdir", default="/app", help="Repo working directory inside the image.")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    ensure_dir(out_dir)

    run_cmd(
        [
            "java",
            "-jar",
            str(args.widoco_jar),
            "-ontFile",
            str(args.in_ttl),
            "-outFolder",
            str(out_dir),
            "-uniteSections",
            "-includeAnnotationProperties",
            "-lang",
            "en-de",
            "-getOntologyMetadata",
            "-noPlaceHolderText",
            "-rewriteAll",
            "-webVowl",
        ],
        cwd=args.workdir,
        check=True,
    )


if __name__ == "__main__":
    main()
