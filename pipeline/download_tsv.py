# pipeline/download_tsv.py
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import assert_nonempty_file, ensure_dir, run_cmd


DEFAULT_BASE_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub"
)


def main() -> None:
    ap = argparse.ArgumentParser(description="Download one TSV from Google Sheets publish URL.")
    ap.add_argument("--name", required=True, help="Component name (for logging only).")
    ap.add_argument("--gid", required=True, help="Google Sheets gid.")
    ap.add_argument("--out", required=True, help="Output TSV path.")
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base published URL.")
    args = ap.parse_args()

    out_path = Path(args.out)
    ensure_dir(out_path.parent)

    url = f"{args.base_url}?gid={args.gid}&single=true&output=tsv"
    # Use curl to match your existing behavior; robust retries are appropriate here.
    run_cmd(
        [
            "curl",
            "-sS",
            "-L",
            "--retry",
            "5",
            "--retry-delay",
            "2",
            "--fail",
            url,
            "-o",
            str(out_path),
        ],
        check=True,
    )
    assert_nonempty_file(out_path, f"TSV ({args.name})")


if __name__ == "__main__":
    main()
