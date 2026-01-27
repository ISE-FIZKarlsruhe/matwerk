# pipeline/docs_shmarql.py
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pipeline.common import ensure_dir, run_cmd, assert_nonempty_dir


def main() -> None:
    ap = argparse.ArgumentParser(description="Build site/docs via shmarql docs_build.")
    ap.add_argument("--docs-dir", required=True, help="Widoco docs directory.")
    ap.add_argument("--mkdocs-yml", required=True, help="mkdocs.yml path.")
    ap.add_argument("--out-dir", required=True, help="Output site directory (in workspace).")
    ap.add_argument("--config-yml", default="a.yml", help="Base shmarql config file path.")
    ap.add_argument("--workdir", default="/app", help="Workdir inside container.")
    args = ap.parse_args()

    docs_dir = Path(args.docs_dir)
    mkdocs_yml = Path(args.mkdocs_yml)
    out_dir = Path(args.out_dir)

    if not docs_dir.exists():
        raise FileNotFoundError(f"docs-dir not found: {docs_dir}")
    if not mkdocs_yml.exists():
        raise FileNotFoundError(f"mkdocs.yml not found: {mkdocs_yml}")

    ensure_dir(out_dir)

    # Shmarql image convention is /src/docs and /src/site; use symlinks.
    run_cmd(["bash", "-lc", f"rm -rf /src/docs /src/site && mkdir -p /src/site"], cwd=args.workdir, check=True)
    run_cmd(["bash", "-lc", f"ln -s '{docs_dir}' /src/docs"], cwd=args.workdir, check=True)

    # Put configs in a predictable place (current working directory)
    # (This assumes args.config-yml is either in workdir already or an absolute path.)
    run_cmd(["bash", "-lc", f"cp '{mkdocs_yml}' ./mkdocs.yml"], cwd=args.workdir, check=True)
    run_cmd(["bash", "-lc", f"cp '{args.config_yml}' ./a.yml"], cwd=args.workdir, check=True)

    # Run shmarql (use uv in the shmarql image)
    run_cmd(["bash", "-lc", "uv run python -m shmarql docs_build -f a.yml"], cwd=args.workdir, check=True)

    # Copy output back to the requested out-dir
    run_cmd(["bash", "-lc", f"cp -r /src/site/. '{out_dir}/'"], cwd=args.workdir, check=True)
    assert_nonempty_dir(out_dir, "shmarql site output")


if __name__ == "__main__":
    main()
