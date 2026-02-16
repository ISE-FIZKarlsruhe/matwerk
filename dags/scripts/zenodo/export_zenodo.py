# scripts/zenodo/export_zenodo.py
from __future__ import annotations
from .export.cli import main as _cli_main

def run(argv: list[str] | None = None) -> None:
    """
    Programmatic entrypoint. If argv is provided, it is used instead of sys.argv.
    """
    if argv is None:
        _cli_main()
        return

    import sys
    old = sys.argv
    try:
        sys.argv = ["export_zenodo", *argv]
        _cli_main()
    finally:
        sys.argv = old

if __name__ == "__main__":
    _cli_main()
