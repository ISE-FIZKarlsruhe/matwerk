# pipeline/build_component.py
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import assert_nonempty_file, ensure_dir, replace_in_file, run_cmd


DEFAULT_ONTBASE = "https://nfdi.fiz-karlsruhe.de/ontology"


def main() -> None:
    ap = argparse.ArgumentParser(description="Build one OWL component with ROBOT template + merge + explain.")
    ap.add_argument("--name", required=True, help="Component name (for logging).")
    ap.add_argument("--src-owl", required=True, help="Base ontology OWL (e.g., ontology/mwo-full.owl).")
    ap.add_argument("--tsv", required=True, help="Template TSV path.")
    ap.add_argument("--out-owl", required=True, help="Output OWL path.")
    ap.add_argument("--out-explain", required=True, help="Output markdown explanation path.")
    ap.add_argument(
        "--inputs",
        action="append",
        default=[],
        help="Additional OWL inputs (repeatable). Use for dependency chaining.",
    )
    ap.add_argument("--ontbase", default=DEFAULT_ONTBASE, help="Base prefix IRI for ROBOT --prefix.")
    ap.add_argument("--robot-bin", default="robot", help="ROBOT executable name/path.")
    ap.add_argument("--reasoner", default="hermit", help="Reasoner for ROBOT explain.")
    ap.add_argument("--workdir", default="/app", help="Repo working directory inside the image.")
    args = ap.parse_args()

    out_owl = Path(args.out_owl)
    out_explain = Path(args.out_explain)
    ensure_dir(out_owl.parent)
    ensure_dir(out_explain.parent)

    # Build merge inputs: always include src-owl + any extra upstream OWLs
    merge_cmd = [args.robot_bin, "merge", "--include-annotations", "true", "-i", str(args.src_owl)]
    for inp in args.inputs:
        merge_cmd.extend(["-i", str(inp)])

    # Merge + template -> out owl
    merge_cmd.extend(
        [
            "template",
            "--template",
            str(args.tsv),
            "--prefix",
            f"nfdicore: {args.ontbase}/",
            "--output",
            str(out_owl),
        ]
    )

    # Retry with -vvv on failure (matching your existing pattern)
    try:
        run_cmd(merge_cmd, cwd=args.workdir, check=True)
    except Exception:
        run_cmd([args.robot_bin, "-vvv", *merge_cmd[1:]], cwd=args.workdir, check=True)

    # Apply your datatype fix (component-local)
    replace_in_file(
        out_owl,
        "<ontology:NFDI_0001008>",
        '<ontology:NFDI_0001008 rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">',
    )

    assert_nonempty_file(out_owl, f"Component OWL ({args.name})")

    # Explain inconsistencies for this component
    explain_cmd = [
        args.robot_bin,
        "explain",
        "--reasoner",
        args.reasoner,
        "--input",
        str(out_owl),
        "-M",
        "inconsistency",
        "--explanation",
        str(out_explain),
    ]

    try:
        run_cmd(explain_cmd, cwd=args.workdir, check=True)
    except Exception:
        run_cmd([args.robot_bin, "-vvv", *explain_cmd[1:]], cwd=args.workdir, check=True)

    # Explanation file may be empty if no issues; do not assert non-empty.


if __name__ == "__main__":
    main()
