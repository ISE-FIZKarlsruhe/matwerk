# pipeline/merge_all.py
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import assert_nonempty_file, ensure_dir, glob_files, replace_in_file, run_cmd

DEFAULT_ONTBASE = "https://nfdi.fiz-karlsruhe.de/ontology"


def main() -> None:
    ap = argparse.ArgumentParser(description="Reason TBox and merge components + Zenodo into all_NotReasoned.ttl.")
    ap.add_argument("--src-owl", required=True, help="Base ontology OWL (TBox), e.g. ontology/mwo-full.owl")
    ap.add_argument("--components-dir", required=True, help="Directory containing component OWLs")
    ap.add_argument("--zenodo-ttl", required=True, help="Zenodo TTL path")
    ap.add_argument("--out-not-reasoned", required=True, help="Output TTL path for merged not-reasoned KG")
    ap.add_argument("--robot-bin", default="robot", help="ROBOT executable")
    ap.add_argument("--reasoner", default="hermit", help="Reasoner for TBox reasoning")
    ap.add_argument("--workdir", default="/app", help="Repo working directory inside the image")
    args = ap.parse_args()

    components_dir = Path(args.components_dir)
    out_not_reasoned = Path(args.out_not_reasoned)
    ensure_dir(out_not_reasoned.parent)

    # 1) Reason the ontology (TBox) - produces a file in components_dir for traceability
    mwo_reasoned = components_dir / "mwo_reasoned.owl"
    ensure_dir(components_dir)

    run_cmd(
        [
            args.robot_bin,
            "reason",
            "--reasoner",
            args.reasoner,
            "--input",
            str(args.src_owl),
            "--axiom-generators",
            "SubClass SubDataProperty ClassAssertion EquivalentObjectProperty PropertyAssertion "
            "InverseObjectProperties SubObjectProperty",
            "--output",
            str(mwo_reasoned),
        ],
        cwd=args.workdir,
        check=True,
    )
    assert_nonempty_file(mwo_reasoned, "mwo_reasoned.owl")

    # 2) Merge OWL components -> all_NotReasoned.owl (intermediate)
    # Gather component OWLs; exclude mwo_reasoned.owl itself if you do not want it in inputs.
    component_owls = [p for p in glob_files(components_dir, "*.owl") if p.name != "mwo_reasoned.owl"]
    if not component_owls:
        raise RuntimeError(f"No component OWL files found in {components_dir}")

    all_not_reasoned_owl = out_not_reasoned.with_suffix(".owl")

    merge_cmd = [
        args.robot_bin,
        "merge",
        "--include-annotations",
        "true",
        "-i",
        str(args.src_owl),
        "--inputs",
        str(components_dir / "*.owl"),
        "--output",
        str(all_not_reasoned_owl),
    ]
    run_cmd(merge_cmd, cwd=args.workdir, check=True)

    replace_in_file(
        all_not_reasoned_owl,
        "<ontology:NFDI_0001008>",
        '<ontology:NFDI_0001008 rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">',
    )
    assert_nonempty_file(all_not_reasoned_owl, "all_NotReasoned.owl")

    # 3) Merge with Zenodo data -> all_NotReasoned.ttl
    run_cmd(
        [
            args.robot_bin,
            "merge",
            "--include-annotations",
            "true",
            "-i",
            str(all_not_reasoned_owl),
            "-i",
            str(args.zenodo_ttl),
            "--output",
            str(out_not_reasoned),
        ],
        cwd=args.workdir,
        check=True,
    )

    assert_nonempty_file(out_not_reasoned, "all_NotReasoned.ttl")


if __name__ == "__main__":
    main()
