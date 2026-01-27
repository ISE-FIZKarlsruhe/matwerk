# pipeline/merge_components.py
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import assert_nonempty_file, ensure_dir, glob_files, replace_in_file, run_cmd


def main() -> None:
    ap = argparse.ArgumentParser(description="Reason TBox and merge OWL components only -> base not-reasoned.")
    ap.add_argument("--src-owl", required=True)
    ap.add_argument("--components-dir", required=True)
    ap.add_argument("--out-owl", required=True, help="Output OWL path (base), e.g. .../all_NotReasoned.base.owl")
    ap.add_argument("--out-ttl", required=True, help="Output TTL path (base), e.g. .../all_NotReasoned.base.ttl")
    ap.add_argument("--robot-bin", default="robot")
    ap.add_argument("--reasoner", default="hermit")
    ap.add_argument("--workdir", default="/app")
    args = ap.parse_args()

    components_dir = Path(args.components_dir)
    out_owl = Path(args.out_owl)
    out_ttl = Path(args.out_ttl)

    ensure_dir(components_dir)
    ensure_dir(out_owl.parent)
    ensure_dir(out_ttl.parent)

    # 1) TBox reason (traceability)
    mwo_reasoned = components_dir / "mwo_reasoned.owl"
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

    # 2) Ensure component OWLs exist
    component_owls = [p for p in glob_files(components_dir, "*.owl") if p.name != "mwo_reasoned.owl"]
    if not component_owls:
        raise RuntimeError(f"No component OWL files found in {components_dir}")

    # 3) Merge components -> base OWL
    run_cmd(
        [
            args.robot_bin,
            "merge",
            "--include-annotations",
            "true",
            "-i",
            str(args.src_owl),
            "--inputs",
            str(components_dir / "*.owl"),
            "--output",
            str(out_owl),
        ],
        cwd=args.workdir,
        check=True,
    )

    # Fix datatype annotation
    replace_in_file(
        out_owl,
        "<ontology:NFDI_0001008>",
        '<ontology:NFDI_0001008 rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">',
    )
    assert_nonempty_file(out_owl, "all_NotReasoned.base.owl")

    # 4) Convert -> base TTL (so downstream always has TTL)
    run_cmd([args.robot_bin, "convert", "-i", str(out_owl), "-o", str(out_ttl)], cwd=args.workdir, check=True)
    assert_nonempty_file(out_ttl, "all_NotReasoned.base.ttl")


if __name__ == "__main__":
    main()
