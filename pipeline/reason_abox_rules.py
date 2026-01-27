# pipeline/reason_abox_rules.py
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import (
    assert_nonempty_file,
    ensure_dir,
    robot_query_count_triples,
    run_cmd,
)

DEFAULT_RULE_ORDER = [
    ("infer-equivalent-props.ru", "infer-equivalent-props"),
    ("infer-inverses.ru", "infer-inverses"),
    ("infer-property-chains.ru", "infer-property-chains"),
    ("infer-transitive.ru", "infer-transitive"),
    ("infer-subprops.ru", "infer-subprops"),
    ("infer-domain-types.ru", "infer-domain-types"),
    ("infer-range-types.ru", "infer-range-types"),
    ("infer-subclass-types.ru", "infer-subclass-types"),
    ("infer-sameas-closure.ru", "infer-sameas-closure"),
    ("infer-sameas-types.ru", "infer-sameas-types"),
    ("infer-sameas-props.ru", "infer-sameas-props"),
]


def main() -> None:
    ap = argparse.ArgumentParser(description="ABox materialization via ROBOT SPARQL UPDATE rule chain.")
    ap.add_argument("--in-ttl", required=True, help="Input TTL (all_NotReasoned.ttl).")
    ap.add_argument("--rules-dir", required=True, help="Directory containing *.ru update files and count-triples.rq.")
    ap.add_argument("--out-ttl", required=True, help="Final output TTL (all.ttl).")
    ap.add_argument("--work-dir", required=True, help="Directory for intermediate steps and counts.")
    ap.add_argument("--robot-bin", default="robot", help="ROBOT executable.")
    ap.add_argument(
        "--count-query",
        default="count-triples.rq",
        help="SPARQL query filename under rules-dir used to count triples (default: count-triples.rq).",
    )
    ap.add_argument("--workdir", default="/app", help="Repo working directory inside the image.")
    ap.add_argument(
        "--write-counts",
        action="store_true",
        help="Write a TSV summary of triple counts per step into <work-dir>/counts.tsv.",
    )
    args = ap.parse_args()

    rules_dir = Path(args.rules_dir)
    work_dir = Path(args.work_dir)
    ensure_dir(work_dir)

    count_query_path = rules_dir / args.count_query
    if not count_query_path.exists():
        raise FileNotFoundError(f"Missing triple count query: {count_query_path}")

    current_file = Path(args.in_ttl)
    assert_nonempty_file(current_file, "Input TTL")

    counts_rows: list[str] = ["step\tlabel\ttriples\tadded\n"]

    def count_triples(file_path: Path) -> int:
        return robot_query_count_triples(
            robot_bin=args.robot_bin,
            input_file=file_path,
            count_query_path=count_query_path,
            out_tsv=work_dir / f"{file_path.name}.count.tsv",
        )

    current_count = count_triples(current_file)
    if args.write_counts:
        counts_rows.append(f"0\tnot_reasoned\t{current_count}\t0\n")

    for idx, (rule_file, label) in enumerate(DEFAULT_RULE_ORDER, start=1):
        rule_path = rules_dir / rule_file
        if not rule_path.exists():
            raise FileNotFoundError(f"Missing rule file: {rule_path}")

        next_file = work_dir / f"step{idx}.ttl"
        before = current_count

        run_cmd(
            [
                args.robot_bin,
                "query",
                "-i",
                str(current_file),
                "--update",
                str(rule_path),
                "-o",
                str(next_file),
            ],
            cwd=args.workdir,
            check=True,
        )

        assert_nonempty_file(next_file, f"Rule output {label}")
        current_file = next_file
        current_count = count_triples(current_file)
        added = current_count - before

        print(f"[pipeline] {label}: added {added} triples (total {current_count})")
        if args.write_counts:
            counts_rows.append(f"{idx}\t{label}\t{current_count}\t{added}\n")

    out_ttl = Path(args.out_ttl)
    ensure_dir(out_ttl.parent)
    out_ttl.write_bytes(current_file.read_bytes())

    # Remove default empty prefix lines as you did in bash
    text = out_ttl.read_text(encoding="utf-8", errors="replace").splitlines(True)
    text = [ln for ln in text if not ln.startswith("@prefix : <")]
    out_ttl.write_text("".join(text), encoding="utf-8")

    assert_nonempty_file(out_ttl, "Final all.ttl")

    if args.write_counts:
        (work_dir / "counts.tsv").write_text("".join(counts_rows), encoding="utf-8")


if __name__ == "__main__":
    main()
