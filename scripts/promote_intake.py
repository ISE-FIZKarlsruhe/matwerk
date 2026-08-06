#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Promote rows from the intake sheet into production-sheet rows.

A curator fills in the **intake** sheet with human-friendly columns and no IDs. The
**production** workbook needs the same information as ROBOT template rows: a minted
identity, a class IRI, and — because values live in their own nodes — one extra row
per value the entity is denoted by.

This script does that conversion and stops. It writes two CSVs for a human to paste:

    <out>/entities.csv     the intake rows, with an ID minted and the TYPE filled in
    <out>/value_nodes.csv  one row per value node the entities point at, typed

Nothing is written back to any spreadsheet: promotion is a decision, and the two
files are exactly what a curator would otherwise have typed by hand.

Why it refuses rows
-------------------
A production row whose `I` column names a label that exists nowhere resolves to
*nothing* — ROBOT drops the relation silently, and the KG is quietly missing an edge.
3.8 % of the workbook's references are already in that state. So a row whose
references cannot be resolved is reported and **not** promoted, with the missing
labels named, rather than written out to fail later.

IDs
---
`<epoch_ms><counter>`, the scheme the workbook already uses (e.g. `17588917500551` =
epoch 1758891750055 + counter 1). Each node is minted independently — an entity and
its value nodes do not share a stem — and a counter guarantees uniqueness within a
run. Already-promoted rows (an ID present) are skipped.

Usage
-----
    python scripts/promote_intake.py \
        --intake  intake.csv \
        --sheets-dir <dir with the production <tab>.csv files> \
        --type https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009 \
        --out promoted/
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import time
from typing import Dict, List, Optional, Tuple

MATWERK_KG = "https://nfdi.fiz-karlsruhe.de/matwerk/msekg/"
NFDI = "https://nfdi.fiz-karlsruhe.de/ontology/"
OBO = "http://purl.obolibrary.org/obo/"

# Intake column  ->  (production column, class of the value node it becomes)
# A value node is only minted for columns that denote something; columns that name an
# entity already in the KG (creator, affiliation, licence) must resolve instead.
VALUE_NODE_CLASSES = {
    "short description": (NFDI + "NFDI_0001018", "description of {label}"),
    "description": (NFDI + "NFDI_0001018", "description of {label}"),
    "link (url / pid)": (NFDI + "NFDI_0000223", "url of {label}"),
    "url": (NFDI + "NFDI_0000223", "url of {label}"),
    "sparql endpoint": (NFDI + "NFDI_0001095", "sparql endpoint of {label}"),
    "documentation link": (NFDI + "NFDI_0000223", "documentation of {label}"),
    "repository link": (NFDI + "NFDI_0001210", "repository of {label}"),
}
# Columns that must resolve to an existing instance, never be minted.
MUST_RESOLVE = {"creator(s)", "creator(s) affiliation", "contributor(s)",
                "contributor(s) affiliation", "license", "licence"}


class Minter:
    """`<epoch_ms><counter>` — the workbook's own scheme, unique within a run."""

    def __init__(self) -> None:
        self._n = 0

    def mint(self) -> str:
        self._n += 1
        return f"{MATWERK_KG}{int(time.time() * 1000)}{self._n}"


def read_csv(path: str) -> List[List[str]]:
    with open(path, encoding="utf-8-sig", newline="") as fh:
        return list(csv.reader(fh))


def build_label_index(sheets_dir: str) -> Dict[str, Tuple[str, str]]:
    """Every label already in the production workbook -> (tab, ID)."""
    idx: Dict[str, Tuple[str, str]] = {}
    for f in sorted(glob.glob(os.path.join(sheets_dir, "*.csv"))):
        tab = os.path.basename(f)[:-4]
        rows = read_csv(f)
        if len(rows) < 3:
            continue
        lab_cols = [i for i, c in enumerate(rows[1]) if c.strip().startswith("A rdfs:label")]
        for r in rows[2:]:
            for i in lab_cols:
                if i < len(r) and r[i].strip():
                    idx.setdefault(r[i].strip().lower(), (tab, r[0].strip()))
    return idx


def promote(intake_path: str, sheets_dir: str, entity_type: str, out_dir: str,
            id_col: str = "#") -> dict:
    rows = read_csv(intake_path)
    if len(rows) < 3:
        raise SystemExit("intake sheet has no data rows")
    head, directives, data = rows[0], rows[1], rows[2:]
    lower = [h.strip().lower() for h in head]
    labels = build_label_index(sheets_dir)
    minter = Minter()

    def col(name: str) -> Optional[int]:
        return lower.index(name) if name in lower else None

    i_id = col(id_col.lower()) if col(id_col.lower()) is not None else 0
    i_label = next((i for i, h in enumerate(lower) if h in ("label", "name", "title")), None)
    if i_label is None:
        raise SystemExit(f"no label column found in {head}")

    entities: List[List[str]] = []
    value_nodes: List[List[str]] = []
    skipped: List[dict] = []
    promoted: List[dict] = []

    for n, r in enumerate(data, start=3):
        if not any(c.strip() for c in r):
            continue
        if i_id < len(r) and r[i_id].strip():
            continue                                   # already promoted
        label = r[i_label].strip() if i_label < len(r) else ""
        if not label:
            continue

        # 1) every reference this row makes must already exist
        missing = []
        for i, h in enumerate(lower):
            if h in MUST_RESOLVE and i < len(r) and r[i].strip():
                for part in re.split(r"[|;]", r[i]):
                    part = part.strip()
                    if part and part.lower() not in labels:
                        missing.append(f"{head[i]}: {part}")
        if missing:
            skipped.append({"row": n, "label": label, "missing": missing})
            continue

        # 2) mint the entity
        ent_id = minter.mint()
        ent_row = {"#": ent_id, "TYPE": entity_type, "Label": label}
        made: List[str] = []

        # 3) one value node per denoting column
        for i, h in enumerate(lower):
            val = r[i].strip() if i < len(r) else ""
            if not val or h not in VALUE_NODE_CLASSES:
                continue
            cls, pattern = VALUE_NODE_CLASSES[h]
            vn_label = pattern.format(label=label)
            vn_id = minter.mint()
            value_nodes.append([vn_id, cls, vn_label,
                                val if cls != NFDI + "NFDI_0001018" else "",
                                val if cls == NFDI + "NFDI_0001018" else ""])
            ent_row[head[i]] = vn_label       # the production row references it BY LABEL
            made.append(vn_label)

        # 4) resolved references pass through unchanged (they already resolve by label)
        for i, h in enumerate(lower):
            if h in MUST_RESOLVE and i < len(r) and r[i].strip():
                ent_row[head[i]] = r[i].strip()

        entities.append(ent_row)
        promoted.append({"row": n, "label": label, "id": ent_id, "value_nodes": made})

    os.makedirs(out_dir, exist_ok=True)
    ent_cols = ["#", "TYPE", "Label"] + [h for h in head
                                         if h.strip().lower() in VALUE_NODE_CLASSES
                                         or h.strip().lower() in MUST_RESOLVE]
    with open(os.path.join(out_dir, "entities.csv"), "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(ent_cols)
        for e in entities:
            w.writerow([e.get(c, "") for c in ent_cols])
    with open(os.path.join(out_dir, "value_nodes.csv"), "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["#", "TYPE", "label", "has url", "Value"])
        w.writerows(value_nodes)

    print(f"[promote] promoted {len(entities)} row(s) -> {len(value_nodes)} value node(s)")
    for p in promoted:
        print(f"   row {p['row']}: {p['label']}  ->  {p['id'].rsplit('/', 1)[-1]}"
              + (f"  (+{len(p['value_nodes'])} value nodes)" if p["value_nodes"] else ""))
    if skipped:
        print(f"[promote] {len(skipped)} row(s) NOT promoted — references do not resolve:")
        for s in skipped:
            print(f"   row {s['row']}: {s['label']}")
            for m in s["missing"]:
                print(f"       missing -> {m}")
        print("   Create those entities in the production workbook first, then re-run.")
    return {"promoted": promoted, "skipped": skipped,
            "entities": len(entities), "value_nodes": len(value_nodes)}


def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--intake", required=True, help="CSV export of the intake tab")
    ap.add_argument("--sheets-dir", required=True,
                    help="directory of production <tab>.csv exports (for label resolution)")
    ap.add_argument("--type", default=NFDI + "NFDI_0000009",
                    help="class IRI for the promoted entities (default: dataset)")
    ap.add_argument("--out", default="promoted")
    args = ap.parse_args(argv)
    promote(args.intake, args.sheets_dir, args.type, args.out)


if __name__ == "__main__":
    main()
