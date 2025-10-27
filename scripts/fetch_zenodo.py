#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import re
import sys
import argparse
import subprocess
from pathlib import Path
from typing import Optional, Tuple, Iterable

from rdflib import ConjunctiveGraph

SPARQL_QUERY = """\
PREFIX obo:  <http://purl.obolibrary.org/obo/>
PREFIX nfdi: <https://nfdi.fiz-karlsruhe.de/ontology/>

SELECT DISTINCT ?dataset ?url WHERE {
  VALUES ?class {
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
    <http://purls.helmholtz-metadaten.de/mwo/MWO_0001058>
    <http://purls.helmholtz-metadaten.de/mwo/MWO_0001056>
    <http://purls.helmholtz-metadaten.de/mwo/MWO_0001057>
  }
  ?dataset a ?class .
  ?dataset obo:IAO_0000235 ?urlNode .
  ?urlNode nfdi:NFDI_0001008 ?u .
  BIND(STR(?u) AS ?url)

  FILTER(isIRI(?dataset))
  FILTER(CONTAINS(LCASE(?url), "zenodo"))  # only URLs mentioning “zenodo”
}
"""

DOI_RE = re.compile(r'(10\.\d{4,9}/\S+)', re.IGNORECASE)
ZENODO_REC_RE = re.compile(r'https?://zenodo\.org/record[s]?/(\d+)', re.IGNORECASE)

def guess_rdflib_format(path: Path) -> str:
    suf = path.suffix.lower()
    if suf in {".ttl"}:
        return "turtle"
    if suf in {".trig"}:
        return "trig"
    if suf in {".nt"}:
        return "nt"
    if suf in {".nq"}:
        return "nquads"
    if suf in {".rdf", ".xml", ".owl"}:
        return "xml"
    if suf in {".jsonld", ".json"}:
        return "json-ld"
    # default
    return "turtle"

def _normalize_doi(s: str) -> Optional[str]:
    if not s:
        return None
    v = s.strip()
    v = v.replace("https://doi.org/", "").replace("http://doi.org/", "").replace("doi:", "")
    v = v.strip().strip("/")
    return v.lower() if v else None

def detect_invocation(url: str) -> Tuple[Optional[str], Optional[str]]:
    m = DOI_RE.search(url)
    if m:
        doi = _normalize_doi(m.group(1))
        if doi:
            return "doi", doi
    m = ZENODO_REC_RE.search(url)
    if m:
        rec_id = m.group(1)
        return "record-url", f"https://zenodo.org/record/{rec_id}"
    return None, None

def safe_slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)

def query_local_turtle(turtle_path: Path, query: str) -> Iterable[Tuple[str, str]]:
    fmt = guess_rdflib_format(turtle_path)
    g = ConjunctiveGraph()
    g.parse(str(turtle_path), format=fmt)

    res = g.query(query)
    # rows are rdflib.query.ResultRow; access by variable name
    for row in res:
        # ensure both are present
        if "dataset" in res.vars and "url" in res.vars:
            dataset = str(row.dataset)
            url = str(row.url)
            if dataset and url:
                yield dataset, url
        else:
            # fallback by attribute if projection changed
            try:
                dataset = str(getattr(row, "dataset"))
                url = str(getattr(row, "url"))
                if dataset and url:
                    yield dataset, url
            except Exception:
                continue

def run_harvest(mode: str, value: str, out_dir: Path, python_exec: str, make_snapshots: bool, extra_args: list[str]) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    if mode == "doi":
        stem = safe_slug(value)
    else:
        m = ZENODO_REC_RE.search(value)
        stem = f"record_{m.group(1)}" if m else safe_slug(value)
    out_path = out_dir / f"{stem}.ttl"

    cmd = [
        python_exec, "-m", "scripts.zenodo.export_zenodo",
        f"--{mode}", value,
        "--out", str(out_path),
    ]
    if make_snapshots:
        cmd.append("--make-snapshots")
    if extra_args:
        cmd.extend(extra_args)

    print("→ Running:", " ".join(cmd))
    return subprocess.run(cmd).returncode

def main():
    ap = argparse.ArgumentParser(description="Query local TTL/TRiG and harvest Zenodo records.")
    ap.add_argument("--data", default="data/all.ttl", help="Path to Turtle/TRiG (or RDF/XML/NT/NQ/JSON-LD)")
    ap.add_argument("--out-csv", default="data/zenodo/datasets_urls.csv", help="CSV output for dataset,url pairs")
    ap.add_argument("--out-dir", default="data/zenodo/harvested", help="Directory to write harvester outputs (.trig)")
    ap.add_argument("--limit", type=int, default=-1, help="Limit harvest count")
    ap.add_argument("--dry-run", action="store_true", help="Only print actions")
    ap.add_argument("--no-snapshots", action="store_true", help="Do not pass --make-snapshots to harvester")
    ap.add_argument("--extra-arg", action="append", default=[], help="Extra args to pass to harvester (repeatable)")
    args = ap.parse_args()

    data_path = Path(args.data)
    if not data_path.exists():
        print(f"[ERROR] File not found: {data_path}")
        sys.exit(2)

    # 1) Query local graph
    rows = list(query_local_turtle(data_path, SPARQL_QUERY))
    print(f"Found {len(rows)} (dataset, url) rows.")

    # 2) Save to CSV
    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["dataset", "url"])
        for ds, u in rows:
            w.writerow([ds, u])
    print(f"Saved rows to {out_csv}")
    
    # 3) Prepare distinct harvest tasks
    seen = set()
    tasks: list[Tuple[str, str]] = []
    for _ds, url in rows:
        mode, value = detect_invocation(url)
        if not mode:
            # silently skip non-DOI / non-Zenodo links
            continue
        key = (mode, value)
        if key in seen:
            continue
        seen.add(key)
        tasks.append(key)

    if args.limit > 0:
        tasks = tasks[: args.limit]

    print(f"Prepared {len(tasks)} unique harvest tasks.")

    # 4) Execute
    failed = []
    for mode, value in tasks:
        if args.dry_run:
            print(f"[DRY RUN] Would harvest --{mode} {value}")
            continue
        rc = run_harvest(
            mode, value, Path(args.out_dir),
            python_exec=sys.executable,
            make_snapshots=not args.no_snapshots,
            extra_args=args.extra_arg,
        )
        if rc != 0:
            print(f"[ERROR] Harvester exited {rc} for --{mode} {value}")
            failed.append((mode, value))

    if failed:
        print("\nSome harvests failed:")
        for mode, value in failed:
            print(f"  --{mode} {value}")
        #sys.exit(1)
    else:
        print("\nAll harvests succeeded!")

if __name__ == "__main__":
    main()
