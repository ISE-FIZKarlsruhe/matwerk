# rdf_harvester/registry.py
"""Read the RDF-file registration tab from the Google Sheet.

The tab is fetched as TSV through the same publish-to-web mechanism
``dags/spreadsheets.py`` uses, so registering data needs nothing but a new row:

    | source | url                                        | files          | label |
    |--------|--------------------------------------------|----------------|-------|
    |        | https://github.com/Owner/repo              |                | ...   |  <- auto
    | github | https://github.com/Owner/repo              | data/*.ttl     | ...   |
    | zenodo | https://zenodo.org/records/4195050         | kg.ttl         | ...   |
    |        | 10.5281/zenodo.4195050                     |                | ...   |  <- auto

* ``source`` may be left blank — it is inferred from the URL.
* ``files`` may be left blank — the harvester then picks the released artifacts
  automatically (see :mod:`.select`). Fill it in only to override that, e.g. to
  pin one file of an ontology repo.

Only these input columns are read; every other column in the tab (the status /
graph-IRI / query columns written back by the Apps Script) is ignored, so the
sheet can carry machine-written output without confusing the harvester.
"""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field
from typing import List, Optional

import requests

PUB_TSV = ("https://docs.google.com/spreadsheets/d/e/{publish_id}"
           "/pub?gid={gid}&single=true&output=tsv")

# Accepted column names, in PREFERENCE order (see _pick). `type` is deliberately last
# in _COL_SOURCE so a ROBOT `TYPE` column never wins over an explicit `source` column.
_COL_SOURCE = ("source", "kind", "type")
_COL_URL = ("url", "link", "repository", "repo", "record", "doi", "location")
_COL_FILES = ("files", "file", "include", "rdf files", "rdf_files")
_COL_LABEL = ("label", "name", "title", "notes", "comment")
# How often the curator wants the data re-fetched. Read for reporting; the DAGs
# themselves run weekly (incremental) and monthly (full re-sync).
_COL_SCHEDULE = ("re-sync", "resync", "sync", "schedule", "refresh")


@dataclass
class Registration:
    source: str                 # "github" | "zenodo"
    url: str                    # exactly as the curator typed it (match key for the sheet)
    files: List[str] = field(default_factory=list)   # optional include globs
    label: str = ""
    row: int = 0                # 1-based row in the tab (handy for logs)


def infer_source(url: str) -> Optional[str]:
    u = (url or "").lower()
    if "github.com" in u:
        return "github"
    if "zenodo.org" in u or re.search(r"10\.5281/zenodo\.", u):
        return "zenodo"
    return None


def _pick(header: List[str], names) -> Optional[int]:
    """Index of the first column matching ``names``, in *preference* order.

    Preference order, not header order: the tab also carries a ROBOT ``TYPE`` column
    holding a class IRI, and scanning the header left-to-right would match that as the
    "type"/source column and read `NFDI_0000009` as if it were `github`. Trying the
    preferred name first avoids the collision without renaming the ROBOT column.
    """
    lower = [h.strip().lower() for h in header]
    for want in names:                       # names is an ordered sequence
        if want in lower:
            return lower.index(want)
    return None


def parse_tsv(text: str) -> List[Registration]:
    rows = list(csv.reader(io.StringIO(text), delimiter="\t"))
    if not rows:
        return []
    header = rows[0]
    i_src, i_url = _pick(header, _COL_SOURCE), _pick(header, _COL_URL)
    i_files, i_label = _pick(header, _COL_FILES), _pick(header, _COL_LABEL)
    if i_url is None:
        raise ValueError(f"registration tab has no url column; header={header}")

    out: List[Registration] = []
    for n, r in enumerate(rows[1:], start=2):
        def cell(i: Optional[int]) -> str:
            return (r[i].strip() if i is not None and i < len(r) else "")

        url = cell(i_url)
        if not url or url.startswith("#"):
            continue
        src = (cell(i_src) or "").lower() or (infer_source(url) or "")
        if src not in {"github", "zenodo"}:
            print(f"[registry] row {n}: cannot tell source for {url!r} — skipped")
            continue
        files = [f.strip() for f in re.split(r"[,;\n]", cell(i_files)) if f.strip()]
        out.append(Registration(source=src, url=url, files=files,
                                label=cell(i_label), row=n))
    return out


def fetch_registrations(publish_id: str, gid: str, timeout: int = 60) -> List[Registration]:
    url = PUB_TSV.format(publish_id=publish_id, gid=gid)
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    regs = parse_tsv(r.content.decode("utf-8", errors="replace"))
    print(f"[registry] {len(regs)} registrations "
          f"({sum(1 for x in regs if x.source == 'github')} github, "
          f"{sum(1 for x in regs if x.source == 'zenodo')} zenodo)")
    return regs
