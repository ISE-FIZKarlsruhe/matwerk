# rdf_harvester/harvest.py
"""Fetch the RDF files behind one registration (GitHub release or Zenodo record).

Both sources reduce to the same shape — candidate files with a stable download
URL — so they share one artifact selector (:mod:`.select`) and one
downloader/parser:

* **GitHub** — the latest release: its assets, plus the RDF files in the
  repository tree at the release tag (ontology repos especially ship the RDF in
  the tree rather than as assets).
* **Zenodo** — the record's files (a deposit lists its files directly).

Files are kept verbatim: nothing is remodelled. Anything that does not parse
into a non-empty graph is skipped and logged.
"""
from __future__ import annotations

import fnmatch
import hashlib
import posixpath
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from .api import GitHubClient
from .select import select_artifacts
from .zenodo_api import ZenodoClient

# extension -> rdflib parser format
_RDF_FORMATS = {
    ".ttl": "turtle", ".n3": "n3", ".nt": "nt", ".nq": "nquads",
    ".trig": "trig", ".owl": "xml", ".rdf": "xml", ".jsonld": "json-ld",
}
RDF_EXTS = set(_RDF_FORMATS)


def _ext(path: str) -> str:
    p = path.lower()
    return p[p.rfind("."):] if "." in posixpath.basename(p) else ""


def is_rdf_path(path: str) -> bool:
    return _ext(path) in RDF_EXTS


def _guess_format(path: str) -> str:
    return _RDF_FORMATS.get(_ext(path), "turtle")


def _matches(path: str, patterns: List[str]) -> bool:
    """Glob match against both the full path and the basename."""
    base = posixpath.basename(path)
    return any(fnmatch.fnmatch(path, p) or fnmatch.fnmatch(base, p) for p in patterns)


@dataclass
class FileRDF:
    path: str
    source: str               # "asset" | "tree"
    download_url: str
    content_type: str
    sha256: str
    triples: int
    graph: object             # rdflib.Graph — verbatim file content


@dataclass
class SourceHarvest:
    """Result of harvesting one registered URL."""
    kind: str                          # "github" | "zenodo"
    key: str                           # owner/repo | zenodo record id
    registered_url: str                # exactly as registered (the sheet's match key)
    status: str                        # "ok" | "no_releases" | "no_rdf" | "not_found"
    version: Optional[str] = None      # release tag / zenodo version
    landing_url: Optional[str] = None  # human-facing page
    published_at: Optional[str] = None
    files: List[FileRDF] = field(default_factory=list)
    note: str = ""


def _parse_bytes(raw: bytes, fmt: str, public_id: str):
    from rdflib import Graph

    for f in [fmt] + [x for x in ("turtle", "xml", "json-ld", "nt") if x != fmt]:
        g = Graph()
        try:
            g.parse(data=raw, format=f, publicID=public_id)
            return g
        except Exception:  # noqa: BLE001 — try the next format
            continue
    print(f"[harvest]   could not parse {public_id}")
    return None


def _download_and_parse(fetch: Callable[[str], bytes], candidates: List[dict]) -> List[FileRDF]:
    files: List[FileRDF] = []
    for c in candidates:
        try:
            raw = fetch(c["download_url"])
        except Exception as e:  # noqa: BLE001
            print(f"[harvest]   download failed {c['download_url']}: {e}")
            continue
        g = _parse_bytes(raw, _guess_format(c["path"]), c["download_url"])
        if g is None or len(g) == 0:
            continue
        files.append(FileRDF(
            path=c["path"], source=c["source"], download_url=c["download_url"],
            content_type=_guess_format(c["path"]),
            sha256=hashlib.sha256(raw).hexdigest(), triples=len(g), graph=g,
        ))
        print(f"[harvest]   + {c['path']}  ({len(g)} triples, {c['source']})")
    return files


def _narrow(candidates: List[dict], include: List[str]) -> List[dict]:
    """Files named by the curator win; otherwise pick the artifacts automatically."""
    if include:
        picked = [c for c in candidates if _matches(c["path"], include)]
        if picked:
            print(f"[harvest]   {len(picked)} file(s) matched the registered list {include}")
            return picked
        print(f"[harvest]   none of {include} matched — falling back to auto-select")
    n = len(candidates)
    picked = select_artifacts(candidates)
    if n != len(picked):
        print(f"[harvest]   selected {len(picked)}/{n} RDF files "
              f"(released artifacts; variants collapsed to the full Turtle)")
    return picked


# --------------------------------------------------------------------------- #
# GitHub
# --------------------------------------------------------------------------- #
def _github_candidates(client: GitHubClient, owner_repo: str, rel: dict) -> List[dict]:
    tag = rel.get("tag_name") or rel.get("name") or "HEAD"
    by_path: Dict[str, dict] = {}
    for entry in client.get_tree(owner_repo, tag):
        if entry.get("type") == "blob" and is_rdf_path(entry.get("path", "")):
            p = entry["path"]
            by_path[p] = {"path": p, "source": "tree",
                          "download_url": client.raw_url(owner_repo, tag, p)}
    for a in rel.get("assets", []) or []:
        name = a.get("name", "")
        if is_rdf_path(name) and a.get("browser_download_url"):
            by_path[name] = {"path": name, "source": "asset",
                             "download_url": a["browser_download_url"]}
    return list(by_path.values())


def harvest_github(client: GitHubClient, owner_repo: str, registered_url: str,
                   include: Optional[List[str]] = None) -> SourceHarvest:
    rel = client.latest_release(owner_repo)
    if not rel:
        return SourceHarvest("github", owner_repo, registered_url, "no_releases",
                             note="repository has no published releases")
    files = _download_and_parse(
        client.download_bytes,
        _narrow(_github_candidates(client, owner_repo, rel), include or []),
    )
    return SourceHarvest(
        "github", owner_repo, registered_url, "ok" if files else "no_rdf",
        version=rel.get("tag_name") or rel.get("name"),
        landing_url=rel.get("html_url"), published_at=rel.get("published_at"),
        files=files,
        note="" if files else "no RDF found in the release (assets or tree)",
    )


# --------------------------------------------------------------------------- #
# Zenodo
# --------------------------------------------------------------------------- #
def harvest_zenodo(client: ZenodoClient, ref: str, registered_url: str,
                   include: Optional[List[str]] = None) -> SourceHarvest:
    rec = client.get_record(ref)
    if not rec:
        return SourceHarvest("zenodo", ref, registered_url, "not_found",
                             note="could not resolve the Zenodo record/DOI")
    rec_id = str(rec.get("id"))
    landing = ZenodoClient.html_url(rec)
    version = ZenodoClient.version(rec) or ZenodoClient.doi(rec)
    candidates = [f for f in ZenodoClient.files_of(rec) if is_rdf_path(f["path"])]
    if not candidates:
        return SourceHarvest("zenodo", rec_id, registered_url, "no_rdf",
                             version=version, landing_url=landing,
                             note="record has no RDF files")
    files = _download_and_parse(client.download_bytes, _narrow(candidates, include or []))
    return SourceHarvest(
        "zenodo", rec_id, registered_url, "ok" if files else "no_rdf",
        version=version, landing_url=landing,
        published_at=ZenodoClient.published_at(rec), files=files,
        note="" if files else "no RDF parsed from the record files",
    )
