# zenodo_api.py
# Resilient Zenodo client with retries, backoff, and tunable paging via env vars.

from __future__ import annotations

import os
import time
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    # Expect these in your package; fallback defaults if not present.
    from .constants import ZENODO_API as _ZENODO_API_CONST, DEFAULT_PAGE_SIZE as _DEFAULT_PAGE_SIZE_CONST
    ZENODO_API = _ZENODO_API_CONST
    DEFAULT_PAGE_SIZE = _DEFAULT_PAGE_SIZE_CONST
except Exception:
    ZENODO_API = "https://zenodo.org/api/records"
    DEFAULT_PAGE_SIZE = 25

# ----- Tunables via env (sane defaults) -----
ZENODO_PAGE_SIZE = int(os.getenv("ZENODO_PAGE_SIZE", str(DEFAULT_PAGE_SIZE)))  # 25–100 are polite
ZENODO_MAX_PAGES = int(os.getenv("ZENODO_MAX_PAGES", "0"))  # 0 = no cap
ZENODO_TIMEOUT = float(os.getenv("ZENODO_TIMEOUT", "30"))  # seconds
ZENODO_RETRY_TOTAL = int(os.getenv("ZENODO_RETRY_TOTAL", "6"))
ZENODO_BACKOFF_FACTOR = float(os.getenv("ZENODO_BACKOFF", "1.5"))
ZENODO_STRICT_ERRORS = os.getenv("ZENODO_STRICT", "0") == "1"  # if False, soft-skip on 5xx after retries


def _build_session() -> requests.Session:
    retry = Retry(
        total=ZENODO_RETRY_TOTAL,
        connect=ZENODO_RETRY_TOTAL,
        read=ZENODO_RETRY_TOTAL,
        status=ZENODO_RETRY_TOTAL,
        backoff_factor=ZENODO_BACKOFF_FACTOR,  # 0s,1.5s,3s,4.5s,...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
        raise_on_status=False,  # we'll call raise_for_status() ourselves
    )
    sess = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


_SESS = _build_session()


def zenodo_request(url: str, params: Optional[dict] = None, token: Optional[str] = None) -> dict:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    r = _SESS.get(url, params=params, headers=headers, timeout=ZENODO_TIMEOUT)
    # Retries already applied by the adapter; now decide whether to fail hard.
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        if not ZENODO_STRICT_ERRORS and code in (500, 502, 503, 504, 429):
            # Soft-skip transient server problems in non-strict mode.
            print(f"WARNING: transient Zenodo error {code} on {r.url}; skipping this request.")
            return {"hits": {"hits": [], "total": 0}}
        raise
    return r.json()


def normalize_community(community: str) -> str:
    """
    Accepts either a slug ('nfdi-matwerk') or a Zenodo community URL.
    Returns the slug.
    """
    c = community.strip()
    if c.startswith("http://") or c.startswith("https://"):
        path = urlparse(c).path.rstrip("/")
        parts = [p for p in path.split("/") if p]
        if parts and parts[-1] != "communities":
            return parts[-1]
    return c


def _extract_total(hits_obj) -> Optional[int]:
    """Zenodo 'hits.total' can be int or {'value': int} depending on ES version."""
    if isinstance(hits_obj, dict):
        v = hits_obj.get("value")
        return int(v) if isinstance(v, int) else None
    if isinstance(hits_obj, int):
        return hits_obj
    return None


def iter_community_records(community: str, token: Optional[str]) -> Iterable[dict]:
    """
    Iterates over all (or capped) records of a Zenodo community.
    Honors env caps: ZENODO_PAGE_SIZE, ZENODO_MAX_PAGES.
    """
    slug = normalize_community(community)
    page = 1
    total = None

    while True:
        if ZENODO_MAX_PAGES and page > ZENODO_MAX_PAGES:
            break

        params = {
            "communities": slug,
            "size": ZENODO_PAGE_SIZE,
            "page": page,
            "sort": "mostrecent",
            "all_versions": 1,
        }
        data = zenodo_request(ZENODO_API, params=params, token=token)

        hits_wrapper = data.get("hits", {}) or {}
        hits = hits_wrapper.get("hits", []) or []

        if total is None:
            total = _extract_total(hits_wrapper.get("total"))
            if total is not None:
                print(f"Found ~{total} records in community '{slug}'.")

        if not hits:
            break

        for rec in hits:
            yield rec

        page += 1
        time.sleep(0.2)  # gentle pacing


def get_metadata(rec: dict) -> dict:
    return rec.get("metadata", rec) or {}


def get_doi(rec: dict) -> Optional[str]:
    meta = rec.get("metadata") or {}
    return meta.get("doi") or rec.get("doi")


def best_external_url(rec: dict) -> Optional[str]:
    links = rec.get("links", {}) or {}
    return (
        links.get("doi")
        or links.get("record_html")
        or links.get("self")
        or (f"https://zenodo.org/records/{rec.get('id')}" if rec.get("id") else None)
    )


def extract_files(rec: dict) -> List[Dict[str, str]]:
    """
    Returns a list of dicts with keys: key, checksum, link
    Handles both legacy 'files' list and newer 'files.entries' structure.
    """
    files: List[Dict[str, str]] = []

    # Legacy: top-level "files": [...]
    if isinstance(rec.get("files"), list):
        for f in rec["files"]:
            if not isinstance(f, dict):
                continue
            link = (
                (f.get("links", {}) or {}).get("self")
                or (f.get("links", {}) or {}).get("download")
                or (f.get("links", {}) or {}).get("content")
            )
            files.append(
                {
                    "key": f.get("key") or f.get("filename"),
                    "checksum": f.get("checksum"),
                    "link": link,
                }
            )

    # Newer structure: "files": {"entries": { ... }} or list-like
    elif isinstance(rec.get("files"), dict):
        entries = rec["files"].get("entries")
        if isinstance(entries, dict):
            entries = list(entries.values())
        if isinstance(entries, list):
            for f in entries:
                if not isinstance(f, dict):
                    continue
                links = f.get("links", {}) or {}
                link = links.get("content") or links.get("self")
                files.append(
                    {
                        "key": f.get("key") or f.get("id") or f.get("filename"),
                        "checksum": f.get("checksum"),
                        "link": link,
                    }
                )

    return files


def get_records_by_ids(ids: Iterable[str | int], token: Optional[str] = None) -> Iterable[dict]:
    for rid in ids:
        url = f"{ZENODO_API}/{rid}"
        try:
            yield zenodo_request(url, token=token)
        except Exception as e:
            print(f"! Failed to fetch record {rid}: {e}")
