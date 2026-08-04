# rdf_harvester/zenodo_api.py
"""Resolve a registered Zenodo reference to its RDF files.

Accepts whatever a curator is likely to paste into the registration sheet: a
record URL (``https://zenodo.org/records/123``, also the older ``/record/123``),
a DOI (``10.5281/zenodo.123`` or ``https://doi.org/…``), or a bare record id.

Zenodo deposits list their files directly, so — unlike GitHub — there is no tree
to walk: the record's files *are* the released artifacts, and they go through the
same artifact selection as GitHub release assets.
"""
from __future__ import annotations

import re
from typing import List, Optional

import requests

ZENODO_API = "https://zenodo.org/api/records"
_REC_URL_RE = re.compile(r"zenodo\.org/records?/(\d+)", re.IGNORECASE)
_DOI_RE = re.compile(r"(10\.\d{4,9}/[^\s]+)", re.IGNORECASE)
_ZENODO_DOI_RE = re.compile(r"10\.5281/zenodo\.(\d+)", re.IGNORECASE)


def record_id_from(ref: str) -> Optional[str]:
    """Extract a Zenodo record id from a URL / DOI / bare id, else ``None``."""
    if not ref:
        return None
    ref = ref.strip()
    m = _REC_URL_RE.search(ref)
    if m:
        return m.group(1)
    m = _ZENODO_DOI_RE.search(ref)
    if m:
        return m.group(1)
    if ref.isdigit():
        return ref
    return None


class ZenodoClient:
    def __init__(self, token: Optional[str] = None, timeout: int = 60) -> None:
        self.timeout = timeout
        self.token = token
        self.s = requests.Session()
        self.s.headers.update({"Accept": "application/json",
                               "User-Agent": "matwerk-rdf-harvester"})

    def _params(self) -> dict:
        return {"access_token": self.token} if self.token else {}

    def download_bytes(self, url: str) -> bytes:
        r = self.s.get(url, params=self._params(), timeout=self.timeout)
        r.raise_for_status()
        return r.content

    def get_record(self, ref: str) -> Optional[dict]:
        """Fetch the record JSON for a record id / URL; falls back to a DOI search."""
        rec_id = record_id_from(ref)
        if rec_id:
            try:
                r = self.s.get(f"{ZENODO_API}/{rec_id}", params=self._params(), timeout=self.timeout)
                if r.status_code == 200:
                    return r.json()
            except Exception as e:  # noqa: BLE001
                print(f"[zenodo] record {rec_id} fetch failed: {e}")
        # not a zenodo-hosted DOI → resolve via the search API
        m = _DOI_RE.search(ref or "")
        if not m:
            return None
        doi = m.group(1).rstrip("/.")
        try:
            p = self._params(); p.update({"q": f'doi:"{doi}"', "size": 1})
            r = self.s.get(ZENODO_API, params=p, timeout=self.timeout)
            if r.status_code == 200:
                hits = (r.json().get("hits") or {}).get("hits") or []
                if hits:
                    return hits[0]
        except Exception as e:  # noqa: BLE001
            print(f"[zenodo] DOI search failed for {doi}: {e}")
        return None

    @staticmethod
    def files_of(rec: dict) -> List[dict]:
        """Return [{'path','source','download_url'}] for every file in the record.

        Zenodo has used a few different shapes for the file list over the years;
        handle the current ``files[].key/links.self`` and the legacy
        ``files[].filename/links.download``.
        """
        out: List[dict] = []
        for f in rec.get("files") or []:
            name = f.get("key") or f.get("filename") or ""
            links = f.get("links") or {}
            url = links.get("self") or links.get("download") or f.get("links", {}).get("content")
            if name and url:
                out.append({"path": name, "source": "asset", "download_url": url})
        return out

    @staticmethod
    def html_url(rec: dict) -> str:
        return ((rec.get("links") or {}).get("html")
                or f"https://zenodo.org/records/{rec.get('id')}")

    @staticmethod
    def version(rec: dict) -> str:
        """The deposit's own version string, if it declares one.

        Deliberately does NOT fall back to ``revision`` (Zenodo's internal edit
        counter, e.g. "5"), which reads like a version but means nothing to a
        curator; callers fall back to the version DOI instead.
        """
        return str((rec.get("metadata") or {}).get("version") or "")

    @staticmethod
    def doi(rec: dict) -> str:
        return str((rec.get("metadata") or {}).get("doi") or rec.get("doi") or "")

    @staticmethod
    def published_at(rec: dict) -> str:
        return str(rec.get("created") or (rec.get("metadata") or {}).get("publication_date") or "")
