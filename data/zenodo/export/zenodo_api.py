import time
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse
import requests
from .constants import ZENODO_API, DEFAULT_PAGE_SIZE

def zenodo_request(url: str, params: Optional[dict] = None, token: Optional[str] = None) -> dict:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    r = requests.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

def normalize_community(community: str) -> str:
    c = community.strip()
    if c.startswith("http://") or c.startswith("https://"):
        path = urlparse(c).path.rstrip("/")
        parts = [p for p in path.split("/") if p]
        if parts and parts[-1] != "communities":
            return parts[-1]
    return c

def iter_community_records(community: str, token: Optional[str]) -> Iterable[dict]:
    page = 1
    total = None
    while True:
        params = {
            "communities": community,
            "size": DEFAULT_PAGE_SIZE,
            "page": page,
            "sort": "mostrecent",
            "all_versions": 1,
        }
        data = zenodo_request(ZENODO_API, params=params, token=token)
        hits = data.get("hits", {}).get("hits", [])
        if total is None:
            raw_total = data.get("hits", {}).get("total")
            total = raw_total.get("value") if isinstance(raw_total, dict) else raw_total
            print(f"Found ~{total} records in community '{community}'.")
        if not hits:
            break
        for rec in hits:
            yield rec
        page += 1
        time.sleep(0.3)

def get_metadata(rec: dict) -> dict:
    return rec.get("metadata", rec)

def get_doi(rec: dict) -> Optional[str]:
    meta = rec.get("metadata") or {}
    return meta.get("doi") or rec.get("doi")

def best_external_url(rec: dict) -> Optional[str]:
    links = rec.get("links", {}) or {}
    return links.get("doi") or links.get("record_html") or links.get("self") or (f"https://zenodo.org/records/{rec.get('id')}" if rec.get("id") else None)

def extract_files(rec: dict) -> List[Dict[str, str]]:
    files: List[Dict[str, str]] = []
    if isinstance(rec.get("files"), list):
        for f in rec["files"]:
            if not isinstance(f, dict):
                continue
            link = f.get("links", {}).get("self") or f.get("links", {}).get("download") or f.get("links", {}).get("content")
            files.append({
                "key": f.get("key") or f.get("filename"),
                "checksum": f.get("checksum"),
                "link": link,
            })
    elif isinstance(rec.get("files"), dict):
        entries = rec["files"].get("entries")
        if isinstance(entries, dict):
            entries = entries.values()
        if isinstance(entries, list):
            for f in entries:
                if not isinstance(f, dict):
                    continue
                links = f.get("links", {}) or {}
                link = links.get("content") or links.get("self")
                files.append({
                    "key": f.get("key") or f.get("id") or f.get("filename"),
                    "checksum": f.get("checksum"),
                    "link": link,
                })
    return files

def get_records_by_ids(ids, token: Optional[str] = None):
    for rid in ids:
        url = f"{ZENODO_API}/{rid}"
        try:
            yield zenodo_request(url, token=token)
        except Exception as e:
            print(f"! Failed to fetch record {rid}: {e}")
