# rdf_harvester/api.py
"""Minimal GitHub REST client for the harvester.

Only the endpoints the harvester needs: releases, the git tree at a tag, and
raw/asset downloads. An optional token (Airflow Variable ``matwerk_github_token``
or env ``GITHUB_TOKEN``) lifts the 60/hour anonymous rate limit to 5000/hour,
which matters once more than a handful of repositories are registered.
"""
from __future__ import annotations

import os
import time
from typing import List, Optional

import requests

GITHUB_API = "https://api.github.com"
RAW_BASE = "https://raw.githubusercontent.com"


class GitHubClient:
    def __init__(self, token: Optional[str] = None, timeout: int = 60,
                 max_rate_wait_s: int = 90) -> None:
        self.timeout = timeout
        self.max_rate_wait_s = max_rate_wait_s
        self.s = requests.Session()
        self.s.headers.update({
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "matwerk-github-harvester",
        })
        token = token or os.environ.get("GITHUB_TOKEN") or ""
        if token:
            self.s.headers["Authorization"] = f"Bearer {token}"
        self.authenticated = bool(token)

    # -------- low level --------
    def _get(self, url: str, *, params: Optional[dict] = None, stream: bool = False) -> requests.Response:
        last: Optional[requests.Response] = None
        for attempt in range(4):
            r = self.s.get(url, params=params, timeout=self.timeout, stream=stream)
            last = r
            # primary rate limit: wait until reset if it is soon, else give up
            if r.status_code in (403, 429) and r.headers.get("X-RateLimit-Remaining") == "0":
                reset = int(r.headers.get("X-RateLimit-Reset", "0") or 0)
                wait = reset - int(time.time()) if reset else 15
                if 0 < wait <= self.max_rate_wait_s:
                    print(f"[github] rate-limited, sleeping {wait}s")
                    time.sleep(wait + 1)
                    continue
            # transient server errors: small backoff
            if r.status_code in (502, 503, 504):
                time.sleep(2 * (attempt + 1))
                continue
            return r
        return last  # type: ignore[return-value]

    def get_json(self, path_or_url: str, params: Optional[dict] = None):
        url = path_or_url if path_or_url.startswith("http") else f"{GITHUB_API}{path_or_url}"
        r = self._get(url, params=params)
        r.raise_for_status()
        return r.json()

    # -------- releases --------
    def list_releases(self, owner_repo: str) -> List[dict]:
        """All published releases, newest first (excludes drafts)."""
        try:
            data = self.get_json(f"/repos/{owner_repo}/releases", params={"per_page": 100})
        except requests.HTTPError as e:
            print(f"[github] releases fetch failed for {owner_repo}: {e}")
            return []
        return [r for r in data if not r.get("draft")] if isinstance(data, list) else []

    def latest_release(self, owner_repo: str) -> Optional[dict]:
        """The release GitHub marks 'latest' (newest non-prerelease); falls back
        to the newest published release if none is flagged latest."""
        try:
            return self.get_json(f"/repos/{owner_repo}/releases/latest")
        except requests.HTTPError:
            rels = self.list_releases(owner_repo)
            return rels[0] if rels else None

    # -------- tree at a ref (tag) --------
    def get_tree(self, owner_repo: str, ref: str) -> List[dict]:
        """Recursive git tree at ``ref`` (a tag/branch/sha). Returns the list of
        entries ({"path","type","sha"}); logs if GitHub truncated a huge tree."""
        try:
            data = self.get_json(f"/repos/{owner_repo}/git/trees/{ref}", params={"recursive": "1"})
        except requests.HTTPError as e:
            print(f"[github] tree fetch failed for {owner_repo}@{ref}: {e}")
            return []
        if data.get("truncated"):
            print(f"[github] WARNING: tree truncated for {owner_repo}@{ref}; some files may be missed")
        return data.get("tree", []) or []

    def raw_url(self, owner_repo: str, ref: str, path: str) -> str:
        return f"{RAW_BASE}/{owner_repo}/{ref}/{path}"

    # -------- downloads --------
    def download_bytes(self, url: str) -> bytes:
        r = self._get(url)
        r.raise_for_status()
        return r.content
