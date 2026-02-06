from __future__ import annotations

import os
import shutil
import subprocess
import tarfile
import tempfile
from typing import Optional

import requests

from airflow.exceptions import AirflowFailException


def run_cmd(cmd, cwd=None, env=None):
    p = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )
    if p.stdout:
        print(p.stdout, end="")
    if p.returncode != 0:
        if p.stderr:
            print(p.stderr, end="")
        raise AirflowFailException(f"Command failed rc={p.returncode}: {' '.join(cmd)}")
    return p

def github_list_dir(owner_repo: str, path: str, ref: str) -> list[dict]:
    url = f"https://api.github.com/repos/{owner_repo}/contents/{path}"
    r = requests.get(url, params={"ref": ref}, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise AirflowFailException(f"GitHub contents API returned non-list for {path}")
    return data


def github_download(download_url: str, out_path: str) -> None:
    r = requests.get(download_url, timeout=120)
    r.raise_for_status()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(r.content)
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        raise AirflowFailException(f"Downloaded file empty: {out_path}")


def download_github_dir(owner_repo: str, repo_path: str, ref: str, out_dir: str) -> None:
    """
    Recursively download a directory via GitHub contents API.
    """
    os.makedirs(out_dir, exist_ok=True)
    items = github_list_dir(owner_repo, repo_path, ref)
    for it in items:
        t = it.get("type")
        name = it.get("name", "")
        if not name:
            continue

        if t == "file":
            dl = it.get("download_url")
            if dl:
                github_download(dl, os.path.join(out_dir, name))

        elif t == "dir":
            download_github_dir(
                owner_repo,
                f"{repo_path}/{name}",
                ref,
                os.path.join(out_dir, name),
            )