import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests

RDF_EXTS = {".ttl", ".trig", ".nt", ".nq", ".n3", ".owl", ".rdf", ".xml", ".jsonld"}
ZIP_EXTS = {".zip"}

def should_fetch_rdf_like(file_key: Optional[str], url: Optional[str]) -> bool:
    name = (file_key or (Path(url).name if url else "")).lower()
    return any(name.endswith(ext) for ext in RDF_EXTS)

def is_zip(file_key: Optional[str], url: Optional[str]) -> bool:
    name = (file_key or (Path(url).name if url else "")).lower()
    return any(name.endswith(ext) for ext in ZIP_EXTS)

def download_rdf_files(files: List[Dict[str, str]], dest: Path, token: Optional[str]) -> List[Tuple[Path, Optional[str], Dict[str, str]]]:
    dest.mkdir(parents=True, exist_ok=True)
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    results: List[Tuple[Path, Optional[str], Dict[str, str]]] = []
    for f in files:
        url = f.get("link")
        key = f.get("key")
        if not url or not key:
            continue
        if not should_fetch_rdf_like(key, url):
            continue
        outpath = dest / key
        try:
            with requests.get(url, headers=headers, stream=True, timeout=300) as r:
                r.raise_for_status()
                ctype = r.headers.get("Content-Type")
                with open(outpath, "wb") as fp:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            fp.write(chunk)
            results.append((outpath, ctype, f))
        except Exception as e:
            print(f"  ! Failed to download {url}: {e}")
    return results

def download_rdf_and_zip_collect(files: List[Dict[str, str]], dest: Path, token: Optional[str]):
    """Download RDF-like files and ZIPs (extract inner RDF). 
    Returns list of (local_path, content_type, file_meta, inner_name_or_None)."""
    dest.mkdir(parents=True, exist_ok=True)
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    results: List[Tuple[Path, Optional[str], Dict[str, str], Optional[str]]] = []

    for f in files:
        url = f.get("link")
        key = f.get("key")
        if not url or not key:
            continue

        # Direct RDF
        if should_fetch_rdf_like(key, url):
            outpath = dest / key
            try:
                with requests.get(url, headers=headers, stream=True, timeout=300) as r:
                    r.raise_for_status()
                    ctype = r.headers.get("Content-Type")
                    with open(outpath, "wb") as fp:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                fp.write(chunk)
                results.append((outpath, ctype, f, None))
            except Exception as e:
                print(f"  ! Failed to download {url}: {e}")
            continue

        # ZIP containing possible RDF
        if is_zip(key, url):
            zpath = dest / key
            try:
                with requests.get(url, headers=headers, stream=True, timeout=300) as r:
                    r.raise_for_status()
                    with open(zpath, "wb") as fp:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                fp.write(chunk)
                extract_dir = dest / (key + ".extracted")
                extract_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(zpath, "r") as zf:
                    for member in zf.infolist():
                        inner_name = member.filename
                        if inner_name.endswith("/"):
                            continue
                        lower = inner_name.lower()
                        if not any(lower.endswith(ext) for ext in RDF_EXTS):
                            continue
                        target_path = extract_dir / inner_name
                        target_path.parent.mkdir(parents=True, exist_ok=True)
                        with zf.open(member) as src, open(target_path, "wb") as dst:
                            dst.write(src.read())
                        # No reliable content-type for inner files; let importer guess by extension
                        results.append((target_path, None, f, inner_name))
            except Exception as e:
                print(f"  ! Failed to download/extract ZIP {url}: {e}")
    return results
