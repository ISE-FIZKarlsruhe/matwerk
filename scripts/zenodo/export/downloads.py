import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests
import os

RDF_EXTS = {".ttl", ".trig", ".nt", ".nq", ".n3", ".owl", ".rdf", ".xml", ".jsonld"}
ZIP_EXTS = {".zip"}

# ---- Size cap: 500 MB (in bytes) ----
MAX_FILE_BYTES = 500 * 1024 * 1024

# ---- Helpers ----
def should_fetch_rdf_like(file_key: Optional[str], url: Optional[str]) -> bool:
    name = (file_key or (Path(url).name if url else "")).lower()
    return any(name.endswith(ext) for ext in RDF_EXTS)

def is_zip(file_key: Optional[str], url: Optional[str]) -> bool:
    name = (file_key or (Path(url).name if url else "")).lower()
    return any(name.endswith(ext) for ext in ZIP_EXTS)

def _content_length_too_big(headers: dict, max_bytes: int) -> bool:
    try:
        cl = headers.get("Content-Length")
        if cl is None:
            return False
        return int(cl) > max_bytes
    except Exception:
        return False

def _head_allows_skip(url: str, headers: dict, max_bytes: int, timeout: int = 30) -> bool:
    """
    Try a HEAD first to avoid downloading very large files.
    If HEAD fails or has no Content-Length, we won't skip here (streaming still enforces the cap).
    """
    try:
        rh = requests.head(url, headers=headers, allow_redirects=True, timeout=timeout)
        if rh.ok and _content_length_too_big(rh.headers, max_bytes):
            return True
    except Exception:
        pass
    return False

def _stream_download_with_cap(resp: requests.Response, outpath: Path, max_bytes: int) -> Tuple[bool, int]:
    """
    Stream response body to file, enforcing a size cap.
    Returns (ok, bytes_written). If cap exceeded, deletes partial file and returns (False, bytes_written).
    """
    bytes_written = 0
    tmp_path = outpath.with_suffix(outpath.suffix + ".part")
    try:
        with open(tmp_path, "wb") as fp:
            for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):  # 4 MB chunks
                if not chunk:
                    continue
                bytes_written += len(chunk)
                if bytes_written > max_bytes:
                    # abort and cleanup
                    resp.close()
                    fp.flush()
                    fp.close()
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    return False, bytes_written
                fp.write(chunk)
        # atomic-ish move
        tmp_path.replace(outpath)
        return True, bytes_written
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return False, bytes_written

def _safe_extract_member_path(base_dir: Path, member_name: str) -> Optional[Path]:
    """
    Prevent zip-slip: ensure the resolved path stays within base_dir.
    Returns the safe target path or None if unsafe.
    """
    target = (base_dir / member_name).resolve()
    base = base_dir.resolve()
    if not str(target).startswith(str(base) + os.sep) and target != base:
        return None
    return target

# ---- Public API ----
def download_rdf_files(
    files: List[Dict[str, str]], dest: Path, token: Optional[str]
) -> List[Tuple[Path, Optional[str], Dict[str, str]]]:
    """
    Download standalone RDF-like files, skipping anything > MAX_FILE_BYTES.
    Returns: [(local_path, content_type, file_meta)]
    """
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

        # Skip early if HEAD says too big
        if _head_allows_skip(url, headers, MAX_FILE_BYTES):
            print(f"  - Skip (size>500MB by HEAD): {url}")
            continue

        try:
            with requests.get(url, headers=headers, stream=True, timeout=300) as r:
                r.raise_for_status()

                # Skip early if GET headers show too big
                if _content_length_too_big(r.headers, MAX_FILE_BYTES):
                    print(f"  - Skip (size>500MB by GET headers): {url}")
                    continue

                ok, _bytes = _stream_download_with_cap(r, outpath, MAX_FILE_BYTES)
                if not ok:
                    print(f"  - Skip (stream exceeded 500MB): {url}")
                    continue

                ctype = r.headers.get("Content-Type")
                results.append((outpath, ctype, f))

        except Exception as e:
            print(f"  ! Failed to download {url}: {e}")

    return results

def download_rdf_and_zip_collect(
    files: List[Dict[str, str]], dest: Path, token: Optional[str]
) -> List[Tuple[Path, Optional[str], Dict[str, str], Optional[str]]]:
    """
    Download RDF-like files and ZIPs (extract inner RDF).
    Enforces MAX_FILE_BYTES:
      - Skip direct RDF files > 500 MB (header or stream).
      - Skip ZIPs > 500 MB (header or stream).
      - Skip INNER RDF entries > 500 MB (by ZipInfo.file_size).
    Returns: [(local_path, content_type, file_meta, inner_name_or_None)]
    """
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

            if _head_allows_skip(url, headers, MAX_FILE_BYTES):
                print(f"  - Skip RDF (size>500MB by HEAD): {url}")
                continue

            try:
                with requests.get(url, headers=headers, stream=True, timeout=300) as r:
                    r.raise_for_status()

                    if _content_length_too_big(r.headers, MAX_FILE_BYTES):
                        print(f"  - Skip RDF (size>500MB by GET headers): {url}")
                        continue

                    ok, _bytes = _stream_download_with_cap(r, outpath, MAX_FILE_BYTES)
                    if not ok:
                        print(f"  - Skip RDF (stream exceeded 500MB): {url}")
                        continue

                    ctype = r.headers.get("Content-Type")
                    results.append((outpath, ctype, f, None))
            except Exception as e:
                print(f"  ! Failed to download {url}: {e}")
            continue

        # ZIP containing possible RDF
        if is_zip(key, url):
            zpath = dest / key

            if _head_allows_skip(url, headers, MAX_FILE_BYTES):
                print(f"  - Skip ZIP (size>500MB by HEAD): {url}")
                continue

            try:
                with requests.get(url, headers=headers, stream=True, timeout=600) as r:
                    r.raise_for_status()

                    if _content_length_too_big(r.headers, MAX_FILE_BYTES):
                        print(f"  - Skip ZIP (size>500MB by GET headers): {url}")
                        continue

                    ok, _bytes = _stream_download_with_cap(r, zpath, MAX_FILE_BYTES)
                    if not ok:
                        print(f"  - Skip ZIP (stream exceeded 500MB): {url}")
                        continue

                extract_dir = dest / (key + ".extracted")
                extract_dir.mkdir(parents=True, exist_ok=True)

                with zipfile.ZipFile(zpath, "r") as zf:
                    for member in zf.infolist():
                        inner_name = member.filename
                        # Skip directories
                        if inner_name.endswith("/"):
                            continue
                        lower = inner_name.lower()
                        if not any(lower.endswith(ext) for ext in RDF_EXTS):
                            continue
                        # Size guard for inner files
                        if member.file_size and member.file_size > MAX_FILE_BYTES:
                            print(f"  - Skip inner RDF (>{MAX_FILE_BYTES}B): {inner_name} in {key}")
                            continue
                        # Path traversal guard
                        target_path = _safe_extract_member_path(extract_dir, inner_name)
                        if target_path is None:
                            print(f"  ! Unsafe ZIP path skipped: {inner_name}")
                            continue
                        target_path.parent.mkdir(parents=True, exist_ok=True)
                        # Extract in a streaming-friendly way
                        with zf.open(member) as src, open(target_path, "wb") as dst:
                            # read in chunks, but we already checked file_size
                            for chunk in iter(lambda: src.read(4 * 1024 * 1024), b""):
                                dst.write(chunk)

                        # No reliable content-type for inner files; let importer guess by extension
                        results.append((target_path, None, f, inner_name))

            except Exception as e:
                print(f"  ! Failed to download/extract ZIP {url}: {e}")

    return results
