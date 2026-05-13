from __future__ import annotations

import hashlib
import os
import re
import time

import requests
from requests.auth import HTTPDigestAuth

from airflow.sdk import Variable
from airflow.exceptions import AirflowFailException



def _get_virtuoso_config() -> tuple[str, str, str, str]:
    vcrud = Variable.get("matwerk-virtuoso_crud")
    vsparql = Variable.get("matwerk-virtuoso_sparql")
    vuser = Variable.get("matwerk-virtuoso_user")
    vpass = Variable.get("matwerk-virtuoso_pass")

    if not (vcrud and vsparql and vuser and vpass):
        raise AirflowFailException("Missing virtuoso_* variables (crud/sparql/user/pass)")
    return vcrud, vsparql, vuser, vpass


def _read_int_var(name: str, default: int) -> int:
    try:
        v = Variable.get(name)
    except Exception:
        return default
    try:
        return int(v)
    except Exception:
        return default


def drop_graph(graph: str, timeout: int = 60) -> None:
    _, vsparql, vuser, vpass = _get_virtuoso_config()
    auth = HTTPDigestAuth(vuser, vpass)

    q = f"drop silent graph <{graph}>"
    url = (
        vsparql.rstrip("/")
        + "?default-graph-uri=&query="
        + requests.utils.quote(q)
        + "&format=text%2Fhtml&timeout=0&signal_void=on"
    )

    r = requests.get(url, auth=auth, timeout=timeout)
    print("DROP status:", r.status_code)
    if r.status_code != 200:
        print(r.text[:1000])
        raise AirflowFailException("DROP GRAPH failed")


def upload_ttl_bytes(graph: str, ttl_bytes: bytes, timeout: tuple[int, int] = (10, 600)) -> None:
    vcrud, _, vuser, vpass = _get_virtuoso_config()
    auth = HTTPDigestAuth(vuser, vpass)

    url = vcrud.rstrip("/") + "?graph=" + requests.utils.quote(graph, safe="")
    r = requests.post(
        url,
        data=ttl_bytes,
        headers={"Content-Type": "text/turtle"},
        auth=auth,
        timeout=timeout,
    )
    print("UPLOAD status:", r.status_code)
    if r.status_code not in (200, 201, 204):
        print(r.text[:1000])
        raise AirflowFailException("Virtuoso upload failed")


def upload_ttl_bytes_chunked(
    graph: str,
    ttl_bytes: bytes,
    *,
    chunk_size_bytes: int,
    timeout: tuple[int, int] = (10, 600),
    retries: int = 5,
    retry_sleep_s: float = 2.0,
) -> dict:
    """
    Upload ttl_bytes in chunks. This assumes ttl_bytes already represents valid Turtle
    per chunk. The caller is responsible for safe chunk boundaries.
    """
    results = {"chunks": 0, "bytes_total": len(ttl_bytes), "statuses": []}

    # Degenerate case: small payload
    if len(ttl_bytes) <= chunk_size_bytes:
        _upload_with_retry(graph, ttl_bytes, timeout=timeout, retries=retries, retry_sleep_s=retry_sleep_s, chunk_idx=1)
        results["chunks"] = 1
        results["statuses"].append("ok")
        return results

    offset = 0
    idx = 1
    while offset < len(ttl_bytes):
        part = ttl_bytes[offset : offset + chunk_size_bytes]
        _upload_with_retry(graph, part, timeout=timeout, retries=retries, retry_sleep_s=retry_sleep_s, chunk_idx=idx)
        results["chunks"] += 1
        results["statuses"].append("ok")
        offset += chunk_size_bytes
        idx += 1

    return results


def _upload_with_retry(
    graph: str,
    chunk: bytes,
    *,
    timeout: tuple[int, int],
    retries: int,
    retry_sleep_s: float,
    chunk_idx: int,
) -> None:
    """
    Wrap upload_ttl_bytes with retries
    """
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            print(f"[INFO] Uploading chunk {chunk_idx} attempt {attempt} bytes={len(chunk)}")
            upload_ttl_bytes(graph, chunk, timeout=timeout)
            return
        except Exception as e:
            last_err = e
            print(f"[WARN] Chunk {chunk_idx} attempt {attempt} failed: {repr(e)}")
            if attempt < retries:
                time.sleep(retry_sleep_s)
    raise AirflowFailException(f"Chunk {chunk_idx} failed after {retries} retries: {repr(last_err)}")


def _iter_turtle_chunks_by_statement(
    ttl_path: str,
    *,
    target_chunk_bytes: int,
) -> list[bytes]:
    """
    Pragmatic Turtle chunker:
    - preserves @prefix/@base header lines and repeats them at the top of each chunk
    - only cuts after a line that ends with '.' (after stripping)
    - aims to keep each chunk under target_chunk_bytes (approx)

    This is designed for ROBOT-generated Turtle where statements are usually dot-terminated per line.
    For maximal robustness, consider publishing N-Triples (.nt) instead and chunk by lines.
    """
    with open(ttl_path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    header: list[str] = []
    body_start = 0
    for i, line in enumerate(lines):
        s = line.strip()
        if s.startswith("@prefix") or s.startswith("@base") or s == "":
            header.append(line)
            body_start = i + 1
            continue
        # stop at first non-header line
        body_start = i
        break

    chunks: list[bytes] = []
    current: list[str] = header.copy()
    current_bytes = sum(len(x.encode("utf-8")) for x in current)

    # if there is no body, still return one chunk (header only) to fail clearly on upload
    if body_start >= len(lines):
        chunks.append("".join(current).encode("utf-8"))
        return chunks

    for line in lines[body_start:]:
        current.append(line)
        current_bytes += len(line.encode("utf-8"))

        # only break chunks on likely statement boundary
        is_boundary = line.strip().endswith(".")
        if is_boundary and current_bytes >= target_chunk_bytes:
            chunks.append("".join(current).encode("utf-8"))
            current = header.copy()
            current_bytes = sum(len(x.encode("utf-8")) for x in current)

    # flush remainder
    if len(current) > len(header):
        chunks.append("".join(current).encode("utf-8"))
    else:
        # edge case: no body lines appended, still emit header-only chunk
        chunks.append("".join(current).encode("utf-8"))

    return chunks


def upload_ttl_file(
    graph: str,
    ttl_path: str,
    delete_first: bool = True,
    *,
    # Chunking knobs:
    chunk_upload: bool = True,
    # Default chunk size (bytes). You can override via Airflow Variable virtuoso_chunk_bytes.
    chunk_bytes_default: int = 5 * 1024 * 1024,  # 5 MiB
    # Retry knobs:
    retries: int = 5,
    retry_sleep_s: float = 2.0,
    timeout: tuple[int, int] = (10, 600),
) -> dict:
    """
    Upload a TTL file to Virtuoso with optional chunking.

    Returns a dict containing basic stats (chunk count etc.) which callers can store in reports.
    """
    if not os.path.exists(ttl_path):
        raise AirflowFailException(f"TTL file not found: {ttl_path}")
    if os.path.getsize(ttl_path) == 0:
        raise AirflowFailException(f"TTL file empty: {ttl_path}")

    if delete_first:
        print("Dropping graph:", graph)
        drop_graph(graph)

    # Resolve chunk size from Variable if set
    chunk_size_bytes = _read_int_var("virtuoso_chunk_bytes", chunk_bytes_default)

    print("Uploading TTL to graph:", graph)
    print(f"[INFO] ttl_path={ttl_path} size_bytes={os.path.getsize(ttl_path)}")
    print(f"[INFO] chunk_upload={chunk_upload} chunk_size_bytes={chunk_size_bytes}")

    if not chunk_upload:
        with open(ttl_path, "rb") as f:
            data = f.read()
        _upload_with_retry(graph, data, timeout=timeout, retries=retries, retry_sleep_s=retry_sleep_s, chunk_idx=1)
        print("Virtuoso load done (single request)")
        return {"chunks": 1, "bytes_total": os.path.getsize(ttl_path), "mode": "single"}

    turtle_chunks = _iter_turtle_chunks_by_statement(ttl_path, target_chunk_bytes=chunk_size_bytes)

    print(f"[INFO] Prepared {len(turtle_chunks)} turtle chunks")
    stats = {"chunks": 0, "bytes_total": os.path.getsize(ttl_path), "mode": "turtle-dotline", "chunk_bytes": chunk_size_bytes}

    for idx, chunk in enumerate(turtle_chunks, start=1):
        _upload_with_retry(graph, chunk, timeout=timeout, retries=retries, retry_sleep_s=retry_sleep_s, chunk_idx=idx)
        stats["chunks"] += 1

    print("Virtuoso load done (chunked)")
    return stats


# -----------------------------------------------------------------------------
# Per-named-graph publishing for TriG inputs (zenodo)
# -----------------------------------------------------------------------------

_GRAPH_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_filename_for_graph(graph_iri: str, max_len: int = 60) -> str:
    base = _GRAPH_SAFE_RE.sub("_", graph_iri).strip("_")[:max_len]
    digest = hashlib.sha1(graph_iri.encode("utf-8")).hexdigest()[:10]
    return f"{base}_{digest}" if base else digest


def publish_trig_per_graph(
    trig_path: str,
    *,
    workdir: str,
    delete_first: bool = True,
    default_graph_iri: str | None = None,
) -> dict:
    """
    Parse a TriG file and upload each named graph to Virtuoso under a graph IRI
    equal to the named-graph IRI itself. Each context is materialised as a TTL
    file in `workdir` (so it can be re-uploaded later if needed) and pushed via
    the existing `upload_ttl_file` chunked uploader.

    If `default_graph_iri` is provided, the TriG's *default* graph (which holds
    e.g. the harvester's BFO/MWO provenance about each named graph) is uploaded
    to that graph IRI as well. When `default_graph_iri` is None the default
    graph contents are not published.

    Returns a report dict suitable for embedding into publish_report.json.
    """
    from rdflib import Dataset, Graph

    if not os.path.exists(trig_path) or os.path.getsize(trig_path) == 0:
        raise AirflowFailException(f"TriG file missing/empty: {trig_path}")

    ds = Dataset()
    ds.parse(trig_path, format="trig")
    os.makedirs(workdir, exist_ok=True)

    results: list[dict] = []
    for ctx in ds.contexts():
        ctx_id = str(ctx.identifier)
        is_default = (
            ctx.identifier == ds.default_context.identifier
            or ctx_id.startswith("urn:x-rdflib:default")
        )
        if is_default:
            if default_graph_iri is None:
                continue
            target_iri = default_graph_iri
        else:
            target_iri = ctx_id

        # Materialise this context.
        flat = Graph()
        for t in ctx:
            flat.add(t)
        n_triples = len(flat)
        if n_triples == 0:
            results.append({"graph": target_iri, "status": "skipped", "reason": "empty"})
            continue

        ttl_path = os.path.join(workdir, f"{_safe_filename_for_graph(target_iri)}.ttl")
        flat.serialize(destination=ttl_path, format="turtle")

        try:
            stats = upload_ttl_file(
                graph=target_iri,
                ttl_path=ttl_path,
                delete_first=delete_first,
            )
            results.append({
                "graph": target_iri,
                "status": "published",
                "triples": n_triples,
                "ttl_path": ttl_path,
                "stats": stats,
                "from_default_graph": is_default,
            })
        except Exception as e:
            results.append({
                "graph": target_iri,
                "status": "failed",
                "triples": n_triples,
                "ttl_path": ttl_path,
                "error": repr(e),
                "from_default_graph": is_default,
            })

    return {"trig_path": trig_path, "graphs": results}
