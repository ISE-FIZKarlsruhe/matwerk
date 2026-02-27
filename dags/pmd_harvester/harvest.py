# dags/pmd_harvester/harvest.py
from __future__ import annotations

import argparse
import csv
import json
import os
import time
from urllib.parse import urlparse

import requests


ALLOWED_EXTENSIONS = [".ttl", ".json", ".rdf", ".owl"]


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "CKAN-Harvester"})
    return s


def search_datasets(session: requests.Session, base_url: str, search_endpoint: str, query: str, start: int, rows: int):
    url = base_url.rstrip("/") + search_endpoint
    params = {"q": query, "rows": rows, "start": start}
    r = session.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data["result"]


def save_metadata(metadata_path: str, dataset: dict):
    # jsonl
    with open(metadata_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(dataset, ensure_ascii=False) + "\n")


def is_allowed_file(resource: dict) -> bool:
    fmt = (resource.get("format") or "").lower().strip()
    if fmt in ["ttl", "turtle", "json", "rdf", "owl"]:
        return True

    file_url = resource.get("url") or ""
    path = urlparse(file_url).path.lower()
    return any(path.endswith(ext) for ext in ALLOWED_EXTENSIONS)


def download_resource(session: requests.Session, files_dir: str, resource: dict):
    if not is_allowed_file(resource):
        return

    file_url = resource.get("url")
    if not file_url:
        return

    filename = os.path.basename(urlparse(file_url).path)
    if not filename:
        filename = resource.get("id") or "resource"

    filepath = os.path.join(files_dir, filename)

    try:
        with session.get(file_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print("Downloaded:", filename)
    except Exception as e:
        print(f"Download failed for {file_url}: {e}")


def harvest(
    out_dir: str,
    base_url: str,
    search_endpoint: str,
    query: str,
    max_datasets: int,
    rows_per_page: int,
    sleep_seconds: float,
    download_files: bool,
):
    os.makedirs(out_dir, exist_ok=True)
    files_dir = os.path.join(out_dir, "files")
    if download_files:
        os.makedirs(files_dir, exist_ok=True)

    metadata_path = os.path.join(out_dir, "metadata.jsonl")

    session = build_session()

    start = 0
    harvested = 0

    while harvested < max_datasets:
        result = search_datasets(session, base_url, search_endpoint, query, start, rows_per_page)
        datasets = result.get("results") or []
        total = int(result.get("count") or 0)

        if not datasets:
            break

        for ds in datasets:
            print("Harvesting:", ds.get("title") or ds.get("id") or "<no-title>")
            save_metadata(metadata_path, ds)

            if download_files:
                for res in ds.get("resources", []) or []:
                    download_resource(session, files_dir, res)

            harvested += 1
            if harvested >= max_datasets:
                break

        start += rows_per_page
        time.sleep(sleep_seconds)

        if start >= total:
            break

    print("Harvest complete. Total datasets:", harvested)
    return metadata_path


def get_full_metadata(metadata_path: str, out_csv_path: str):
    os.makedirs(os.path.dirname(out_csv_path), exist_ok=True)

    with open(metadata_path, "r", encoding="utf-8") as infile, open(out_csv_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)

        writer.writerow([
            "id",
            "title",
            "description",
            "license_id",
            "metadata_created",
            "metadata_modified",
            "type",
            "contact_name",
            "contact_email",
            "contributor_name",
            "contributor_email",
            "publisher_name",
            "publisher_email",
            "publisher_uri",
            "organization_id",
            "organization_title",
            "organization_description",
            "resource_count",
            "resource_formats",
            "resource_urls",
            "tag_names",
        ])

        for line in infile:
            try:
                ds = json.loads(line)

                dataset_id = ds.get("id", "") or ""
                title = ds.get("title", "") or ""
                notes = ds.get("notes", "") or ""
                license_id = ds.get("license_id", "") or ""
                metadata_created = ds.get("metadata_created", "") or ""
                metadata_modified = ds.get("metadata_modified", "") or ""
                dataset_type = ds.get("type", "") or ""

                contact = ds.get("contact", []) or []
                contact_name = "; ".join((t.get("name") or "") for t in contact)
                contact_email = "; ".join((t.get("email") or "") for t in contact)

                contributor = ds.get("contributor", []) or []
                contributor_name = "; ".join((t.get("name") or "") for t in contributor)
                contributor_email = "; ".join((t.get("email") or "") for t in contributor)

                publisher = ds.get("publisher", []) or []
                publisher_name = "; ".join((t.get("name") or "") for t in publisher)
                publisher_email = "; ".join((t.get("email") or "") for t in publisher)
                publisher_uri = "; ".join((t.get("uri") or "") for t in publisher)

                org = ds.get("organization", {}) or {}
                org_id = org.get("id", "") or ""
                org_title = org.get("title", "") or ""
                org_description = org.get("description", "") or ""

                tags = ds.get("tags", []) or []
                tag_names = "; ".join((t.get("name") or "") for t in tags)

                resources = ds.get("resources", []) or []
                resource_count = len(resources)
                resource_formats = "; ".join((r.get("format") or "") for r in resources)
                resource_urls = "; ".join((r.get("url") or "") for r in resources)

                writer.writerow([
                    dataset_id,
                    title,
                    notes,
                    license_id,
                    metadata_created,
                    metadata_modified,
                    dataset_type,
                    contact_name,
                    contact_email,
                    contributor_name,
                    contributor_email,
                    publisher_name,
                    publisher_email,
                    publisher_uri,
                    org_id,
                    org_title,
                    org_description,
                    resource_count,
                    resource_formats,
                    resource_urls,
                    tag_names,
                ])

            except Exception:
                continue

    print("Full metadata extraction complete:", out_csv_path)


def main(argv: list[str] | None = None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True, help="Output directory for this run (will contain metadata.jsonl, full_metadata.csv, files/)")
    ap.add_argument("--base-url", default="https://dataportal.material-digital.de")
    ap.add_argument("--search-endpoint", default="/api/3/action/package_search")
    ap.add_argument("--query", default="")
    ap.add_argument("--max-datasets", type=int, default=1000)
    ap.add_argument("--rows-per-page", type=int, default=100)
    ap.add_argument("--sleep-seconds", type=float, default=0.2)
    ap.add_argument("--download-files", action="store_true", help="If set, download allowed resources into out-dir/files/")
    args = ap.parse_args(argv)

    metadata_path = harvest(
        out_dir=args.out_dir,
        base_url=args.base_url,
        search_endpoint=args.search_endpoint,
        query=args.query,
        max_datasets=args.max_datasets,
        rows_per_page=args.rows_per_page,
        sleep_seconds=args.sleep_seconds,
        download_files=args.download_files,
    )

    out_csv_path = os.path.join(args.out_dir, "full_metadata.csv")
    get_full_metadata(metadata_path, out_csv_path)


if __name__ == "__main__":
    main()