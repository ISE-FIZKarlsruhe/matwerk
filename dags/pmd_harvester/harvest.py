import requests
import os
import json
import time
import csv
from urllib.parse import urlparse

# ==========================
# CONFIGURATION
# ==========================

BASE_URL = "https://dataportal.material-digital.de"  # url of dataportal
SEARCH_ENDPOINT = "/api/3/action/package_search"

OUTPUT_DIR = "harvest_output"
METADATA_FILE = "metadata.json"
output_file = "full_metadata.csv"

QUERY = ""
MAX_DATASETS = 1000
ROWS_PER_PAGE = 100
SLEEP_SECONDS = 0.2

# Allowed file extensions
ALLOWED_EXTENSIONS = [".ttl", ".json", ".rdf", ".owl"]


# ==========================
# SETUP
# ==========================

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, "files"), exist_ok=True)

session = requests.Session()
session.headers.update({
    "User-Agent": "CKAN-Harvester"
})


# ==========================
# FUNCTIONS
# ==========================

def search_datasets(query="", start=0, rows=100):
    url = BASE_URL + SEARCH_ENDPOINT

    params = {
        "q": query,
        "rows": rows,
        "start": start
    }

    response = session.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    return data["result"]


def save_metadata(dataset):
    path = os.path.join(OUTPUT_DIR, METADATA_FILE)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(dataset) + "\n")


def is_allowed_file(resource):
    # Check format field
    format_name = resource.get("format", "").lower()
    if format_name in ["ttl", "turtle", "json", "rdf", "owl"]:
        return True

    # Check file extension from URL
    file_url = resource.get("url", "")
    path = urlparse(file_url).path.lower()

    for ext in ALLOWED_EXTENSIONS:
        if path.endswith(ext):
            return True

    return False


def download_resource(resource):
    if not is_allowed_file(resource):
        return

    file_url = resource.get("url")
    if not file_url:
        return

    filename = os.path.basename(urlparse(file_url).path)

    if not filename:
        filename = resource.get("id")

    filepath = os.path.join(OUTPUT_DIR, "files", filename)

    try:
        with session.get(file_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        print("Downloaded:", filename)

    except:
        pass


def harvest():
    start = 0
    harvested = 0

    while harvested < MAX_DATASETS:
        result = search_datasets(
            query=QUERY,
            start=start,
            rows=ROWS_PER_PAGE
        )

        datasets = result["results"]
        total = result["count"]

        if not datasets:
            break

        for dataset in datasets:
            print("Harvesting:", dataset.get("title"))

            # Save metadata
            save_metadata(dataset)

            # Download only requested format, i.e. TTL and JSON
            for resource in dataset.get("resources", []):
                download_resource(resource)

            harvested += 1
            if harvested >= MAX_DATASETS:
                break

        start += ROWS_PER_PAGE
        time.sleep(SLEEP_SECONDS)

        if start >= total:
            break

    print("Harvest complete. Total datasets:", harvested)


def get_full_metadata():
    path = os.path.join(OUTPUT_DIR, output_file)
    with open(OUTPUT_DIR + '/' + METADATA_FILE, "r", encoding="utf-8") as infile, \
      open(path, "w", newline="", encoding="utf-8") as outfile:

      writer = csv.writer(outfile)

    # CSV Header
      writer.writerow ([
        # Core dataset fields
        "id",
        "title",
       #  "name",
        "description",
       # "state",
       #  "private",
       #  "license_title",
        "license_id",
        "metadata_created",
        "metadata_modified",
       # "version",
        "type",

        #contact and creator 
        "contact_name",
        "contact_email",
        "contributor_name",
        "contributor_email",
        "publisher_name",
        "publisher_email",
        "publisher_uri",

        # Organization
        "organization_id",
        "organization_title",
        #"organization_name",
        "organization_description",

        # Statistics
        "resource_count",
        # "tag_count",
        # "semantic_resource_count",

        # Resource info (flattened)
        "resource_formats",
        "resource_urls",

        # Tags
        "tag_names",

        # Extras (flattened key-value pairs)
       # "extras"
    ])              

      for line in infile:
        try:
            ds = json.loads(line)

            # ------------------------
            # Core Dataset Fields
            # ------------------------
            dataset_id = ds.get("id", "")
            title = ds.get("title", "")
          #  name = ds.get("name", "")
            notes = ds.get("notes", "")
           # state = ds.get("state", "")
           # private = ds.get("private", "")
           # license_title = ds.get("license_title", "")
            license_id = ds.get("license_id", "")
            metadata_created = ds.get("metadata_created", "")
            metadata_modified = ds.get("metadata_modified", "")
           # version = ds.get("version", "")
            dataset_type = ds.get("type", "")

            # -------------------------
            # contact/contributor fields
            #--------------------------
            contact = ds.get("contact", []) or []
            contact_name =  "; ".join(t.get("name", "") for t in contact)
            contact_email = "; ".join(t.get("email", "") for t in contact)
            #------------- contributor
            contributor = ds.get("contributor", []) or []
            contributor_name =  "; ".join(t.get("name", "") for t in contributor)
            contributor_email = "; ".join(t.get("email", "") for t in contributor)

              #------------- contributor
            publisher = ds.get("publisher", []) or []
            publisher_name =  "; ".join(t.get("name", "") for t in publisher)
            publisher_email = "; ".join(t.get("email", "") for t in publisher)
            publisher_uri = "; ".join(t.get("uri", "") for t in publisher)

            # ------------------------
            # Organization
            # ------------------------
            org = ds.get("organization", {}) or {}

            org_id = org.get("id", "")
            org_title = org.get("title", "")
            org_name = org.get("name", "")
            org_description = org.get("description", "")

            # ------------------------
            # Tags
            # ------------------------
            tags = ds.get("tags", []) or []
            tag_names = "; ".join(t.get("name", "") for t in tags)
            tag_count = len(tags)

            # ------------------------
            # Resources
            # ------------------------
            resources = ds.get("resources", []) or []
            resource_count = len(resources)

            resource_formats = "; ".join(
                r.get("format", "") for r in resources
            )

            resource_urls = "; ".join(
                r.get("url", "") for r in resources
            )

            # Count semantic resources
            semantic_formats = ["ttl", "turtle", "json", "rdf", "xml", "jsonld"]
            semantic_resource_count = sum(
                1 for r in resources
                if r.get("format", "").lower() in semantic_formats
            )

            # ------------------------
            # Extras (Flatten Key=Value)
            # ------------------------
            extras = ds.get("extras", []) or []
            extras_dict = {
                e.get("key", ""): e.get("value", "")
                for e in extras
            }

            extras_flat = "; ".join(
                f"{k}={v}" for k, v in extras_dict.items()
            )

            # ------------------------
            # Write Row
            # ------------------------
            writer.writerow([
                dataset_id,
                title,
               # name,
                notes,
              #  state,
               # private,
               # license_title,
                license_id,
                metadata_created,
                metadata_modified,
              #  version,
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
              #  org_name,
                org_description,

                resource_count,
               # tag_count,
               # semantic_resource_count,

                resource_formats,
                resource_urls,

                tag_names
              #  extras_flat
            ])

        except Exception:
            continue

    print("Full metadata extraction complete.")

# ==========================
# RUN
# ==========================

if __name__ == "__main__":
    harvest()
    get_full_metadata()
