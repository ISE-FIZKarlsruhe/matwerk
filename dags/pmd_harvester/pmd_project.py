import requests
from bs4 import BeautifulSoup
from langdetect import detect
from urllib.parse import urljoin, urlparse
import csv
import re
import time

BASE_URL = "https://www.material-digital.de"
PROJECTS_URL = f"{BASE_URL}/projects/"


# -----------------------------------
# Force English response
# -----------------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9"
}


# -----------------------------------
# Clean text
# -----------------------------------
def clean_text(text):

    if not text:
        return ""

    text = re.sub(r"\s+", " ", text)

    return text.strip()


# -----------------------------------
# Language detection
# -----------------------------------
def is_english(text):

    if not text or len(text) < 20:
        return False

    try:
        return detect(text) == "en"

    except:
        return False


# -----------------------------------
# Request helper
# -----------------------------------
def get_soup(url):

    response = requests.get(
        url,
        headers=HEADERS,
        timeout=30
    )

    response.raise_for_status()

    return BeautifulSoup(
        response.text,
        "html.parser"
    )


# -----------------------------------
# QName extraction
# -----------------------------------
def get_qname(url):

    parsed = urlparse(url)

    path = parsed.path.rstrip("/")

    return path.split("/")[-1]


# -----------------------------------
# Extract English text blocks
# -----------------------------------
def extract_english_blocks(soup):

    blocks = []

    for tag in soup.find_all([
        "h1",
        "h2",
        "h3",
        "p",
        "li"
    ]):

        text = clean_text(
            tag.get_text(" ")
        )

        if is_english(text):
            blocks.append(text)

    return blocks


# -----------------------------------
# Extract tasks + locations
# -----------------------------------
# -----------------------------------
# Extract tasks + locations
# Split first column into:
#   organization
#   task
# using newline separator
# -----------------------------------
def extract_tasks_locations(soup):

    rows = []

    tables = soup.find_all("table")

    for table in tables:

        tr_list = table.find_all("tr")

        for tr in tr_list:

            cols = tr.find_all(["td", "th"])

            if len(cols) < 2:
                continue

            # -----------------------------------
            # First column:
            # organization + task
            # -----------------------------------
            raw_lines = list(
                cols[0].stripped_strings
            )

            # Remove empty lines
            raw_lines = [
                clean_text(x)
                for x in raw_lines
                if clean_text(x)
            ]

            organization = ""
            task = ""

            # -----------------------------------
            # Split by newline structure
            # -----------------------------------
            if len(raw_lines) >= 2:

                organization = raw_lines[0]

                task = " ".join(raw_lines[1:])

            elif len(raw_lines) == 1:

                organization = raw_lines[0]

            # -----------------------------------
            # Second column:
            # location
            # -----------------------------------
            location = clean_text(
                cols[1].get_text(" ")
            )

            # -----------------------------------
            # Keep only English rows
            # -----------------------------------
            combined_text = (
                organization + " " + task
            )

            if is_english(combined_text):
                
                rows.append({
                    "organization": organization,
                    "task": task,
                    "location": location
                })

    return rows

# -----------------------------------
# Get all project URLs
# -----------------------------------
def get_project_links():

    soup = get_soup(PROJECTS_URL)

    links = set()

    for a in soup.find_all("a", href=True):

        href = a["href"]

        full_url = urljoin(
            BASE_URL,
            href
        )

        if "/project/" in full_url:
            links.add(full_url.rstrip("/"))

    return sorted(links)


# -----------------------------------
# Extract project data
# -----------------------------------
def extract_project(project_url):

    soup = get_soup(project_url)

    qname = get_qname(project_url)

    # -----------------------------------
    # Project title
    # -----------------------------------
    title = ""

    h1 = soup.find("h1")

    if h1:

        candidate = clean_text(
            h1.get_text()
        )

        if is_english(candidate):
            title = candidate

    # -----------------------------------
    # English content
    # -----------------------------------
    english_blocks = extract_english_blocks(
        soup
    )

    # -----------------------------------
    # Tasks + locations
    # -----------------------------------
    task_rows = extract_tasks_locations(
        soup
    )

    # -----------------------------------
    # Build rows
    # -----------------------------------
    rows = []

    for task_row in task_rows:

        if(task_row["location"] != "Location"):

         rows.append({
            "project_name": qname,
            "project_title": title,
            "project_url": project_url,
            "organization": task_row["organization"],
            "task": task_row["task"],
            "location": task_row["location"]
          #  "english_content": " | ".join(
            #    english_blocks[:10]
           # )
         })

    # fallback if no tasks
    if not rows:

        rows.append({
            "qname": qname,
            "project_name": title,
            "project_url": project_url,
            "organization": "",
            "task": "",
            "location": ""
           # "english_content": " | ".join(
            #    english_blocks[:10]
            #)
        })

    return rows


# -----------------------------------
# Save CSV
# -----------------------------------
def save_csv(rows, filename):

    fieldnames = [
        "project_name",
        "project_title",
        "project_url",
        "organization",
        "task",
        "location",
       # "english_content"
    ]

    with open(
        filename,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(row)


# -----------------------------------
# Main
# -----------------------------------
#if __name__ == "__main__":
def main(args):

    # Clear CSV at each run
    open("data/pmd/pmd_projects_english.csv", "w").close()

    print("Extracting project links...")

    project_links = get_project_links()

    print(f"Found {len(project_links)} projects")

    all_rows = []

    for idx, url in enumerate(project_links, start=1):

        try:

            print(f"[{idx}/{len(project_links)}] {url}")

            rows = extract_project(url)

            all_rows.extend(rows)

            time.sleep(1)

        except Exception as e:

            print(f"ERROR: {url}")
            print(e)

    save_csv(
        all_rows,
        "data/pmd/pmd_projects_english.csv"
    )

    print("\nDONE")
    print(
        f"Saved {len(all_rows)} rows "
        f"to pmd_project_english.csv"
    )

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])