import json
import os
from datetime import datetime


def define_env(env):
    """mkdocs-macros hook: load docs/dumps.json and compute derived stats."""
    dumps_path = os.path.join(os.path.dirname(__file__), "dumps.json")
    try:
        with open(dumps_path, "r", encoding="utf-8") as f:
            dumps = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        dumps = {"latest_version": None, "releases": []}

    env.variables["dumps"] = dumps

    # Compute derived variables from the latest release
    if dumps.get("releases"):
        latest = dumps["releases"][0]
        total = sum(g["stats"]["triples"] for g in latest.get("graphs", []))

        # Human-friendly triple count (e.g. "~234K")
        if total >= 1_000_000:
            triples_display = f"~{total / 1_000_000:.1f}M"
        elif total >= 1_000:
            triples_display = f"~{total // 1_000}K"
        else:
            triples_display = str(total)

        # Format date as DD.MM.YYYY from YYYY-MM-DD
        raw_date = latest.get("publish_date", "")
        try:
            dt = datetime.strptime(raw_date, "%Y-%m-%d")
            date_display = dt.strftime("%d.%m.%Y")
        except ValueError:
            date_display = raw_date

        env.variables["kg_version"] = latest.get("version", "")
        env.variables["kg_date"] = date_display
        env.variables["kg_triples"] = triples_display
        env.variables["kg_triples_exact"] = f"{total:,}"
        env.variables["kg_zenodo_doi"] = latest.get("zenodo_doi", "")
        env.variables["kg_zenodo_url"] = latest.get("zenodo_url", "")
        env.variables["kg_mwo_version"] = latest.get("mwo_version", "")
        env.variables["kg_graph_count"] = len(latest.get("graphs", []))
    else:
        env.variables["kg_version"] = ""
        env.variables["kg_date"] = ""
        env.variables["kg_triples"] = ""
        env.variables["kg_triples_exact"] = ""
        env.variables["kg_zenodo_doi"] = ""
        env.variables["kg_zenodo_url"] = ""
        env.variables["kg_mwo_version"] = ""
        env.variables["kg_graph_count"] = 0
