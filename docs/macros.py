import json
import os
import re
from datetime import datetime
from urllib.parse import quote, urlparse


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


# --------------------------------------------------------------------------- #
# Run links for SPARQL code fences
# --------------------------------------------------------------------------- #
# Up to shmarql v0.72 the documentation was rendered by shmarql itself, which
# gave every ```sparql fence a run link. The site is now built by mkdocs and
# served as static HTML, so shmarql never sees these pages and the fences are
# plain highlighted text -- a reader has to copy the query and paste it into the
# editor. This hook puts the affordance back at build time: the editor accepts a
# ?query= parameter and prefills from it, so each fence gets a link that opens
# the query already loaded.

_FENCE_OPEN = re.compile(r"^(?P<indent>[ \t]*)(?P<ticks>`{3,})[ \t]*sparql\b", re.I)
_FENCE_CLOSE = re.compile(r"^[ \t]*`{3,}[ \t]*$")

# The real limit is the encoded request line, not the query text: percent-encoding
# roughly doubles a SPARQL query. nginx allows 8 KB by default, so cap the finished
# URL well below that. A query that would exceed it gets no link rather than one
# that fails at the proxy.
_MAX_URL_CHARS = 6000

RUN_LINK_LABEL = "Run this query"


def _editor_path(env) -> str:
    """Path of the query editor, taken from site_url so the prefix is not fixed here."""
    site_url = (env.conf.get("site_url") or "") if hasattr(env, "conf") else ""
    base = urlparse(site_url).path.rstrip("/")
    return (base or "") + "/query"


def _add_run_links(markdown: str, editor: str) -> str:
    out = []
    lines = markdown.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        opening = _FENCE_OPEN.match(line)
        if not opening:
            out.append(line)
            i += 1
            continue

        body = []
        out.append(line)
        i += 1
        while i < len(lines) and not _FENCE_CLOSE.match(lines[i]):
            body.append(lines[i])
            out.append(lines[i])
            i += 1
        if i < len(lines):          # the closing fence
            out.append(lines[i])
            i += 1

        query = "\n".join(body).strip()
        already_linked = i < len(lines) and RUN_LINK_LABEL in lines[i]
        if query and not already_linked:
            url = editor + "?query=" + quote(query, safe="")
            if len(url) <= _MAX_URL_CHARS:
                indent = opening.group("indent")
                out.append("")
                out.append(f'{indent}[{RUN_LINK_LABEL}]({url}){{ .sparql-run-link }}')
    return "\n".join(out)


def on_pre_page_macros(env):
    """Append a run link after every SPARQL fence, before macros are expanded."""
    try:
        env.markdown = _add_run_links(env.markdown, _editor_path(env))
    except Exception:
        # A documentation convenience must never fail the build.
        pass
