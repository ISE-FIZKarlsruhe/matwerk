# rdf_harvester/run.py
"""Entry point for the registered-RDF-files harvester.

Reads the registration tab of the Google Sheet (GitHub repos / Zenodo records,
optionally naming which files to include), fetches the RDF, and writes:

  * ``rdf_files.ttl``          — TriG: one named graph per RDF file + a default
                                 metadata graph linking each file to its source
  * ``rdf_files_state.json``   — per-graph content hash + version for weekly
                                 change-detection (latest release/version wins)
  * ``rdf_files_summary.json`` — run report

Usable from Airflow (``run(...)``) and standalone, e.g.::

    python -m rdf_harvester.run --out-dir /tmp/out \
        --registration "github,https://github.com/Owner/repo,cto-full.ttl"
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from rdf_harvester.api import GitHubClient
    from rdf_harvester.discover import discover_repos_from_kg, normalize_owner_repo
    from rdf_harvester.harvest import harvest_github, harvest_zenodo
    from rdf_harvester.registry import Registration, fetch_registrations, infer_source
    from rdf_harvester.zenodo_api import ZenodoClient, record_id_from
    from rdf_harvester import graph_build as gb
else:
    from .api import GitHubClient
    from .discover import discover_repos_from_kg, normalize_owner_repo
    from .harvest import harvest_github, harvest_zenodo
    from .registry import Registration, fetch_registrations, infer_source
    from .zenodo_api import ZenodoClient, record_id_from
    from . import graph_build as gb


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _load_state(path: Optional[str]) -> Dict[str, dict]:
    if path and os.path.exists(path):
        try:
            return json.load(open(path, encoding="utf-8")).get("graphs", {})
        except Exception as e:  # noqa: BLE001
            print(f"[run] could not read state {path}: {e}")
    return {}


def run(
    *,
    out_dir: str,
    registrations: Optional[List[Registration]] = None,
    publish_id: str = "",
    gid: str = "",
    kg_ttl: Optional[str] = None,
    github_token: Optional[str] = None,
    zenodo_token: Optional[str] = None,
    state_path: Optional[str] = None,
    out_name: str = "rdf_files.ttl",
    mwo_version: Optional[str] = None,
) -> dict:
    """Harvest every registered RDF file into a per-named-graph TriG.

    Registrations come from the sheet tab (``publish_id`` + ``gid``) unless an
    explicit list is passed. ``kg_ttl`` is optional: when a registered repo is
    already an entity in the KG, its file-graphs are attached to that entity
    instead of to a freshly minted node.
    """
    os.makedirs(out_dir, exist_ok=True)
    harvested_at = _utc_now()
    state_path = state_path or os.path.join(out_dir, "rdf_files_state.json")
    prev_state = _load_state(state_path)

    if registrations is None:
        if not (publish_id and gid):
            raise ValueError("need registrations, or publish_id+gid of the registration tab")
        registrations = fetch_registrations(publish_id, gid)

    # KG entities keyed by owner/repo, so harvested graphs attach to what the KG knows
    kg_map: Dict[str, str] = discover_repos_from_kg(kg_ttl) if kg_ttl else {}

    gh = GitHubClient(token=github_token)
    zen = ZenodoClient(token=zenodo_token)
    print(f"[run] github auth: {'token' if gh.authenticated else 'anonymous (60/h)'}")

    ds = gb.Dataset()
    meta = gb.Graph()
    new_state: Dict[str, dict] = {}
    reports: List[dict] = []
    seen: set = set()

    for reg in registrations:
        print(f"[run] === row {reg.row}: {reg.source} {reg.url} "
              f"{'files=' + ','.join(reg.files) if reg.files else '(auto-select)'} ===")
        if reg.source == "github":
            owner_repo = normalize_owner_repo(reg.url)
            if not owner_repo:
                reports.append({"row": reg.row, "url": reg.url, "source": reg.source,
                                "key": None, "kg_iri": None, "status": "bad_url",
                                "version": None, "landing_url": None,
                                "note": "not an owner/repo github url", "files": []})
                continue
            sh = harvest_github(gh, owner_repo, reg.url, reg.files)
            kg_iri = kg_map.get(owner_repo)
        else:
            sh = harvest_zenodo(zen, record_id_from(reg.url) or reg.url, reg.url, reg.files)
            kg_iri = None

        file_infos: List[dict] = []
        if sh.status == "ok":
            file_infos = gb.add_source(ds, meta, sh, kg_iri, harvested_at, mwo_version=mwo_version)

        for fi in file_infos:
            g = fi["graph_iri"]
            seen.add(g)
            prior = prev_state.get(g)
            fi["change"] = ("new" if prior is None
                            else "changed" if prior.get("sha256") != fi["sha256"]
                            else "unchanged")
            new_state[g] = {"sha256": fi["sha256"], "version": fi["version"],
                            "harvested_at": harvested_at, "registered_from": reg.url}

        reports.append({
            "row": reg.row, "url": reg.url, "source": reg.source, "key": sh.key,
            "kg_iri": kg_iri, "status": sh.status, "version": sh.version,
            "landing_url": sh.landing_url, "note": sh.note, "files": file_infos,
        })

    removed = [g for g in prev_state if g not in seen]

    gb.finalize(ds, meta)
    out_ttl = os.path.join(out_dir, out_name)
    ds.serialize(destination=out_ttl, format="trig")

    with open(state_path, "w", encoding="utf-8") as f:
        json.dump({"harvested_at": harvested_at, "graphs": new_state}, f, indent=2)

    n_files = sum(len(r["files"]) for r in reports)
    summary = {
        "harvested_at": harvested_at,
        "trig": out_ttl,
        "metadata_graph_iri": gb.METADATA_GRAPH_IRI,
        "registrations": len(registrations),
        "registrations_with_rdf": sum(1 for r in reports if r["files"]),
        "named_graphs": n_files,
        "new": sum(1 for r in reports for f in r["files"] if f.get("change") == "new"),
        "changed": sum(1 for r in reports for f in r["files"] if f.get("change") == "changed"),
        "unchanged": sum(1 for r in reports for f in r["files"] if f.get("change") == "unchanged"),
        "orphaned_graphs": removed,
        "results": reports,
    }
    with open(os.path.join(out_dir, "rdf_files_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"[run] wrote {out_ttl}: {n_files} named graphs from "
          f"{summary['registrations_with_rdf']}/{len(registrations)} registrations "
          f"(new={summary['new']} changed={summary['changed']} unchanged={summary['unchanged']})")
    return summary


def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(description="Harvest registered RDF files (GitHub/Zenodo) into a TriG.")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--publish-id", default="", help="Google Sheet publish id (2PACX-...)")
    ap.add_argument("--gid", default="", help="gid of the registration tab")
    ap.add_argument("--kg-ttl", default="", help="merged KG TTL (attach graphs to known entities)")
    ap.add_argument("--registration", action="append", default=[],
                    help="manual row 'source,url[,file1;file2]' (repeatable; bypasses the sheet)")
    ap.add_argument("--github-token", default="")
    ap.add_argument("--zenodo-token", default="")
    ap.add_argument("--state", default="")
    ap.add_argument("--mwo-version", default="", help="ontology version for the classes written (default: matwerk_mwo_version)")
    args = ap.parse_args(argv)

    regs: Optional[List[Registration]] = None
    if args.registration:
        regs = []
        for i, spec in enumerate(args.registration, start=2):
            parts = [p.strip() for p in spec.split(",")]
            url = parts[1] if len(parts) > 1 and parts[1] else parts[0]
            src = (parts[0].lower() if len(parts) > 1 and parts[0] else "") or (infer_source(url) or "")
            files = [f for f in (parts[2].split(";") if len(parts) > 2 and parts[2] else []) if f]
            regs.append(Registration(source=src, url=url, files=files, row=i))

    run(
        out_dir=args.out_dir, registrations=regs,
        publish_id=args.publish_id, gid=args.gid,
        kg_ttl=args.kg_ttl or None,
        github_token=args.github_token or None,
        zenodo_token=args.zenodo_token or None,
        state_path=args.state or None,
        mwo_version=args.mwo_version or None,
    )


if __name__ == "__main__":
    main()
