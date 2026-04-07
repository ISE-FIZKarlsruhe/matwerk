#!/usr/bin/env python3
"""
One-time bootstrap script to create initial RDF dumps from the current Virtuoso state.

Usage:
    python bootstrap_dumps.py \
        --sparql   https://-/sparql \
        --user     dba \
        --pass     secret \
        --version  1.0.0 \
        --out-dir  ./bootstrap_dumps

This will:
  1. CONSTRUCT all triples from each named graph
  2. Save each as {graph_name}_v{version}.ttl
  3. Compute stats and write metadata TTL per graph
  4. Write a dumps.json manifest (copy to docs/dumps.json to seed the site)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import requests
from requests.auth import HTTPDigestAuth


GRAPH_ROOT = "https://nfdi.fiz-karlsruhe.de/matwerk"
MWO_VERSION = "3.0.1"
MWO_VERSION_IRI = f"http://purls.helmholtz-metadaten.de/mwo/mwo.owl/{MWO_VERSION}"

NAMED_GRAPHS = [
    "spreadsheets_assertions",
    "spreadsheets_inferences",
    "spreadsheets_validated",
    "zenodo_validated",
    "endpoints_validated",
]


def sparql_dump_ntriples(endpoint: str, graph_uri: str, user: str, passwd: str, page_size: int = 10000) -> str:
    """
    Dump all triples from a named graph as N-Triples using SELECT pagination.
    SELECT avoids the CONSTRUCT hash-dictionary limit in Virtuoso.
    """
    auth = HTTPDigestAuth(user, passwd)
    all_lines = []
    offset = 0
    total = 0

    while True:
        query = (
            f"SELECT ?s ?p ?o FROM <{graph_uri}> "
            f"WHERE {{ ?s ?p ?o }} "
            f"LIMIT {page_size} OFFSET {offset}"
        )
        r = requests.get(
            endpoint,
            params={"query": query, "format": "application/sparql-results+json"},
            auth=auth,
            timeout=(10, 600),
        )
        if r.status_code != 200:
            print(f"[ERROR] SELECT for {graph_uri} failed ({r.status_code}): {r.text[:500]}")
            return ""

        bindings = r.json().get("results", {}).get("bindings", [])
        if not bindings:
            break

        for b in bindings:
            s = _sparql_val_to_nt(b["s"])
            p = _sparql_val_to_nt(b["p"])
            o = _sparql_val_to_nt(b["o"])
            all_lines.append(f"{s} {p} {o} .")

        total += len(bindings)
        offset += page_size
        print(f"    fetched {total} triples so far...")

        if len(bindings) < page_size:
            break

    return "\n".join(all_lines) + "\n" if all_lines else ""


def _sparql_val_to_nt(val: dict) -> str:
    """Convert a SPARQL JSON result binding value to N-Triples syntax."""
    t = val["type"]
    v = val["value"]
    if t == "uri":
        return f"<{v}>"
    elif t == "bnode":
        # Virtuoso may return bnode values like "b1250563" or "nodeID://b1250563"
        bnode_id = v.replace("nodeID://", "").replace("://", "").lstrip("/")
        return f"_:{bnode_id}"
    elif t == "literal" or t == "typed-literal":
        # Escape special chars in literal value
        escaped = v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")
        lang = val.get("xml:lang")
        dt = val.get("datatype")
        if lang:
            return f'"{escaped}"@{lang}'
        elif dt:
            return f'"{escaped}"^^<{dt}>'
        else:
            return f'"{escaped}"'
    return f'"{v}"'


def compute_stats(ttl_path: str) -> dict:
    """Parse TTL and compute basic stats using rdflib."""
    from rdflib import Graph, RDF

    g = Graph()
    g.parse(ttl_path, format="turtle")

    subs, preds, objs, type_objs = set(), set(), set(), set()
    type_assertions = 0
    for s, p, o in g:
        subs.add(s)
        preds.add(p)
        objs.add(o)
        if p == RDF.type:
            type_assertions += 1
            type_objs.add(o)

    return {
        "triples": len(g),
        "subjects": len(subs),
        "predicates": len(preds),
        "objects": len(objs),
        "type_assertions": type_assertions,
        "distinct_types": len(type_objs),
    }


def build_metadata_ttl(graph_name: str, version: str, publish_date: str, stats: dict) -> str:
    """Build metadata TTL using MWO/nfdicore vocabulary (option B: stats as description text)."""
    dataset_iri = f"{GRAPH_ROOT}/{graph_name}"
    desc_iri = f"{dataset_iri}/dump-description"
    version_iri = f"{dataset_iri}/dump-version/{version}"
    id_iri = f"{dataset_iri}/dump-identifier/{version}"
    license_iri = "https://purls.helmholtz-metadaten.de/msekg/17453312603732"
    creator_iri = "https://purls.helmholtz-metadaten.de/msekg/17458299010501"

    desc_text = (
        f"RDF dump of named graph: {dataset_iri}\\n"
        f"Version: {version}\\n"
        f"Date: {publish_date}\\n"
        f"MWO ontology version: {MWO_VERSION} ({MWO_VERSION_IRI})\\n"
        f"Statistics:\\n"
        f"  triples: {stats['triples']}\\n"
        f"  distinct subjects: {stats['subjects']}\\n"
        f"  distinct predicates: {stats['predicates']}\\n"
        f"  distinct objects: {stats['objects']}\\n"
        f"  rdf:type assertions: {stats['type_assertions']}\\n"
        f"  distinct types (classes used): {stats['distinct_types']}"
    )

    return "\n".join([
        '@prefix obo:      <http://purl.obolibrary.org/obo/> .',
        '@prefix nfdicore:  <https://nfdi.fiz-karlsruhe.de/ontology/> .',
        '@prefix xsd:      <http://www.w3.org/2001/XMLSchema#> .',
        '@prefix dct:      <http://purl.org/dc/terms/> .',
        '',
        f'<{dataset_iri}>',
        f'  a nfdicore:NFDI_0000009 ;',
        f'  nfdicore:NFDI_0000142 <{license_iri}> ;',
        f'  nfdicore:NFDI_0001027 <{creator_iri}> ;',
        f'  nfdicore:NFDI_0001006 <{id_iri}> ;',
        f'  obo:IAO_0000235 <{desc_iri}> ;',
        f'  dct:conformsTo <{MWO_VERSION_IRI}> .',
        '',
        f'<{desc_iri}>',
        f'  a nfdicore:NFDI_0000018 ;',
        f'  nfdicore:NFDI_0001007 "{desc_text}" .',
        '',
        f'<{id_iri}>',
        f'  a obo:IAO_0020000 ;',
        f'  nfdicore:NFDI_0001007 "v{version}" .',
        '',
        f'<{version_iri}>',
        f'  a nfdicore:NFDI_0001053 ;',
        f'  nfdicore:NFDI_0001007 "{version}" .',
        '',
    ])


def main():
    parser = argparse.ArgumentParser(description="Bootstrap initial RDF dumps from Virtuoso")
    parser.add_argument("--sparql", required=True, help="Virtuoso SPARQL endpoint URL")
    parser.add_argument("--user", required=True, help="Virtuoso username")
    parser.add_argument("--pass", dest="passwd", required=True, help="Virtuoso password")
    parser.add_argument("--version", default="1.0.0", help="Version string (default: 1.0.0)")
    parser.add_argument("--out-dir", default="./bootstrap_dumps", help="Output directory")
    args = parser.parse_args()

    publish_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    os.makedirs(args.out_dir, exist_ok=True)

    manifest_graphs = []

    for graph_name in NAMED_GRAPHS:
        graph_uri = f"{GRAPH_ROOT}/{graph_name}"
        print(f"\n--- {graph_name} ---")
        print(f"  CONSTRUCT from <{graph_uri}> ...")

        nt_data = sparql_dump_ntriples(args.sparql, graph_uri, args.user, args.passwd)
        if not nt_data.strip():
            print(f"  [SKIP] Empty result for {graph_name}")
            continue

        # Parse N-Triples and serialize as Turtle for a cleaner dump
        from rdflib import Graph as RdfGraph
        g = RdfGraph()
        g.parse(data=nt_data, format="nt")
        dump_filename = f"{graph_name}_v{args.version}.ttl"
        dump_path = os.path.join(args.out_dir, dump_filename)
        g.serialize(destination=dump_path, format="turtle")
        file_size = os.path.getsize(dump_path)
        print(f"  Wrote {dump_path} ({file_size:,} bytes, {len(g):,} triples)")

        stats = compute_stats(dump_path)
        print(f"  Stats: {stats['triples']:,} triples, {stats['subjects']:,} subjects, "
              f"{stats['distinct_types']} types")

        meta_ttl = build_metadata_ttl(graph_name, args.version, publish_date, stats)
        meta_path = os.path.join(args.out_dir, f"{graph_name}_v{args.version}_metadata.ttl")
        with open(meta_path, "w", encoding="utf-8") as f:
            f.write(meta_ttl)
        print(f"  Wrote {meta_path}")

        manifest_graphs.append({
            "graph_name": graph_name,
            "graph_uri": graph_uri,
            "dump_file": dump_filename,
            "metadata_file": f"{graph_name}_v{args.version}_metadata.ttl",
            "stats": stats,
        })

    # Write manifest
    manifest = {
        "latest_version": args.version,
        "releases": [
            {
                "version": args.version,
                "publish_date": publish_date,
                "mwo_version": MWO_VERSION,
                "mwo_version_iri": MWO_VERSION_IRI,
                "zenodo_doi": "",
                "zenodo_url": "",
                "graphs": manifest_graphs,
            }
        ],
    }

    manifest_path = os.path.join(args.out_dir, "dumps.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"\nWrote manifest: {manifest_path}")
    print(f"Copy this to docs/dumps.json to seed the documentation site.")


if __name__ == "__main__":
    main()
