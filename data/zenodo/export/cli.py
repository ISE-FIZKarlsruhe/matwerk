import os
from pathlib import Path
from typing import Set
from rdflib.graph import ConjunctiveGraph
from rdflib.term import URIRef, Literal
from rdflib.namespace import RDFS

import argparse

from .downloads import download_rdf_files, download_rdf_and_zip_collect
from .iri_utils import normalize_graph_uris, safe_slug
from .rdf_builders import build_record_in_default_graph
from .state import load_state, save_state, record_key
from .zenodo_api import normalize_community, iter_community_records, extract_files, get_records_by_ids
from .mint import get_or_mint_instance, get_or_mint_file_graph, get_or_mint_file_identifier
from .rdf_import import import_rdf_into_named_graph
from .zenodo_api import get_metadata

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--community", default="https://zenodo.org/communities/nfdi-matwerk/", help="Zenodo community slug or URL")
    parser.add_argument("--record-ids", default=None, help="Comma-separated Zenodo record IDs (bypass community fetch)")
    parser.add_argument("--out", default="zenodo.ttl", help="Path to write RDF output")
    parser.add_argument("--format", default="turtle", choices=["turtle", "xml", "json-ld", "nt", "n3", "trig"])
    parser.add_argument("--download-dir", default="./download", help="Directory to download files")
    parser.add_argument("--max", type=int, default=-1, help="Limit number of records processed")
    parser.add_argument("--state-file", default=".graph_ids.json", help="JSON file to persist instance/node/file-graph IRIs")
    args = parser.parse_args()

    token = os.environ.get("ZENODO_TOKEN")
    comm_slug = normalize_community(args.community)

    state_path = Path(args.state_file)
    state = load_state(state_path)

    ds = ConjunctiveGraph()
    g = ds.default_context
    ds.bind("nfdi", "https://nfdi.fiz-karlsruhe.de/ontology/")
    ds.bind("obo", "http://purl.obolibrary.org/obo/")
    ds.bind("dcterms", "http://purl.org/dc/terms/")

    count = 0
    download_root = Path(args.download_dir).expanduser().resolve() if args.download_dir else None
    if download_root:
        download_root.mkdir(parents=True, exist_ok=True)

    session_nodes: Set[str] = set()

    # Choose records iterator
    if args.record_ids:
        ids = [x.strip() for x in args.record_ids.split(",") if x.strip()]
        records_iter = get_records_by_ids(ids, token)
        print(f"Fetching {len(ids)} record(s) by ID: {', '.join(ids)}")
    else:
        records_iter = iter_community_records(comm_slug, token)

    for rec in records_iter:
        count += 1
        key = record_key(rec)

        instance_iri = get_or_mint_instance(state, key, session_nodes)
        print(f"[{count}] Record {rec.get('id')} -> instance {instance_iri}")

        # Default graph enrichment for the record
        build_record_in_default_graph(g, rec, instance_iri, state, key, session_nodes)

        # Handle files: enrich ALL, and import RDF from direct files and ZIPs
        files = extract_files(rec)

        downloaded_map = {}  # file_key -> list of (local_path, content_type, inner_name)
        if download_root:
            rid = rec.get("id")
            dest = download_root / str(rid)
            # Direct RDF
            dl_direct = download_rdf_files(files, dest, token)
            for local_path, content_type, fmeta in dl_direct:
                fkey = fmeta.get("key") or fmeta.get("filename") or local_path.name
                downloaded_map.setdefault(fkey, []).append((local_path, content_type, None))
            # ZIP + inner RDF
            dl_zip = download_rdf_and_zip_collect(files, dest, token)
            for local_path, content_type, fmeta, inner_name in dl_zip:
                fkey = fmeta.get("key") or fmeta.get("filename") or local_path.name
                downloaded_map.setdefault(fkey, []).append((local_path, content_type, inner_name))

        # Enrich all files
        for fmeta in files:
            file_key = fmeta.get("key") or fmeta.get("filename") or "file"
            file_link = fmeta.get("link")
            file_uri = URIRef(file_link) if file_link and file_link.startswith("http") else URIRef(f"urn:file:{safe_slug(file_key)}")

            # minted file identifier (literal)
            fid = URIRef(get_or_mint_file_identifier(state, key, file_key, session_nodes))
            g.add((fid, URIRef("http://purl.org/dc/terms/identifier"), Literal(file_uri)))
            
            # hasPart relation
            g.add((instance_iri, URIRef("http://purl.obolibrary.org/obo/BFO_0000051"), fid))

            # label
            g.add((fid, RDFS.label, Literal(file_key)))

            # title and doi propagation
            meta = get_metadata(rec)
            title = meta.get("title")
            doi = meta.get("doi") or rec.get("doi")
            if title:
                g.add((fid, URIRef("http://purl.obolibrary.org/obo/OBI_0002135"), Literal(title)))
            if doi:
                g.add((fid, URIRef("https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006"), Literal(doi)))
                doi_url = (rec.get("links", {}) or {}).get("doi") or (f"https://doi.org/{doi}" if not str(doi).startswith("http") else str(doi))
                g.add((fid, URIRef("http://purl.org/dc/terms/isPartOf"), URIRef(doi_url)))
                g.add((fid, RDFS.seeAlso, URIRef(doi_url)))

            # If downloaded/collected RDF exists for this file, import and link named graph(s)
            for local_path, content_type, inner_name in downloaded_map.get(file_key, []):
                combined_key = f"{file_key}!{inner_name}" if inner_name else file_key
                graph_iri = get_or_mint_file_graph(state, key, combined_key, session_nodes)
                ok = import_rdf_into_named_graph(ds, local_path, graph_iri, content_type)
                if ok:
                    g.add((fid, RDFS.seeAlso, graph_iri))
                    g.add((instance_iri, RDFS.seeAlso, graph_iri))
                else:
                    print(f"  (skipped linking {local_path.name}; parse failed)")

        if 0 < args.max <= count:
            break

    save_state(state_path, state)

    print(f"Serializing {count} records to {args.out} as {args.format}…")
    if args.format.lower() != "trig":
        has_named = any(1 for ctx in ds.contexts() if ctx.identifier != g.identifier)
        if has_named:
            print("Note: Dataset contains named graphs (imported RDF). Switching to TriG.")
            args.format = "trig"

    normalize_graph_uris(ds)
    ds.serialize(destination=args.out, format=args.format)
    print("Done.")
