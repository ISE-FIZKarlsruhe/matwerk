# common/graph_metadata.py
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import quote

import json


def utc_now_iso_seconds() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

@dataclass(frozen=True)
class RdfStats:
    triples: int
    subjects: int
    predicates: int
    objects: int
    type_assertions: int
    distinct_type_objects: int  # how many distinct rdf:type targets


def compute_rdf_stats(rdf_path: str) -> RdfStats:
    """
    Parses RDF and returns cheap, useful summary stats.

    Requires rdflib in your environment.
    Supports turtle/ntriples/rdfxml/jsonld depending on file content.
    """
    from rdflib import Graph, RDF

    g = Graph()
    g.parse(rdf_path)

    triples = len(g)

    subs = set()
    preds = set()
    objs = set()
    type_objs = set()
    type_assertions = 0

    for s, p, o in g:
        subs.add(s)
        preds.add(p)
        objs.add(o)
        if p == RDF.type:
            type_assertions += 1
            type_objs.add(o)

    return RdfStats(
        triples=triples,
        subjects=len(subs),
        predicates=len(preds),
        objects=len(objs),
        type_assertions=type_assertions,
        distinct_type_objects=len(type_objs),
    )

@dataclass(frozen=True)
class GraphPublishFacts:
    graph_root: str             # https://.../msekg
    stage: str                  # merge / spreadsheets / validation_checks ...
    dag_id: str
    run_id: str

    data_graph_uri: str         # https://.../msekg/<stage>
    ttl_path: str
    started_at: str             # ISO Z
    ended_at: str               # ISO Z

    # optional run/task facts
    task_id: str | None = None
    operator: str | None = None
    log_url: str | None = None
    hostname: str | None = None
    
    # rdf stats
    stats: RdfStats | None = None


def build_metadata_ttl(f: GraphPublishFacts) -> str:

    inst_begin = f"{f.graph_root}/begin"
    inst_end = f"{f.graph_root}/end"
    process_iri = f"{f.graph_root}/publish-process"
    temporal_region_iri = f"{f.graph_root}/temporal-region"

    # local predicate namespace
    ttl_abs = os.path.abspath(f.ttl_path)
    dataset_iri = f"{f.graph_root}/graph-dataset"
    desc_iri = f"{f.graph_root}/description"
    url_iri  = f"{f.graph_root}/url"
    id_iri   = f"{f.graph_root}/identifier"
    pub_iri  = f"{f.graph_root}/publisher"
    license_iri  = "https://purls.helmholtz-metadaten.de/msekg/17453312603732" # MIT license
    creator_iri  = "https://purls.helmholtz-metadaten.de/msekg/17458299010501" # NFDI-MatWerk

    stats = f.stats
    desc_value = (
        f"Published data graph: {f.data_graph_uri}\n"
        f"Airflow dag_id: {f.dag_id}\n"
        f"Airflow run_id: {f.run_id}\n"
        f"Airflow stage: {f.stage}\n"
        f"Airflow input_file: file://{ttl_abs}\n"
    )
    # Optional execution context (append only if present)
    exec_lines = []
    if f.task_id:
        exec_lines.append(f"  task_id: {f.task_id}")
    if f.operator:
        exec_lines.append(f"  operator: {f.operator}")
    if f.log_url:
        exec_lines.append(f"  log_url: {f.log_url}")
    if f.hostname:
        exec_lines.append(f"  hostname: {f.hostname}")
    if exec_lines:
        desc_value += "Execution:\n" + "\n".join(exec_lines) + "\n"

    # Stats (always present but values may be NA)
    desc_value += (
        "Statistics:\n"
        f"  triples: {stats.triples if stats else 'NA'}\n"
        f"  subjects: {stats.subjects if stats else 'NA'}\n"
        f"  predicates: {stats.predicates if stats else 'NA'}\n"
        f"  objects: {stats.objects if stats else 'NA'}\n"
        f"  rdf:type assertions: {stats.type_assertions if stats else 'NA'}\n"
        f"  distinct rdf:type objects: {stats.distinct_type_objects if stats else 'NA'}"
    )

    lines = [
        f"@prefix obo:  <http://purl.obolibrary.org/obo/> .",
        f"@prefix nfdicore:  <https://nfdi.fiz-karlsruhe.de/ontology/> .",
        f"@prefix time: <http://www.w3.org/2006/time#> .",
        f"@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .",
        "",
        
        # dataset
        f"<{dataset_iri}>",
        f"  a nfdicore:NFDI_0000009 ;",
        f"  nfdicore:NFDI_0000191 <{pub_iri}> ;",
        f"  nfdicore:NFDI_0000142 <{license_iri}> ;",
        f"  nfdicore:NFDI_0001027 <{creator_iri}> ;",
        f"  nfdicore:NFDI_0001023 <{process_iri}> ;",
        f"  nfdicore:NFDI_0001006 <{id_iri}> ;",
        f"  obo:IAO_0000235 <{desc_iri}> ;",
        f"  obo:IAO_0000235 <{url_iri}> .",
        "",
        
        # description node (denoted by)
        f"<{desc_iri}> a nfdicore:NFDI_0000018 ; nfdicore:NFDI_0001007 {json.dumps(desc_value)} .",
        "",

        # url node (denoted by)
        f"<{url_iri}> a nfdicore:NFDI_0000223 ; nfdicore:NFDI_0001008 {json.dumps(f.graph_root)}^^xsd:anyURI .",
        "",
        
        # identifier node
        f"<{id_iri}> a <http://purl.obolibrary.org/obo/IAO_0020000> ; nfdicore:NFDI_0001007 {json.dumps(f.run_id)} .",
        "",
            
        f"<{process_iri}>",
        f"  a obo:BFO_0000015 ;",              # process
        f"  obo:BFO_0000199 <{temporal_region_iri}> .", # occupies temporal region
        "",
        
        f"<{temporal_region_iri}>",
        f"  a obo:BFO_0000038 ;",              # 1D temporal region
        f"  obo:BFO_0000222 <{inst_begin}> ;", # has first instant
        f"  obo:BFO_0000224 <{inst_end}> .",   # has last instant
        "",
        
        f"<{inst_begin}>",
        f"  a obo:BFO_0000148 ;",              # 0D temporal region
        f"  time:inXSDDateTimeStamp \"{f.started_at}\"^^xsd:dateTimeStamp .",
        "",
        
        f"<{inst_end}>",
        f"  a obo:BFO_0000148 ;",
        f"  time:inXSDDateTimeStamp \"{f.ended_at}\"^^xsd:dateTimeStamp .",
        "",
    ]
    lines.append("")  # trailing newline
    return "\n".join(lines)
