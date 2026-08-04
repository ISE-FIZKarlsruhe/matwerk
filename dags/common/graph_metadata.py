# common/graph_metadata.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import json


def utc_now_iso_seconds() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _ttl_str(s: str) -> str:
    """JSON-encoded literal works for Turtle string escaping (matches build_metadata_ttl style)."""
    return json.dumps(s, ensure_ascii=False)

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
    dataset_iri = f"{f.graph_root}"
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
        f"  obo:RO_0002353 <{process_iri}> ;",
        f"  nfdicore:NFDI_0001006 <{id_iri}> ;",
        f"  obo:IAO_0000235 <{desc_iri}> ;",
        f"  obo:IAO_0000235 <{url_iri}> .",
        "",
        
        # description node (denoted by)
        f"<{desc_iri}> a nfdicore:NFDI_0001018 ; nfdicore:NFDI_0001007 {json.dumps(desc_value)} .",
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


# ----------------------------------------------------------------------------
# Per-named-graph provenance for harvested Zenodo RDF + reasoner validation
# ----------------------------------------------------------------------------

@dataclass(frozen=True)
class ValidationFacts:
    """Validation result for a single named graph (filled by the reasoner step)."""
    consistent: bool
    reasoner: str                     # e.g. "openllet"
    log_excerpt: str                  # short, user-facing summary
    full_log: Optional[str] = None    # full reasoner stdout/stderr if available
    checked_at: Optional[str] = None  # ISO timestamp


@dataclass(frozen=True)
class GraphProvenanceFacts:
    """
    Provenance for a single named graph imported from a Zenodo record file.

    Modeled with BFO/MWO/nfdicore (no PROV), mirroring the patterns used by
    common/graph_metadata.build_metadata_ttl and dags/dump_and_archive.py.
    """
    graph_iri: str                    # named-graph IRI (also serves as file artifact IRI)
    record_iri: str                   # parent Zenodo concept IRI (already minted by the harvester)
    file_key: str                     # filename inside the Zenodo deposit
    download_url: Optional[str]       # direct download URL on Zenodo
    record_url: Optional[str]         # human-facing Zenodo record URL
    content_type: Optional[str]       # MIME type, if known
    harvested_at: str                 # ISO timestamp the harvester parsed this file
    validation: Optional[ValidationFacts] = None


def build_graph_provenance_ttl(facts: GraphProvenanceFacts) -> str:
    """
    Build TTL describing a single named graph IRI as an information artifact
    that is part of a Zenodo record. The output is a TTL fragment intended to
    be written into the *default graph* of zenodo.ttl.

    Class/predicate choices align with build_metadata_ttl above:
      - nfdicore:NFDI_0000027  (file)
      - nfdicore:NFDI_0001018  (textual description)
      - nfdicore:NFDI_0001007  (has value)
      - nfdicore:NFDI_0001008  (has url, anyURI)
      - obo:BFO_0000050        (part of)
      - obo:BFO_0000051        (has part)
      - obo:RO_0002353         (output of)
      - obo:BFO_0000015        (process)
      - obo:BFO_0000038        (1D temporal region)
      - obo:BFO_0000148        (0D temporal region)
      - obo:IAO_0000235        (denoted by)
    """
    g = facts.graph_iri
    desc_iri = f"{g}/description"
    val_iri = f"{g}/validation"
    proc_iri = f"{g}/harvest-process"
    temp_iri = f"{g}/harvest-temporal"
    inst_iri = f"{g}/harvest-instant"

    desc_value = (
        f"Imported RDF graph from Zenodo.\n"
        f"file: {facts.file_key}\n"
        f"record: {facts.record_iri}\n"
        f"download_url: {facts.download_url or ''}\n"
        f"record_url: {facts.record_url or ''}\n"
        f"content_type: {facts.content_type or ''}\n"
        f"harvested_at: {facts.harvested_at}"
    )

    lines = [
        f"<{facts.record_iri}> obo:BFO_0000051 <{g}> .",
        "",
        f"<{g}>",
        f"  a nfdicore:NFDI_0000027 ;",
        f"  rdfs:label {_ttl_str(facts.file_key)} ;",
        f"  obo:BFO_0000050 <{facts.record_iri}> ;",
        f"  obo:RO_0002353 <{proc_iri}> ;",
        f"  obo:IAO_0000235 <{desc_iri}> ;",
        f"  obo:IAO_0000235 <{val_iri}>" + (" ;" if facts.download_url else " ."),
    ]
    if facts.download_url:
        lines.append(f"  nfdicore:NFDI_0001008 {_ttl_str(facts.download_url)}^^xsd:anyURI .")
    lines.append("")

    lines += [
        f"<{desc_iri}>",
        f"  a nfdicore:NFDI_0001018 ;",
        f"  rdfs:label \"description\" ;",
        f"  nfdicore:NFDI_0001007 {_ttl_str(desc_value)} .",
        "",
    ]

    val = facts.validation
    if val is None:
        val_text = "status: pending\nValidation will be filled in by the reasoner step."
        val_lines = [
            f"<{val_iri}>",
            f"  a nfdicore:NFDI_0001018 ;",
            f"  rdfs:label \"validation\" ;",
            f"  nfdicore:NFDI_0001007 {_ttl_str(val_text)} .",
            "",
        ]
    else:
        val_text = (
            f"status: {'consistent' if val.consistent else 'INCONSISTENT'}\n"
            f"reasoner: {val.reasoner}\n"
            f"checked_at: {val.checked_at or ''}\n"
            f"summary: {val.log_excerpt}"
        )
        if val.full_log:
            val_text += "\nfull_log:\n" + val.full_log
        val_lines = [
            f"<{val_iri}>",
            f"  a nfdicore:NFDI_0001018 ;",
            f"  rdfs:label \"validation\" ;",
            f"  nfdicore:NFDI_0001007 {_ttl_str(val_text)}"
            + (" ;" if not val.consistent else " ."),
        ]
        if not val.consistent:
            val_lines.append(f"  rdfs:comment \"INCONSISTENT\" .")
        val_lines.append("")
    lines += val_lines

    lines += [
        f"<{proc_iri}>",
        f"  a obo:BFO_0000015 ;",
        f"  obo:BFO_0000199 <{temp_iri}> .",
        "",
        f"<{temp_iri}>",
        f"  a obo:BFO_0000038 ;",
        f"  obo:BFO_0000222 <{inst_iri}> ;",
        f"  obo:BFO_0000224 <{inst_iri}> .",
        "",
        f"<{inst_iri}>",
        f"  a obo:BFO_0000148 ;",
        f"  time:inXSDDateTimeStamp \"{facts.harvested_at}\"^^xsd:dateTimeStamp .",
        "",
    ]
    return "\n".join(lines)


GRAPH_PROVENANCE_PREFIXES = (
    "@prefix obo:      <http://purl.obolibrary.org/obo/> .\n"
    "@prefix nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/> .\n"
    "@prefix rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .\n"
    "@prefix time:     <http://www.w3.org/2006/time#> .\n"
    "@prefix xsd:      <http://www.w3.org/2001/XMLSchema#> .\n"
)
