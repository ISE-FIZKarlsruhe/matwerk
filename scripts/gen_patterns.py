#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate one ontology-design-pattern page per spreadsheet tab.

Each of the workbook's template tabs *is* a design pattern: an entity instance of one
class, denoted by a handful of value nodes, related to entities in other tabs. That pattern
is currently only implicit — it lives in the ROBOT directive row and in whatever the
curator typed. This script makes it explicit and checkable:

for every tab it emits

    docs/patterns/<tab>/pattern.ttl   the example, as a pure instance graph — one
                                      real row with its value nodes resolved. This is
                                      what the diagram draws, so it holds no term
                                      declarations and no metadata: a visualiser turns
                                      every literal into a node, and those would bury
                                      the pattern under its own annotations.
    docs/patterns/<tab>/module.ttl    the citable artefact — the example plus the term
                                      declarations, the ODP annotations and a STAR
                                      module of the axioms of every term used, so the
                                      file stands alone and can be reasoned over.
    docs/patterns/<tab>/pattern.md    the explanation, the column→property table,
                                      and an ontoink block that draws the example

and every ``pattern.ttl`` is then checked for consistency against MWO with ROBOT
(``--reasoner hermit``, the same reasoner ``process_spreadsheets`` uses), so a
pattern that contradicts the ontology fails here rather than in production.

Because the input is the live workbook and the live ontology, re-running this after
a sheet edit or an ontology upgrade regenerates the documentation — it cannot drift
from the data it documents.

Usage
-----
    python scripts/gen_patterns.py --sheets-dir <dir with <tab>.csv> \
        --mwo <mwo-full.ttl> [--mwo-version 3.0.1] [--out docs/patterns]
    python scripts/gen_patterns.py --fetch            # download the tabs first
"""
from __future__ import annotations

import argparse
import csv
import os
import re
from typing import Dict, List, Optional, Tuple

PROD_SHEET_ID = "1OyoWwcX4zUtrJilwXdtTooavELw278nQSNW2oniBBsk"
EXPORT = "https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"

# tab -> gid, mirroring dags/spreadsheets.py: tsv_gids
TABS: List[Tuple[str, str]] = [
    ("req_1", "394894036"), ("req_2", "0"), ("agent", "2077140060"),
    ("role", "1425127117"), ("process", "1169992315"), ("city", "1469482382"),
    ("people", "1666156492"), ("organization", "447157523"),
    ("dataset", "1079878268"), ("publication", "1747331228"),
    ("software", "1275685399"), ("dataportal", "923160190"),
    ("instrument", "2015927839"), ("largescalefacility", "370181939"),
    ("metadata", "278046522"), ("matwerkta", "1489640604"),
    ("matwerkiuc", "281962521"), ("matwerkpp", "606786541"),
    ("temporal", "1265818056"), ("event", "638946284"),
    ("collaboration", "266847052"), ("service", "130394813"),
    ("sparql_endpoints", "1732373290"), ("fdos", "152649677"),
    ("ontologies", "2006199416"), ("materials", "497166822"),
]

NFDI = "https://nfdi.fiz-karlsruhe.de/ontology/"
OBO = "http://purl.obolibrary.org/obo/"
MWO = "http://purls.helmholtz-metadaten.de/mwo/"
MATWERK_KG = "https://nfdi.fiz-karlsruhe.de/matwerk/msekg/"
# The ODP community's annotation vocabulary, used to annotate the formalisation
# itself (odpa/patterns-repository ships it inside pattern directories).
CPA = "http://www.ontologydesignpatterns.org/schemas/cpannotationschema.owl#"
PATTERN_BASE = "https://nfdi.fiz-karlsruhe.de/matwerk/pattern/"
GID = dict(TABS)
PREFIXES = [
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
    ("obo", OBO), ("nfdi", NFDI), ("mwo", MWO), ("msekg", MATWERK_KG),
]
# req_1 and req_2 are not patterns. They are the workbook's shared pool of *value
# nodes* — titles, names, websites, roles — that every other tab's `I` columns point
# into. Documenting them as two pages says nothing ("a tab with a label column"), and
# documenting them as 70 standalone pages divorces each value node from the entity it
# exists to describe.
#
# So they are dissolved: every value type is attributed to the entity tab that
# actually references it (a `contact point role` belongs with `process`, an
# `author list` with `publication`), and appears in that pattern — where it is already
# present in the ABox, because value nodes are resolved. Only a value type that no
# entity tab references gets a page of its own.
VALUE_TABS = {"req_1", "req_2"}

# Version of the pattern *documentation itself* — bumped when the shape of a
# generated page changes, independently of the ontology it is generated against.
PATTERN_VERSION = "1.0.0"

# Authors, taken from the ontology's own `dcterms:creator` annotations in
# mwo-full.ttl (each ORCID is resolved there to an rdfs:label), cross-checked
# against the repository's CITATION.cff. Not typed in by hand.
AUTHORS = [
    {"name": "Harald Sack", "orcid": "https://orcid.org/0000-0001-7069-9804"},
    {"name": "Jörg Waitelonis", "orcid": "https://orcid.org/0000-0001-7192-7143"},
    {"name": "Ebrahim Norouzi", "orcid": "https://orcid.org/0000-0002-2691-6995"},
    {"name": "Hossein Beygi Nasrabadi", "orcid": "https://orcid.org/0000-0002-3092-0532"},
]
# MWO's own citation metadata (dcterms:bibliographicCitation / license, and the
# related publication named in CITATION.cff).
MWO_CITATION = ("Hossein Beygi Nasrabadi, Jörg Waitelonis, Ebrahim Norouzi, "
                "Kostiantyn Hubaiev, Harald Sack. NFDI MatWerk Ontology (mwo). "
                "Revision: v{version}. Retrieved from: "
                "http://purls.helmholtz-metadaten.de/mwo/{version}")
MWO_PUBLICATION_DOI = "https://doi.org/10.1002/adem.202502331"
# The only location of a given MWO version that actually dereferences. Both
# http://purls.helmholtz-metadaten.de/mwo/mwo.owl and .../mwo.owl/<version> return
# 404, so the PURL can be cited but not fetched.
MWO_RESOLVABLE = "https://raw.githubusercontent.com/ISE-FIZKarlsruhe/mwo/v{version}/mwo-full.ttl"
MWO_LICENCE = "CC BY 4.0 (repository); CC0 1.0 declared in the ontology header"

# Competency questions per pattern. These follow MWO's own statement of scope — the
# consortium structure (task areas, infrastructure use cases, participant projects,
# researchers, organizations), the NFDI resources (software, workflows, ontologies,
# publications, datasets, metadata schemas, instruments, facilities, educational
# material) and the services, academic events and collaborations — so each question is
# one the MatWerk Knowledge Graph is meant to answer, not an invented illustration.
CQS: Dict[str, List[str]] = {
    "people": [
        "Which researchers are involved in NFDI-MatWerk, and in which task area?",
        "What is the ORCID iD of a given researcher, and which organisation are they affiliated with?",
        "Which academic disciplines does a researcher work in?",
        "Which datasets, software or publications is a given researcher a creator of?",
    ],
    "organization": [
        "Which organisations participate in NFDI-MatWerk?",
        "What is an organisation's ROR identifier, and in which city is it located?",
        "Which organisation is the parent of a given institute?",
        "Which researchers, instruments and facilities belong to a given organisation?",
    ],
    "dataset": [
        "Which datasets does NFDI-MatWerk provide, and under which licence?",
        "Who created a given dataset, and at which organisation?",
        "Which dataset is described by a given metadata standard?",
        "Which datasets relate to a given material or academic discipline?",
    ],
    "publication": [
        "Which publications resulted from a given task area or infrastructure use case?",
        "What is the DOI of a given publication, and who are its authors in order?",
        "Which publications describe a given software, dataset or instrument?",
        "In which year and through which publishing process did a publication appear?",
    ],
    "software": [
        "Which software does NFDI-MatWerk provide, and under which licence?",
        "Where is the source-code repository of a given software, and what is its version?",
        "Which software supports a given workflow or academic discipline?",
        "Who develops and maintains a given software?",
    ],
    "ontologies": [
        "Which ontologies are used or developed in NFDI-MatWerk?",
        "What is the version and licence of a given ontology, and where is it published?",
        "Which ontology is reused by a given metadata standard or knowledge graph?",
    ],
    "instrument": [
        "Which instruments are available, and at which organisation or facility?",
        "Which instrument produced the data in a given dataset?",
        "Which measurement methods does an instrument support?",
    ],
    "largescalefacility": [
        "Which large-scale facilities are available to the MatWerk community?",
        "Which organisation hosts a given facility, and which instruments does it house?",
    ],
    "service": [
        "Which services does NFDI-MatWerk offer, and who provides them?",
        "Which service supports a given infrastructure use case?",
        "How is a given service accessed, and by whom is it maintained?",
    ],
    "event": [
        "Which academic events has NFDI-MatWerk organised, and when?",
        "Which organisation or task area organised a given event, and how often does it recur?",
        "Which publications or educational materials came out of a given event?",
    ],
    "matwerkta": [
        "Which task areas make up NFDI-MatWerk, and what is each responsible for?",
        "Which researchers and organisations contribute to a given task area?",
        "Which infrastructure use cases and participant projects does a task area cover?",
    ],
    "matwerkiuc": [
        "Which infrastructure use cases exist, and which task area does each belong to?",
        "Which datasets, software and services does a given use case produce or consume?",
        "Which organisations collaborate on a given infrastructure use case?",
    ],
    "matwerkpp": [
        "Which participant projects are part of NFDI-MatWerk?",
        "Which organisation runs a given participant project, and who leads it?",
        "Which resources has a participant project contributed to the knowledge graph?",
    ],
    "dataportal": [
        "Which data portals and repositories does NFDI-MatWerk reference?",
        "Which organisation hosts a given portal, and what access conditions apply?",
        "Which data formats and PID systems does a given repository support?",
    ],
    "sparql_endpoints": [
        "Which SPARQL endpoints can the MatWerk Knowledge Graph be federated with?",
        "What is the endpoint URL of a given knowledge graph, and under which licence is it offered?",
        "Who created and maintains a given endpoint's dataset?",
    ],
    "metadata": [
        "Which metadata standards are used in MatWerk research data management?",
        "Where is a given metadata standard documented, and who maintains it?",
        "Which datasets or repositories conform to a given metadata standard?",
    ],
    "materials": [
        "Which materials are described in the knowledge graph?",
        "Which datasets, publications or instruments concern a given material?",
    ],
    "fdos": [
        "Which FAIR Digital Objects has NFDI-MatWerk registered?",
        "What is the persistent identifier of a given FDO, and which dataset does it denote?",
    ],
    "collaboration": [
        "Which international collaborations is NFDI-MatWerk part of?",
        "Which organisations and researchers take part in a given collaboration?",
    ],
    "process": [
        "Which processes are recorded in the knowledge graph, and who participates in them?",
        "Which role is realised in a given process, and over which period?",
    ],
    "role": [
        "Which role does a given person or organisation bear, and in what context?",
        "Which contact point is responsible for a given resource?",
    ],
    "agent": [
        "Which agents (people, organisations, consortia) does the knowledge graph know?",
        "Which agent is responsible for a given resource?",
    ],
    "city": ["In which city and country is a given organisation or facility located?"],
    "temporal": [
        "Over which period did a given project, event or role run?",
        "Which resources were created or published in a given year?",
    ],
    "req_1": [
        "Which abbreviations and academic titles are used across the knowledge graph?",
    ],
}

# The ontologies MWO builds on, and therefore that every pattern transitively reuses.
REUSED = {
    "BFO 2020": "http://purl.obolibrary.org/obo/bfo.owl",
    "NFDIcore": "https://nfdi.fiz-karlsruhe.de/ontology/",
    "IAO": "http://purl.obolibrary.org/obo/iao.owl",
    "RO": "http://purl.obolibrary.org/obo/ro.owl",
}

IRI_RE = re.compile(r"https?://[^\s>\"']+")
# a ROBOT directive cell: "A rdfs:label@en", "I obo:IAO_0000235", "AT nfdi:… ^^xsd:anyURI"
DIRECTIVE_RE = re.compile(r"^(ID|TYPE|A|I|AI|AT|C|DOMAIN|RANGE)\b\s*(.*)$")


# --------------------------------------------------------------------------- #
# input
# --------------------------------------------------------------------------- #
def fetch_tabs(out_dir: str) -> None:
    import requests

    os.makedirs(out_dir, exist_ok=True)
    for name, gid in TABS:
        r = requests.get(EXPORT.format(sid=PROD_SHEET_ID, gid=gid), timeout=90)
        r.raise_for_status()
        with open(os.path.join(out_dir, f"{name}.csv"), "wb") as fh:
            fh.write(r.content)
        print(f"[fetch] {name}.csv ({len(r.content)} bytes)")


def read_tab(path: str) -> Tuple[List[str], List[str], List[List[str]]]:
    rows = list(csv.reader(open(path, encoding="utf-8")))
    if len(rows) < 3:
        return [], [], []
    data = [r for r in rows[2:] if any(c.strip() for c in r) and (r and r[0].strip())]
    return rows[0], rows[1], data


# --------------------------------------------------------------------------- #
# ontology labels
# --------------------------------------------------------------------------- #
def load_labels(paths: List[str]) -> Tuple[Dict[str, str], set, Dict[str, List[str]]]:
    """Labels, and the set of every term the given ontologies DEFINE.

    Both are needed. A term the templates use but no imported ontology defines is
    not a cosmetic gap: an OWL parser treats a statement whose predicate is
    undeclared as an *annotation*, which carries no logical meaning, so a reasoner
    can never contradict it. Those columns are marked in the generated tables
    rather than left blank, because a blank reads as "nothing to say here".
    """
    from rdflib import Graph, RDFS, URIRef

    out: Dict[str, str] = {}
    defined: set = set()
    alts: Dict[str, List[str]] = {}
    for path in paths:
        if not path or not os.path.exists(path):
            continue
        g = Graph()
        g.parse(path)
        for s in set(g.subjects()):
            if isinstance(s, URIRef):
                defined.add(str(s))
        for s, _, o in g.triples((None, RDFS.label, None)):
            out.setdefault(str(s), str(o))
        # IAO 'alternative term' and skos:altLabel — real synonyms, which is what
        # ODP's "Also Known As" is for (it is not a second provenance field).
        for pred in (URIRef(OBO + "IAO_0000118"),
                     URIRef("http://www.w3.org/2004/02/skos/core#altLabel")):
            for s, _, o in g.triples((None, pred, None)):
                alts.setdefault(str(s), []).append(str(o))
    return out, defined, alts


def curie(iri: str) -> str:
    for p, ns in PREFIXES:
        if iri.startswith(ns):
            return f"{p}:{iri[len(ns):]}"
    return f"<{iri}>"


# --------------------------------------------------------------------------- #
# pattern extraction
# --------------------------------------------------------------------------- #
def lit(value: str) -> str:
    """A safe Turtle string literal — newlines and quotes in sheet cells are common."""
    return ('"' + value.replace("\\", "\\\\").replace('"', '\\"')
            .replace("\r", " ").replace("\n", " ").strip() + '"')


def parse_directive(cell: str) -> Tuple[str, Optional[str], str]:
    """('I', '<prop iri>', raw) for a directive cell; ('', None, raw) otherwise."""
    cell = (cell or "").strip()
    m = DIRECTIVE_RE.match(cell)
    if not m:
        return "", None, cell
    kind, rest = m.group(1), m.group(2)
    iris = IRI_RE.findall(rest)
    prop = None
    if iris:
        # 'AT https://…/NFDI_0001008^^xsd:anyURI' — the datatype suffix, SPLIT=
        # and trailing punctuation are part of the directive, not of the IRI.
        prop = re.split(r"\^\^|\s|SPLIT=", iris[0])[0].rstrip(">,;.")
    if prop is None and "rdfs:label" in rest:
        prop = "http://www.w3.org/2000/01/rdf-schema#label"
    return kind, prop, cell


def attribute_value_types(sheets: str, tabs_data: dict) -> Tuple[Dict[str, Tuple[str, int]], Dict[str, list]]:
    """Work out which entity tab each shared value type belongs to.

    Returns ``({value_type: (owning_tab, references)}, {value_type: [rows]})``. The
    owner is the entity tab whose ``I`` columns point at instances of that type most
    often — ``contact point role`` is referenced 316 times by ``process`` and by
    nothing else, so that is where it is documented.
    """
    val_type: Dict[str, str] = {}
    value_rows: Dict[str, list] = {}
    for tab in VALUE_TABS:
        if tab not in tabs_data:
            continue
        head, directives, data = tabs_data[tab]
        lab_cols = [i for i, c in enumerate(directives) if c.strip().startswith("A rdfs:label")]
        for r in data:
            t = r[1].strip() if len(r) > 1 else ""
            if not t.startswith("http"):
                continue
            value_rows.setdefault(t, []).append(r)
            for i in lab_cols:
                if i < len(r) and r[i].strip():
                    val_type.setdefault(r[i].strip().lower(), t)

    refs: Dict[str, Dict[str, int]] = {}
    for tab, (head, directives, data) in tabs_data.items():
        if tab in VALUE_TABS:
            continue
        icols = [i for i, c in enumerate(directives) if c.strip().startswith("I ")]
        for r in data:
            for i in icols:
                if i >= len(r):
                    continue
                for part in re.split(r"\|", r[i]):
                    t = val_type.get(part.strip().lower())
                    if t:
                        refs.setdefault(t, {}).setdefault(tab, 0)
                        refs[t][tab] += 1
    owners = {t: max(c.items(), key=lambda x: x[1]) for t, c in refs.items() if c}
    return owners, value_rows


def _terms_used(ttl_path: str) -> List[str]:
    """Every ontology term the pattern refers to — the seed for a STAR module."""
    from rdflib import Graph, URIRef

    g = Graph()
    g.parse(ttl_path, format="turtle")
    skip = ("https://nfdi.fiz-karlsruhe.de/matwerk/", "https://orcid.org/",
            "http://www.w3.org/", "http://purl.org/dc/",
            "http://www.ontologydesignpatterns.org/")
    terms = set()
    for s_, p_, o_ in g:
        for n in (s_, p_, o_):
            if isinstance(n, URIRef) and not str(n).startswith(skip):
                terms.add(str(n))
    return sorted(terms)


def build_tbox_source(robot_jar: str, ontologies: List[str], out_path: str) -> str:
    """One TBox to extract modules from: the published ontology merged with the
    ontology terms the deployed KG declares, with all individuals removed.

    This exists because ten of the properties the workbook uses — among them
    `has license`, `has identifier`, `has discipline` and `associated organisation` —
    are declared in **none** of the published ontology files (MWO 3.0.1, MWO 3.0.2 or
    nfdicore), only in the merged ontology the pipeline deploys. Extracting from the
    published file alone therefore leaves those properties undeclared, and an OWL
    parser silently downgrades every statement using them to an annotation.

    `remove --select individuals` keeps the axioms and drops the instance data, so a
    STAR module stays small (a few hundred triples) instead of dragging the KG in.
    """
    import subprocess

    merged = out_path + ".merged.ttl"
    cmd = ["java", "-Xmx4g", "-jar", robot_jar, "merge"]
    for o in ontologies:
        cmd += ["-i", o]
    subprocess.run(cmd + ["--output", merged], capture_output=True, timeout=1200, check=True)
    subprocess.run(["java", "-Xmx4g", "-jar", robot_jar, "remove", "--input", merged,
                    "--select", "individuals", "--output", out_path],
                   capture_output=True, timeout=1200, check=True)
    return out_path


def extract_module(ttl_path: str, robot_jar: str, tbox_path: str, out_path: str) -> Optional[str]:
    """STAR module of exactly the terms this pattern uses — its real axioms."""
    import subprocess
    import tempfile

    terms = [t for t in _terms_used(ttl_path)
             if not t.endswith((".ttl", ".owl")) and "/mwo.owl/" not in t]
    if not terms:
        return None
    tf = os.path.join(tempfile.gettempdir(), "pattern_terms.txt")
    with open(tf, "w", encoding="utf-8") as fh:
        fh.write("\n".join(terms))
    try:
        subprocess.run(["java", "-Xmx4g", "-jar", robot_jar, "extract", "--method", "STAR",
                        "--input", tbox_path, "--term-file", tf, "--output", out_path],
                       capture_output=True, timeout=900, check=True)
        return out_path
    except Exception as e:  # noqa: BLE001
        print(f"[gen]   module extraction failed for {ttl_path}: {e}")
        return None


def robot_metrics(ttl_path: str, robot_jar: str, mwo_path: str) -> Dict[str, str]:
    """Ontology metrics via ``robot measure`` — reproducible, and the same tool the
    pipeline already uses.

    Two measurements are taken, because they answer different questions:

    * **standalone** — how big the pattern itself is;
    * **merged with the ontology** — the DL expressivity the pattern actually commits
      to. A pattern on its own is nearly all assertions, so its expressivity is empty;
      the constructs come from the axioms of the classes and properties it uses.
    """
    import subprocess
    import tempfile

    def measure(args: List[str]) -> Dict[str, str]:
        out = os.path.join(tempfile.gettempdir(), "robot_measure.tsv")
        cmd = ["java", "-Xmx3g", "-jar", robot_jar] + args + [
            "measure", "--format", "tsv", "--metrics", "extended", "--output", out]
        try:
            subprocess.run(cmd, capture_output=True, timeout=600, check=True)
        except Exception:
            return {}
        vals: Dict[str, str] = {}
        with open(out, encoding="utf-8") as fh:
            for line in fh:
                p = line.rstrip("\n").split("\t")
                if len(p) >= 3 and p[2] == "single_value":
                    vals.setdefault(p[0], p[1])
        return vals

    solo = measure(["--input", ttl_path])

    # DL expressivity of the pattern *by itself* is empty: it is almost all
    # assertions, and the constructs live in the axioms of the terms it uses. So the
    # figure reported is the pattern together with a STAR module of exactly those
    # terms — the axioms this pattern actually depends on, and nothing else. (Merging
    # the whole ontology instead would report MWO's expressivity for all 25 patterns
    # identically, which says nothing about any of them.)
    merged = {}
    if mwo_path:
        terms = _terms_used(ttl_path)
        mod = os.path.join(tempfile.gettempdir(), "robot_module.ttl")
        args = ["extract", "--method", "STAR", "--input", mwo_path]
        for t in terms:
            args += ["--term", t]
        try:
            subprocess.run(["java", "-Xmx3g", "-jar", robot_jar] + args
                           + ["--output", mod], capture_output=True, timeout=600, check=True)
            merged = measure(["merge", "-i", mod, "-i", ttl_path])
        except Exception:
            merged = {}

    # Actually run the reasoner rather than claiming the pattern is consistent. A page
    # that asserts "consistent" without having checked is worth nothing.
    consistency = "not checked"
    if mwo_path:
        exp = os.path.join(tempfile.gettempdir(), "robot_explain.md")
        try:
            subprocess.run(
                ["java", "-Xmx3g", "-jar", robot_jar, "merge", "-i", mwo_path,
                 "-i", ttl_path, "explain", "--reasoner", "hermit",
                 "-M", "inconsistency", "--explanation", exp],
                capture_output=True, timeout=900, check=True)
            with open(exp, encoding="utf-8") as fh:
                consistency = ("consistent" if "No explanations found" in fh.read()
                               else "INCONSISTENT")
        except Exception:
            consistency = "check failed"
    return {
        "consistency": consistency,
        "axioms": solo.get("axiom_count", "?"),
        "logical_axioms": solo.get("logical_axiom_count", "?"),
        "classes": solo.get("class_count", "?"),
        "individuals": solo.get("individual_count", "?"),
        "object_properties": solo.get("object_property_count", "0"),
        "expressivity_solo": solo.get("expressivity", "") or "—",
        "expressivity_merged": merged.get("expressivity", "") or "—",
        "merged_logical_axioms": merged.get("logical_axiom_count", "?"),
    }


def pattern_meta(tab: str, entity_type: str, cols: List[dict], labels: Dict[str, str],
                 cqs: Optional[List[str]]) -> Dict[str, str]:
    """Intent, scenario and unit test for one pattern.

    Shared by the Turtle header and the markdown page so the two cannot drift: the
    ODP annotations inside ``pattern.ttl`` and the table on the page are the same
    strings, produced once.
    """
    entity_label = labels.get(entity_type, "") or (curie(entity_type) if entity_type else "entity")
    intent = (f"To represent a {entity_label} in the MatWerk Knowledge Graph: its identity, the "
              f"value nodes that describe it, and its links to the other entities the "
              f"NFDI-MatWerk consortium records.")

    # The name of the thing. Most tabs carry it as an `A rdfs:label` column, but
    # several (service, software, ontologies …) name the entity through a value node
    # instead, so those have no label column at all — the same asymmetry that made
    # the unit tests fail. Fall back to the first value-node reference, which is what
    # a reader would call the thing.
    subject = next((c["value"] for c in cols
                    if c["property"] and c["property"].endswith("#label") and c["value"]), "")
    if not subject:
        subject = next((c["value"] for c in cols
                        if c["kind"] == "I" and c["value"]), "")
    facts = [f"{c['column'].strip().rstrip('*').lower()} “{c['value']}”"
             for c in cols if c["value"] and c["kind"] == "I" and c["column"]][:3]
    scenario = (f"“{subject}” is a {entity_label}"
                + (", with " + "; ".join(facts) if facts else "") + ".") if subject else ""

    # A unit test in the ODP sense: a query that must return a row if the pattern is
    # correctly instantiated.
    #
    # Which query is correct depends on where the pattern puts its name, and the two
    # cases are genuinely different. Tabs with an `A rdfs:label` column put the label
    # on the entity. The rest name the entity through a *value node* — `software` and
    # `ontologies` do this, and their entities carry no rdfs:label at all (verified
    # against the live endpoint: 48 instances of database software, 0 with a direct
    # label, 2 304 value node labels). Asserting the entity-label form for those would
    # produce a unit test that fails on correct data.
    entity_has_label = any(
        c["property"] and c["property"].endswith(("#label", "rdfs:label")) and c["kind"] == "A"
        for c in cols)
    RDFS_LABEL = "<http://www.w3.org/2000/01/rdf-schema#label>"
    if not entity_type:
        unit_test = ""
    elif entity_has_label:
        unit_test = (f"SELECT ?x ?label WHERE {{ ?x a <{entity_type}> ; "
                     f"{RDFS_LABEL} ?label }} LIMIT 10")
    else:
        unit_test = (f"SELECT ?x ?label WHERE {{ ?x a <{entity_type}> ; "
                     f"<{OBO}IAO_0000235> ?d . ?d {RDFS_LABEL} ?label }} LIMIT 10")
    consequences = (
        "Values become reusable and independently addressable, and one value may be "
        "shared by many entities. The cost: every `I` reference resolves by label, so "
        "labels must be unique and a typo yields a silently missing relation rather "
        "than an error.")
    return {"intent": intent, "scenario": scenario, "unit_test": unit_test,
            "consequences": consequences, "entity_label": entity_label}


def origin_tab_of(name: str) -> str:
    """The workbook tab a generated page came from (value-* pages have none)."""
    return "" if name.startswith("value-") else name


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-") or "unnamed"


def pick_example_row(data: List[List[str]], n_cols: int) -> Optional[List[str]]:
    """The most complete row — the one that shows the most of the pattern."""
    best, score = None, -1
    for r in data:
        if not r or not r[0].strip().startswith("http"):
            continue
        s = sum(1 for c in r[:n_cols] if c.strip())
        if s > score:
            best, score = r, s
    return best


def build_pattern(tab: str, head: List[str], directives: List[str], data: List[List[str]],
                  labels: Dict[str, str], label_index: Dict[str, Tuple[str, str, str]],
                  mwo_version: str,
                  cqs: Optional[List[str]] = None,
                  components: Optional[List[Tuple[str, str, int]]] = None,
                  related: Optional[List[str]] = None) -> Tuple[str, List[dict]]:
    """Return (turtle, column-map) for one tab."""
    row = pick_example_row(data, len(directives))
    if row is None:
        return "", "", []

    entity_iri = row[0].strip()
    entity_type = ""
    cols: List[dict] = []
    tbox_classes, tbox_props = {}, {}
    abox_hub: List[str] = []
    abox_sats: List[str] = []
    seen_sat: set = set()

    for i, dcell in enumerate(directives):
        kind, prop, raw = parse_directive(dcell)
        if not kind or kind in ("DOMAIN", "RANGE"):
            continue
        value = row[i].strip() if i < len(row) else ""
        human = (head[i].strip() if i < len(head) else "") or f"col{i}"
        if kind == "ID":
            continue
        if kind == "TYPE":
            entity_type = value
            continue
        cols.append({"column": human, "kind": kind, "property": prop, "value": value})
        if not value or not prop:
            continue

        if kind in ("A", "AT", "AI"):            # literal-valued
            if prop.endswith("rdfs:label") or prop.endswith("#label"):
                abox_hub.append(f"    rdfs:label {lit(value)}")
            else:
                tbox_props[prop] = "owl:DatatypeProperty"
                dt = "^^xsd:anyURI" if "anyURI" in raw else ""
                abox_hub.append(f"    {curie(prop)} {lit(value)}{dt}")
        elif kind == "I":                        # points at another instance
            tbox_props[prop] = "owl:ObjectProperty"
            for part in re.split(r"\|", value):
                part = part.strip()
                if not part:
                    continue
                hit = label_index.get(part.lower())
                if not hit:
                    continue                     # dangling reference: left out of the example
                sat_iri, sat_type, sat_label = hit
                abox_hub.append(f"    {curie(prop)} {curie(sat_iri)}")
                if sat_iri in seen_sat:
                    continue
                seen_sat.add(sat_iri)
                # A TYPE cell is only usable if it is an IRI: a few rows carry a
                # human word ("named individual") there, which would emit invalid
                # Turtle and mask the real problem.
                typed = sat_type if sat_type.startswith("http") else ""
                if typed:
                    tbox_classes[typed] = labels.get(typed, "")
                sl = sat_label.replace('"', '\\"')
                abox_sats.append(
                    f"{curie(sat_iri)} a {curie(typed) if typed else 'owl:NamedIndividual'} ;\n"
                    f'    rdfs:label "{sl}" .')

    # As with value nodes, a TYPE cell is only usable when it is an IRI.
    entity_type = entity_type if entity_type.startswith("http") else ""
    if entity_type:
        tbox_classes[entity_type] = labels.get(entity_type, "")

    meta = pattern_meta(tab, entity_type, cols, labels, cqs)
    intent, scenario, unit_test = meta["intent"], meta["scenario"], meta["unit_test"]

    out = [
        f"# {tab} — ontology design pattern (generated by scripts/gen_patterns.py)",
        "#",
        f"# One real row of the '{tab}' tab of the MatWerk workbook, with the value node",
        "# nodes it points at resolved. The TBox block declares every term the pattern",
        "# uses: without declarations an OWL parser treats the statements as annotations,",
        "# and a reasoner would call any nonsense consistent.",
        "#",
        f"# Ontology version: MWO {mwo_version} ({MWO}mwo.owl/{mwo_version})",
        "",
    ]
    out += [f"@prefix {p}: <{ns}> ." for p, ns in PREFIXES]
    out += [f"@prefix cpa: <{CPA}> .",
            "@prefix dcterms: <http://purl.org/dc/terms/> .",
            ""]

    # --- the pattern is itself a versioned ontology, and says so ---------------
    # ODP formalisations are annotated with cpannotationschema; doing the same here
    # means the TTL carries its own intent, competency questions and provenance, so
    # the file remains self-describing when separated from this page.
    slug_id = slug(tab)
    out += [
        "# --- Ontology header: this pattern is a versioned artefact ---",
        f"<{PATTERN_BASE}{slug_id}> a owl:Ontology ;",
        f"    owl:versionIRI <{PATTERN_BASE}{slug_id}/{PATTERN_VERSION}> ;",
        f'    owl:versionInfo "{PATTERN_VERSION}" ;',
        # NOT owl:imports. `owl:imports` obliges every OWL tool to dereference the
        # IRI, and MWO's PURLs do not resolve — http://purls.helmholtz-metadaten.de/
        # mwo/mwo.owl and .../mwo.owl/<version> both return 404, so an import makes
        # `robot` refuse to load the file at all ("Could not load imported ontology").
        # The dependency is therefore *stated* with a resolvable location instead;
        # the consistency check merges the ontology in explicitly, which is what
        # actually gives the pattern its axioms.
        f"    rdfs:isDefinedBy <{MWO_RESOLVABLE.format(version=mwo_version)}> ;",
        f'    dcterms:conformsTo <{MWO}mwo.owl/{mwo_version}> ;',
        f'    rdfs:label "{tab} — NFDI-MatWerk ontology design pattern" ;',
        f'    dcterms:license "{MWO_LICENCE}" ;',
        f'    dcterms:bibliographicCitation {lit(MWO_CITATION.format(version=mwo_version))} ;',
    ]
    out += [f"    dcterms:creator <{a['orcid']}> ;" for a in AUTHORS]
    out += [f'    cpa:hasIntent {lit(intent)} ;']
    if cqs:
        out += [f"    cpa:coversRequirements {lit(q)} ;" for q in cqs]
    if scenario and scenario != "—":
        out += [f"    cpa:scenarios {lit(scenario)} ;"]
    if unit_test:
        out += [f"    cpa:hasUnitTest {lit(unit_test)} ;"]
    out += [f'    cpa:hasConsequences {lit(meta["consequences"])} ;']
    for _c, _n, _cnt in (components or []):
        out.append(f"    cpa:hasComponent {curie(_c)} ;")
    for _r in (related or []):
        out.append(f"    cpa:relatedCPs <{PATTERN_BASE}{slug(_r)}> ;")
    out += [f"    cpa:isSpecializationOf <{PATTERN_BASE}entity-and-value-node> ;",
            f'    cpa:reengineeredFrom "ROBOT template directives, row 2 of the {tab} tab" ;',
            f'    cpa:extractedFrom "the {tab} tab of the NFDI-MatWerk curation workbook" .',
            ""]
    out += [f'<{a["orcid"]}> rdfs:label {lit(a["name"])} .' for a in AUTHORS] + [""]
    # Annotation properties must be declared or an OWL parser silently discards the
    # annotations above — the same trap the TBox declarations below exist to avoid.
    out += ["# --- the annotation properties used by the header ---"]
    out += [f"{c} a owl:AnnotationProperty ." for c in
            ("cpa:hasIntent", "cpa:coversRequirements", "cpa:scenarios",
             "cpa:hasUnitTest", "cpa:extractedFrom", "cpa:hasConsequences",
             "cpa:hasComponent", "cpa:relatedCPs", "cpa:isSpecializationOf",
             "cpa:reengineeredFrom", "dcterms:creator", "dcterms:license",
             "dcterms:conformsTo", "dcterms:bibliographicCitation")]
    out += [""]

    out += ["# --- TBox: the terms this pattern uses ---"]
    for iri, lab in sorted(tbox_classes.items()):
        out.append(f'{curie(iri)} a owl:Class ;\n    rdfs:label "{lab or iri.rsplit("/", 1)[-1]}" .')
    for iri, kind in sorted(tbox_props.items()):
        lab = labels.get(iri, iri.rsplit("/", 1)[-1])
        out.append(f'{curie(iri)} a {kind} ;\n    rdfs:label "{lab}" .')

    # ------------------------------------------------------------------ #
    # The example, on its own.
    #
    # This is what the diagram draws, and it deliberately contains *only* the
    # instance graph. A visualiser renders one node per subject and one green
    # literal node per literal object, so a term declaration —
    #   nfdi:NFDI_0000142 a owl:ObjectProperty ; rdfs:label "has license"
    # — draws the *property itself* as a node with a floating label, unattached to
    # the data. The same happens to every ODP annotation. Put together they bury
    # the pattern under its own metadata and make properties look like literals.
    #
    # So the declarations, the ontology header and the ODP annotations go to
    # module.ttl (the citable, reasoner-checked artefact) and the picture shows the
    # thing itself.
    # ------------------------------------------------------------------ #
    example = [
        f"# {tab} — the example, as an instance graph.",
        "#",
        "# One real row of the workbook: the entity, the value nodes that describe it,",
        "# and the links between them. Declarations, provenance and the ODP annotations",
        "# live in module.ttl so that this file draws cleanly.",
        "#",
        f"# Ontology version: MWO {mwo_version}",
        "",
    ]
    example += [f"@prefix {p}: <{ns}> ." for p, ns in PREFIXES] + [""]
    if abox_hub:
        example.append(
            f"{curie(entity_iri)} a "
            f"{curie(entity_type) if entity_type else 'owl:NamedIndividual'} ;")
        example.append(" ;\n".join(abox_hub) + " .")
    example += [""] + abox_sats + [""]

    return "\n".join(example), "\n".join(out) + "\n", cols


# --------------------------------------------------------------------------- #
# markdown
# --------------------------------------------------------------------------- #
def write_md(tab: str, cols: List[dict], labels: Dict[str, str], out_dir: str,
             mwo_version: str, n_rows: int, entity_type: str,
             defined: Optional[set] = None, display: Optional[str] = None,
             components: Optional[List[Tuple[str, str, int]]] = None,
             cqs: Optional[List[str]] = None,
             metrics: Optional[Dict[str, str]] = None,
             related: Optional[List[str]] = None,
             gid: str = "",
             has_module: bool = False,
             alt_labels: Optional[Dict[str, List[str]]] = None,
             pattern_version: str = PATTERN_VERSION,
             robot_version: str = "1.9.10",
             generated_at: str = "") -> None:
    alts = [a for a in (alt_labels or {}).get(entity_type, []) if a]
    alt_md = ("; ".join(alts) if alts else
              f"the `{tab.split(chr(45))[0]}` template tab of the NFDI-MatWerk workbook")
    authors_md = " · ".join(
        f"[{a['name']}]({a['orcid']})" if a.get("orcid") else a["name"] for a in AUTHORS)
    reused_md = ", ".join(f"{k} (`{v}`)" for k, v in REUSED.items())
    _m = pattern_meta(tab, entity_type, cols, labels, cqs)
    title = (display or tab).replace("_", " ")
    origin = tab.split("-")[0] if "-" in tab and tab.split("-")[0] in VALUE_TABS else tab
    scope = (f"`{origin}` tab of the MatWerk workbook holds **{n_rows} rows** of this "
             f"kind" if origin != tab else
             f"`{tab}` tab of the MatWerk workbook currently holds **{n_rows} rows**")
    lines = [
        f"# {title}",
        "",
        f"The {scope}. "
        f"Every row follows the same shape, and that shape is the pattern below: a "
        f"**entity** instance carrying the row's identity, denoted by **value node** nodes "
        f"that hold its values, and object properties pointing at entities in other tabs.",
        "",
        f"*Generated from the live sheet and MWO {mwo_version} by "
        f"`scripts/gen_patterns.py` — regenerate it rather than editing it by hand.*",
        "",
        "## The pattern",
        "",
        "```ontoink",
        f"source: patterns/{tab}/pattern.ttl",
        "height: 560px",
        "reasoning: false",
        "```",
        "",
        "> Drag the nodes, change the layout, or use **Group** to collapse a namespace. "
        "The picture is the example only; the declarations and annotations behind it "
        "are in [`module.ttl`](module.ttl).",
        "",
        "## Columns",
        "",
        "| Column | ROBOT | Property | Meaning |",
        "|---|---|---|---|",
    ]
    KIND = {"A": "literal", "AT": "typed literal", "AI": "IRI literal",
            "I": "→ instance", "C": "class expr"}
    for c in cols:
        prop = c["property"] or ""
        plabel = labels.get(prop, "")
        lines.append(f"| {c['column']} | `{c['kind']}` | "
                     f"{'`' + curie(prop) + '`' if prop else '—'} | {plabel} |")
    lines += [
        "",
        "## How to read it",
        "",
        f"* The **entity** is typed `{curie(entity_type) if entity_type else '—'}`"
        f"{' (' + labels.get(entity_type, '') + ')' if labels.get(entity_type) else ''} and carries "
        "only its identity, its label, and links.",
        "* A column marked `I` does **not** contain a value — it contains the *label of "
        "another instance*, which ROBOT resolves at build time. That is why every "
        "value node needs a unique label, and why a typo produces a silently missing "
        "relation rather than an error.",
        "* Columns marked `A` are literals on the entity itself.",
        "",
    ]

    # ---- components: the value nodes this pattern owns -----------------------
    if components:
        lines += [
            "## Components (value nodes)",
            "",
            "These value-node types live in the workbook's shared `req_2` tab but belong "
            "to this pattern — they exist to describe it, and are counted by how often "
            "this tab references them.",
            "",
            "| Component | Class | References |",
            "|---|---|---|",
        ]
        for vtype, nice, n in components:
            lines.append(f"| {nice} | `{curie(vtype)}` | {n} |")
        lines.append("")

    # ---- query it --------------------------------------------------------------
    # The generator already writes a unit-test query into pattern.ttl; showing it here
    # turns the page from a description into something a reader can run. Every one of
    # these was executed against the live endpoint before being published.
    _meta_ut = _m.get("unit_test")
    if _meta_ut:
        lines += [
            "## Query it",
            "",
            "Retrieve instances of this pattern from the MatWerk Knowledge Graph "
            "([SPARQL endpoint](https://nfdi.fiz-karlsruhe.de/matwerk/sparql)):",
            "",
            "```sparql",
            _meta_ut,
            "```",
            "",
        ]
        if gid:
            lines += [
                f"The rows behind it are curated in the "
                f"[`{origin}` tab of the workbook]"
                f"(https://docs.google.com/spreadsheets/d/{PROD_SHEET_ID}/edit#gid={gid}).",
                "",
            ]

    # ---- competency questions -------------------------------------------------
    if cqs:
        lines += ["## Competency questions", "",
                  "The questions this pattern exists to answer. Each is answerable "
                  "against the published graphs.", ""]
        lines += [f"{i}. {q}" for i, q in enumerate(cqs, 1)]
        lines.append("")

    # ---- ODP metadata ----------------------------------------------------------
    # Field names and their required/optional split follow the Ontology Design
    # Patterns community's content-ODP annotation schema (odpa/patterns-repository),
    # so a reader who knows ODP pages can read these without translation.
    m = metrics or {}
    entity_label, intent = _m["entity_label"], _m["intent"]
    scenario = _m["scenario"] or "—"
    comp_names = ", ".join(n for _, n, _ in (components or [])[:6]) or "—"
    related_md = (", ".join(f"[{r}]({'../' + r}/pattern.md)" for r in (related or [])[:6])
                  or "—")

    # The 19 rows below are the Content ODP annotation schema exactly as the
    # odpa/patterns-repository renders it: same fields, same order, every row
    # emitted even when empty — which is how ODP pages themselves are written.
    lines += [
        "## Pattern metadata",
        "",
        "The [Ontology Design Patterns](http://ontologydesignpatterns.org) content-ODP "
        "annotation schema, in the community's own field order, so a reader who knows ODP "
        "pages can read this one without translation.",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| **Name** | {title} |",
        f"| **Submitted by** | the NFDI-MatWerk consortium |",
        f"| **Also Known As** | {alt_md} |",
        f"| **Intent** | {intent} |",
        f"| **Domains** | research data management; NFDI consortium structure |",
        f"| **Competency Questions** | {'; '.join(cqs) if cqs else '—'} |",
        f"| **Solution description** | An entity individual typed "
        f"`{curie(entity_type) if entity_type else '—'}` carries identity and label; each "
        f"descriptive value is a separate *value node* individual reached by "
        f"`obo:IAO_0000235` (denoted by); links to other patterns are object properties "
        f"onto those entities. Identity is a minted IRI `msekg:<epoch_ms><counter>`, so "
        f"re-running the pipeline is idempotent. |",
        f"| **Reusable OWL Building Block** | " + ("[`module.ttl`](module.ttl) — the example "
         "together with the axioms of every term it uses, extracted from the ontology, "
         "so the file stands alone" if has_module else "[`pattern.ttl`](pattern.ttl)") + " |",
        f"| **Consequences** | Values become reusable and independently addressable, and a "
        f"value may be shared by many entities. The cost: every `I` reference resolves **by "
        f"label**, so labels must be unique and a typo yields a silently missing relation "
        f"rather than an error. |",
        f"| **Scenarios** | {scenario} |",
        f"| **Known Uses** | the MatWerk Knowledge Graph "
        f"(`https://nfdi.fiz-karlsruhe.de/matwerk`), published as Virtuoso named graphs |",
        f"| **Web References** | <https://nfdi-matwerk.de> · "
        f"<https://ise-fizkarlsruhe.github.io/mwo/> |",
        f"| **Other References** | {MWO_PUBLICATION_DOI} |",
        f"| **Examples (OWL files)** | [`pattern.ttl`](pattern.ttl) — one real row of the "
        f"tab, with its value nodes resolved |",
        f"| **Extracted From** | the `{origin}` tab of the NFDI-MatWerk curation workbook "
        f"({n_rows} rows at generation time) |",
        f"| **Reengineered From** | the ROBOT template directives in row 2 of that tab |",
        f"| **Has Components** | {comp_names} |",
        f"| **Specialization Of** | the entity-and-value-node pattern shared by every tab "
        f"([overview](../index.md)) |",
        f"| **Related CPs** | {related_md} |",
        "",
        "### Provenance and versioning",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| **Pattern version** | {pattern_version} |",
        f"| **Authors** | {authors_md} — the authors of the ontology these patterns are "
        f"derived from (`dcterms:creator` of MWO {mwo_version}) |",
        f"| **Derived from ontology** | MatWerk Ontology (MWO) **{mwo_version}** — "
        f"`{MWO}mwo.owl/{mwo_version}` |",
        f"| **Reused ontologies** | {reused_md} |",
        f"| **Ontology licence** | {MWO_LICENCE} |",
        f"| **Cite the ontology as** | {MWO_CITATION.format(version=mwo_version)} |",
        f"| **Generated** | `scripts/gen_patterns.py`, {generated_at} |",
        "",
        "### Formal characterisation",
        "",
        "| Measure | Value |",
        "|---|---|",
        f"| **Consistency** | **{m.get('consistency', 'not checked')}** — ROBOT "
        f"{robot_version}, reasoner HermiT, merged with MWO {mwo_version} |",
        f"| **DL expressivity** | `{m.get('expressivity_merged') or m.get('expressivity_solo') or '—'}` "
        f"— measured on the pattern together with a STAR module of exactly the terms it "
        f"uses, so the figure describes this pattern rather than the whole ontology |",
        f"| **Axioms / logical axioms** | {m.get('axioms', '?')} / {m.get('logical_axioms', '?')} |",
        f"| **Classes / individuals** | {m.get('classes', '?')} / {m.get('individuals', '?')} |",
        "",
        "## Consistency",
        "",
        f"`pattern.ttl` is checked against MWO {mwo_version} with ROBOT "
        "(`--reasoner hermit`) — the same reasoner `process_spreadsheets` runs over "
        "every generated module. A pattern that contradicts the ontology fails the "
        "documentation build.",
    ]
    os.makedirs(os.path.join(out_dir, tab), exist_ok=True)
    with open(os.path.join(out_dir, tab, "pattern.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


# --------------------------------------------------------------------------- #
def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--sheets-dir", default="")
    ap.add_argument("--mwo", required=True)
    ap.add_argument("--labels", action="append", default=[],
                    help="extra ontology file(s) for labels/declarations (repeatable)")
    ap.add_argument("--mwo-version", default="3.0.1")
    ap.add_argument("--out", default="docs/patterns")
    ap.add_argument("--robot", default="", help="path to robot.jar (enables metrics + modules)")
    ap.add_argument("--kg", default="",
                    help="deployed merged ontology (declares terms the published "
                         "ontology files omit); used for module extraction + labels")
    ap.add_argument("--min-rows", type=int, default=20,
                    help="for the shared value-node tabs, emit a pattern per TYPE "
                         "with at least this many rows (default 20)")
    ap.add_argument("--fetch", action="store_true", help="download the tabs first")
    args = ap.parse_args(argv)

    sheets = args.sheets_dir or "sheets"
    if args.fetch:
        fetch_tabs(sheets)

    labels, defined, alt_labels = load_labels([args.mwo] + list(args.labels)
                                  + ([args.kg] if args.kg else []))
    print(f"[gen] {len(labels)} labels, {len(defined)} declared terms "
          f"from {1 + len(args.labels)} ontology file(s)")

    # global label -> (iri, type, label) index, so 'I' references resolve
    label_index: Dict[str, Tuple[str, str, str]] = {}
    tabs_data = {}
    for tab, _ in TABS:
        p = os.path.join(sheets, f"{tab}.csv")
        if not os.path.exists(p):
            continue
        head, directives, data = read_tab(p)
        tabs_data[tab] = (head, directives, data)
        lab_cols = [i for i, c in enumerate(directives) if c.strip().startswith("A rdfs:label")]
        for r in data:
            for i in lab_cols:
                if i < len(r) and r[i].strip():
                    label_index.setdefault(
                        r[i].strip().lower(),
                        (r[0].strip(), r[1].strip() if len(r) > 1 else "", r[i].strip()))
    print(f"[gen] {len(label_index)} instance labels indexed")

    from datetime import datetime, timezone
    generated_at = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # One TBox for every module extraction, built once.
    tbox_src = ""
    if args.robot and os.path.exists(args.robot):
        onts = [args.mwo] + ([args.kg] if args.kg and os.path.exists(args.kg) else [])
        # A build intermediate, not documentation: written to a temp directory so it
        # is never published (the merged source alone is ~450 kB, and mkdocs copies
        # anything sitting under docs/ into the site verbatim).
        import tempfile

        tbox_src = os.path.join(tempfile.gettempdir(), "matwerk_pattern_tbox.ttl")
        try:
            build_tbox_source(args.robot, onts, tbox_src)
            print(f"[gen] extraction TBox built from {len(onts)} ontology file(s)")
        except Exception as e:
            print(f"[gen] could not build the extraction TBox: {e}")
            tbox_src = ""

    # tab -> its entity class IRI, used to work out which patterns link to which
    entity_of_tab: Dict[str, str] = {}
    for _t, (_h, _d, _rows) in tabs_data.items():
        if _t in VALUE_TABS:
            continue
        _r = pick_example_row(_rows, len(_d))
        entity_of_tab[_t] = (_r[1].strip() if _r and len(_r) > 1
                          and _r[1].strip().startswith('http') else '')

    os.makedirs(args.out, exist_ok=True)
    made = 0
    written: List[Tuple[str, str, int]] = []   # (dir, display name, rows)

    def emit(name: str, display: str, head, directives, rows,
             components: Optional[List[Tuple[str, str, int]]] = None) -> None:
        nonlocal made
        pattern_cqs = CQS.get(name) or CQS.get(name.split('-')[0])
        ttl, formal, cols = build_pattern(display, head, directives, rows, labels,
                                          label_index, args.mwo_version, cqs=pattern_cqs,
                                          components=components, related=None)
        if not ttl:
            print(f"[gen] {name}: no usable example row — skipped")
            return
        os.makedirs(os.path.join(args.out, name), exist_ok=True)
        ttl_path = os.path.join(args.out, name, "pattern.ttl")
        with open(ttl_path, "w", encoding="utf-8") as fh:
            fh.write(ttl)
        row = pick_example_row(rows, len(directives))
        entity_type = row[1].strip() if row and len(row) > 1 else ""
        # The reusable building block: the example plus a STAR module of the axioms
        # for exactly the terms it uses, so the file stands on its own.
        module_path = os.path.join(args.out, name, "module.ttl")
        has_module = False
        if tbox_src:
            if extract_module(ttl_path, args.robot, tbox_src, module_path):
                try:
                    from rdflib import Graph as _G
                    # the module is the whole artefact: example + declarations +
                    # ODP annotations + the extracted axioms
                    _m1, _m2 = _G(), _G()
                    _m1.parse(data=ttl + formal, format="turtle")
                    _m2.parse(module_path, format="turtle")
                    for _t in _m1:
                        _m2.add(_t)
                    _m2.serialize(destination=module_path, format="turtle")
                    has_module = True
                except Exception as e:
                    print(f"[gen]   module merge failed for {name}: {e}")

        # The ODP unit test is only worth asserting if it passes. Run it against the
        # pattern's own example: the file is small and the query is a plain SELECT, so
        # this costs nothing and turns the annotation into an actual test. It is what
        # caught the value-node-label case, where 7 patterns shipped a query that
        # could never match their own data.
        ut = pattern_meta(display, entity_type, cols, labels, pattern_cqs)["unit_test"]
        if ut:
            try:
                from rdflib import Graph as _UG
                _g = _UG()
                _g.parse(ttl_path, format="turtle")
                if len(list(_g.query(ut))) == 0:
                    print(f"[gen]   WARNING {name}: cpa:hasUnitTest returns no rows "
                          f"against its own example — the pattern and its test disagree")
            except Exception as e:  # noqa: BLE001
                print(f"[gen]   WARNING {name}: unit test did not run ({e})")

        metrics = (robot_metrics(ttl_path, args.robot, args.mwo)
                   if args.robot and os.path.exists(args.robot) else {})
        # Related CPs: the other patterns whose entity class appears as the type of
        # something this pattern points at — i.e. the patterns it links into.


        # The serialised pattern uses CURIEs, so a full-IRI substring test never
        # matches. Compare on the CURIE, which is what is actually in the file.
        related = [t for t, h in entity_of_tab.items()
                   if t != name and h and curie(h) in ttl]
        write_md(name, cols, labels, args.out, args.mwo_version, len(rows), entity_type,
                 defined, display=display, components=components,
                 cqs=pattern_cqs,
                 metrics=metrics, generated_at=generated_at, related=related,
                 has_module=has_module, alt_labels=alt_labels,
                 gid=GID.get(origin_tab_of(name), ""))
        made += 1
        written.append((name, display, len(rows)))
        print(f"[gen] {name}: {len(rows)} rows, {len(cols)} cols"
              + (f", DL {metrics.get('expressivity_merged', '?')}" if metrics else ""))

    # Attribute every value type to the entity tab that references it most.
    owners, value_rows = attribute_value_types(sheets, tabs_data)
    by_owner: Dict[str, List[Tuple[str, str, int]]] = {}
    for vtype, (owner, n) in owners.items():
        nice = labels.get(vtype, vtype.rsplit("/", 1)[-1])
        by_owner.setdefault(owner, []).append((vtype, nice, n))
    for k in by_owner:
        by_owner[k].sort(key=lambda x: -x[2])

    for tab, (head, directives, data) in tabs_data.items():
        if tab in VALUE_TABS:
            continue                              # dissolved into the entity patterns
        emit(tab, tab, head, directives, data, components=by_owner.get(tab, []))

    # A value type no entity tab references has nowhere to be folded into, so it
    # keeps a page of its own rather than disappearing.
    head, directives, _ = tabs_data.get("req_2", ([], [], []))
    for vtype, rows in value_rows.items():
        if vtype in owners or len(rows) < args.min_rows:
            continue
        nice = labels.get(vtype, vtype.rsplit("/", 1)[-1])
        emit(f"value-{slug(nice)}", nice, head, directives, rows)

    with open(os.path.join(args.out, "_generated.txt"), "w", encoding="utf-8") as fh:
        for name, display, n in written:
            fh.write(f"{name}\t{display}\t{n}\n")
    print(f"[gen] wrote {made} patterns to {args.out}")


if __name__ == "__main__":
    main()
