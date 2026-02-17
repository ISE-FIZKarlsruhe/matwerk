from typing import Dict, List, Optional, Set, Tuple
from rdflib.graph import Graph
from rdflib.namespace import RDF, RDFS
from rdflib.term import Literal, URIRef

NFDI_DATASET      = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009"
NFDI_PUBLICATION  = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000190"
NFDI_LECTURE = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0010022"
NFDI_SOFTWARE = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000198"

IAO_0000235 = "http://purl.obolibrary.org/obo/IAO_0000235"
OBI_0002135 = "http://purl.obolibrary.org/obo/OBI_0002135"
NFDI_DOI_PRED = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006"

from .mint import get_or_mint_node_at
from .zenodo_api import get_metadata

def class_iri_for_zenodo_record(meta: dict) -> str:
    """
    Decide which class IRI to use based on Zenodo metadata.
    """

    rt = meta.get("resource_type") or {}
    rt_type = rt.get("type")

    upload_type = meta.get("upload_type")

    record_type = (rt_type or upload_type or "").lower()
    print(f"Determining class IRI for record type: {record_type}")
    #exit()

    if record_type in {"dataset"}:
        return NFDI_DATASET

    if record_type in {
        "publication",
        "article",
        "conferencepaper",
        "book",
        "section",
        "thesis",
        "report",
        "workingpaper",
        "preprint",
        "softwaredocumentation",
    }:
        return NFDI_PUBLICATION

    if record_type in {
        "presentation",
        "poster",
        "lesson",
    }:
        return NFDI_LECTURE
    
    if record_type in {
        "software",
    }:
        return NFDI_SOFTWARE

    # Fallback
    return NFDI_DATASET

def link_value_node_at_index(
    g: Graph,
    subject_uri: URIRef,
    link_predicate_uri: str,
    *,
    state: Dict,
    rec_key: str,
    index: int,
    session_nodes: Set[str],
    class_uri: Optional[str] = None,
    labels: Optional[List[str]] = None,
    literal_props: Optional[List[Tuple[str, str]]] = None,
) -> URIRef:
    node_uri = get_or_mint_node_at(state, rec_key, index, session_nodes)
    g.add((subject_uri, URIRef(link_predicate_uri), node_uri))
    if class_uri:
        g.add((node_uri, RDF.type, URIRef(class_uri)))
    if labels:
        for lab in labels:
            if lab:
                g.add((node_uri, RDFS.label, Literal(lab)))
    if literal_props:
        for pred, val in literal_props:
            if val is not None:
                g.add((node_uri, URIRef(pred), Literal(val)))
    return node_uri

def build_record_in_default_graph(
    g: Graph,
    rec: dict,
    instance_uri: URIRef,
    state: Dict,
    rec_key: str,
    session_nodes: Set[str],
) -> None:
    meta = get_metadata(rec)
    subj = instance_uri
    
    class_iri = class_iri_for_zenodo_record(meta)
    g.add((subj, RDF.type, URIRef(class_iri)))

    title = meta.get("title")
    if title:
        # Reserve index 0 for title node (generic pattern)
        link_value_node_at_index(
            g,
            subj,
            IAO_0000235,
            state=state,
            rec_key=rec_key,
            index=0,
            session_nodes=session_nodes,
            labels=[title],
            literal_props=[(OBI_0002135, title)],
        )

    g.add((subj, RDFS.label, Literal("https://zenodo.org/records/" + rec_key)))
    #exit()
    #doi = meta.get("doi") or rec.get("doi")
    #if doi:
        #g.add((subj, URIRef(NFDI_DOI_PRED), Literal(doi)))

