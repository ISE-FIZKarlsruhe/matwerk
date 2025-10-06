from typing import Dict, List, Optional, Set, Tuple
from rdflib.graph import Graph
from rdflib.namespace import RDF, RDFS
from rdflib.term import Literal, URIRef

NFDI_DATASET = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009"
IAO_0000235 = "http://purl.obolibrary.org/obo/IAO_0000235"
OBI_0002135 = "http://purl.obolibrary.org/obo/OBI_0002135"
NFDI_DOI_PRED = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006"

from .mint import get_or_mint_node_at
from .zenodo_api import get_metadata

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
    g.add((subj, RDF.type, URIRef(NFDI_DATASET)))

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

