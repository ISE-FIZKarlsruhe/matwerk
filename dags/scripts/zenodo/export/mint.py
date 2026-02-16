from rdflib.term import URIRef
from urllib.parse import quote
from .constants import MSEKG_BASE

def _q(s: str) -> str:
    # encode everything except a few safe characters
    return quote(str(s), safe=":@-._~")

def get_or_mint_instance(state, rec_key: str, session_nodes) -> URIRef:
    iri = URIRef(f"{MSEKG_BASE}zenodo/concept/{_q(rec_key)}")
    session_nodes.add(str(iri))
    return iri

def get_or_mint_node_at(state, rec_key: str, index: int, session_nodes) -> URIRef:
    if index < 0:
        raise ValueError("index must be >= 0")
    iri = URIRef(f"{MSEKG_BASE}zenodo/concept/{_q(rec_key)}/node/{index}")
    session_nodes.add(str(iri))
    return iri

def get_or_mint_file_identifier(state, rec_key: str, file_key: str, session_nodes) -> str:
    iri = f"{MSEKG_BASE}zenodo/concept/{_q(rec_key)}/file/{_q(file_key)}"
    session_nodes.add(iri)
    return iri

def get_or_mint_file_graph(state, rec_key: str, file_key: str, session_nodes) -> URIRef:
    iri = URIRef(f"{MSEKG_BASE}zenodo/concept/{_q(rec_key)}/graph/{_q(file_key)}")
    session_nodes.add(str(iri))
    return iri
