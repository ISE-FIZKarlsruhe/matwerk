from pathlib import Path
from typing import Optional
from rdflib.graph import ConjunctiveGraph
from rdflib.term import URIRef

RDF_EXT_MAP = {
    ".ttl": "turtle",
    ".trig": "trig",
    ".nt": "nt",
    ".nq": "nquads",
    ".n3": "n3",
    ".owl": "xml",
    ".rdf": "xml",
    ".xml": "xml",
    ".jsonld": "json-ld",
}

RDF_MIME_MAP = {
    "text/turtle": "turtle",
    "application/trig": "trig",
    "application/n-triples": "nt",
    "application/n-quads": "nquads",
    "text/n3": "n3",
    "application/rdf+xml": "xml",
    "application/ld+json": "json-ld",
}

def guess_format(path: Path, content_type: Optional[str] = None) -> Optional[str]:
    if content_type:
        mt = content_type.split(";")[0].strip().lower()
        if mt in RDF_MIME_MAP:
            return RDF_MIME_MAP[mt]
    return RDF_EXT_MAP.get(path.suffix.lower())

def import_rdf_into_named_graph(g: ConjunctiveGraph, file_path: Path, graph_iri: URIRef, content_type: Optional[str] = None) -> bool:
    fmt = guess_format(file_path, content_type)
    if not fmt:
        return False
    ctx = g.get_context(graph_iri)
    try:
        ctx.parse(file_path.as_posix(), format=fmt)
        return True
    except Exception as e:
        print(f"  ! Failed to parse {file_path} as {fmt}: {e}")
        return False
