import re
from urllib.parse import urlparse, urlunparse, quote
from rdflib.graph import ConjunctiveGraph, Graph
from rdflib.term import URIRef

_UNSAFE_HTTP_CHARS = re.compile(r"[^\w\-\.\~:/\?\#\[\]@!\$&'\(\)\*\+,;=%]|\s")

def iri_to_uri(u: str) -> str:
    p = urlparse(u)
    path = quote(p.path, safe="/%:@!$&'*+,;=")
    query = quote(p.query, safe="=&%:@!$'(),*+;$/?")
    fragment = quote(p.fragment, safe="")
    return urlunparse((p.scheme, p.netloc, path, p.params, query, fragment))

def _needs_encoding(u: str) -> bool:
    return (u.startswith("http://") or u.startswith("https://")) and bool(_UNSAFE_HTTP_CHARS.search(u))

def normalize_graph_uris(g: ConjunctiveGraph) -> None:
    to_add = []
    to_remove = []
    for ctx in g.contexts():
        for s, p, o in ctx.triples((None, None, None)):
            s2 = URIRef(iri_to_uri(str(s))) if isinstance(s, URIRef) and _needs_encoding(str(s)) else s
            p2 = URIRef(iri_to_uri(str(p))) if isinstance(p, URIRef) and _needs_encoding(str(p)) else p
            o2 = URIRef(iri_to_uri(str(o))) if isinstance(o, URIRef) and _needs_encoding(str(o)) else o
            if (s2, p2, o2) != (s, p, o):
                to_remove.append((ctx, (s, p, o)))
                to_add.append((ctx, (s2, p2, o2)))
    for ctx, t in to_remove:
        ctx.remove(t)
    for ctx, t in to_add:
        ctx.add(t)

def safe_slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"^doi:\s*", "", s)
    s = s.replace("/", "_")
    s = re.sub(r"[^a-z0-9._-]+", "", s)
    return s or "id"

def safe_uri_segment(s: str) -> str:
    return quote(s, safe="._-")

def safe_uri(s: str) -> URIRef:
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^\w\-\.]+", "", s)
    return URIRef(s)
