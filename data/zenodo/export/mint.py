import time
from typing import Dict, Set
from rdflib.term import URIRef
from .constants import MSEKG_BASE

def _existing_iris_in(*dicts) -> Set[str]:
    taken: Set[str] = set()
    for d in dicts:
        if not isinstance(d, dict):
            continue
        for v in d.values():
            if isinstance(v, list):
                for x in v:
                    if isinstance(x, str):
                        taken.add(x)
            elif isinstance(v, str):
                taken.add(v)
    return taken

def _now_millis() -> int:
    return int(time.time() * 1000)

def _mint_timestamp_str(prefix: str, taken: Set[str], session_nodes: Set[str]) -> str:
    ts = _now_millis()
    while True:
        iri = f"{prefix}{ts}"
        if iri not in taken and iri not in session_nodes:
            session_nodes.add(iri)
            return iri
        ts += 1

def _mint_timestamp_uri(prefix: str, taken: Set[str], session_nodes: Set[str]) -> URIRef:
    return URIRef(_mint_timestamp_str(prefix, taken, session_nodes))

def get_or_mint_instance(state: Dict, rec_key: str, session_nodes: Set[str]) -> URIRef:
    instances = state.setdefault("instances", {})
    existing = instances.get(rec_key)
    if isinstance(existing, str) and existing:
        return URIRef(existing)
    taken = _existing_iris_in(instances, state.get("nodes", {}), state.get("file_graphs", {}), state.get("file_ids", {}))
    iri = _mint_timestamp_uri(f"{MSEKG_BASE}v/", taken, session_nodes)
    instances[rec_key] = str(iri)
    return iri

def get_or_mint_node_at(state: Dict, rec_key: str, index: int, session_nodes: Set[str]) -> URIRef:
    if index < 0:
        raise ValueError("index must be >= 0")
    nodes = state.setdefault("nodes", {})
    lst = nodes.setdefault(rec_key, [])
    if index < len(lst) and isinstance(lst[index], str) and lst[index]:
        return URIRef(lst[index])
    taken = _existing_iris_in(state.get("instances", {}), nodes, state.get("file_graphs", {}), state.get("file_ids", {}))
    iri = _mint_timestamp_uri(f"{MSEKG_BASE}v/", taken, session_nodes)
    while len(lst) <= index:
        lst.append(None)
    lst[index] = str(iri)
    return iri

def get_or_mint_file_graph(state: Dict, rec_key: str, file_key: str, session_nodes: Set[str]) -> URIRef:
    fg = state.setdefault("file_graphs", {})
    k = f"{rec_key}|{file_key}"
    existing = fg.get(k)
    if isinstance(existing, str) and existing:
        return URIRef(existing)
    taken = _existing_iris_in(state.get("instances", {}), state.get("nodes", {}), fg, state.get("file_ids", {}))
    iri = _mint_timestamp_uri(f"{MSEKG_BASE}g/", taken, session_nodes)
    fg[k] = str(iri)
    return iri

def get_or_mint_file_identifier(state: Dict, rec_key: str, file_key: str, session_nodes: Set[str]) -> str:
    fids = state.setdefault("file_ids", {})
    k = f"{rec_key}|{file_key}"
    existing = fids.get(k)
    if isinstance(existing, str) and existing:
        return existing
    taken = set(fids.values()) | _existing_iris_in(state.get("instances", {}), state.get("nodes", {}), state.get("file_graphs", {}))
    ident = _mint_timestamp_str(f"{MSEKG_BASE}v/", taken, session_nodes)
    fids[k] = ident
    return ident
