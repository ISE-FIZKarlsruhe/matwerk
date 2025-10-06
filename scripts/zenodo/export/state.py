import json
from pathlib import Path
from typing import Dict, List

def record_key(rec: dict) -> str:
    return str(rec.get("conceptrecid") or rec.get("id"))

def _flatten_legacy_nodes(nodes: dict) -> Dict[str, List[str]]:
    per_record: Dict[str, List[str]] = {}
    if not isinstance(nodes, dict):
        return {}
    if any('|' in k for k in nodes.keys() if isinstance(k, str)):
        grouped: Dict[str, List[tuple]] = {}
        for k, iri in nodes.items():
            if not isinstance(iri, str) or not isinstance(k, str):
                continue
            rec, _, slot = k.partition('|')
            grouped.setdefault(rec, []).append((slot, iri))
        for rec, pairs in grouped.items():
            pairs.sort(key=lambda x: x[0])
            per_record[rec] = [iri for _slot, iri in pairs]
        return per_record
    tmp: Dict[str, List[tuple]] = {}
    for slot, mapping in nodes.items():
        if isinstance(mapping, dict):
            for rec, iri in mapping.items():
                if isinstance(iri, str):
                    tmp.setdefault(str(rec), []).append((str(slot), iri))
    for rec, pairs in tmp.items():
        pairs.sort(key=lambda x: x[0])
        per_record[rec] = [iri for _slot, iri in pairs]
    return per_record

def _normalize_state(raw: dict) -> dict:
    """
    Normalize legacy fields AND preserve future/extension fields like 'by_record'.
    """
    # Defaults
    base = {
        "instances": {},
        "nodes": {},
        "file_graphs": {},
        "file_ids": {},
        "graphs": {},
        # New/extended sections we want to round-trip
        "by_record": {},   # <- for snapshot IRIs per (record, func)
    }

    if not isinstance(raw, dict):
        return base

    # graphs (legacy)
    if isinstance(raw.get("graphs"), dict):
        for k, v in raw["graphs"].items():
            if isinstance(v, str):
                base["graphs"][str(k)] = v
    else:
        # super-legacy top-level flat mapping
        for k, v in raw.items():
            if isinstance(v, str):
                base["graphs"][str(k)] = v

    # instances
    if isinstance(raw.get("instances"), dict):
        for k, v in raw["instances"].items():
            if isinstance(v, str):
                base["instances"][str(k)] = v

    # nodes (supports legacy shapes)
    rn = raw.get("nodes")
    if isinstance(rn, dict):
        if any(isinstance(v, dict) for v in rn.values()) or any('|' in k for k in rn.keys() if isinstance(k, str)):
            base["nodes"] = _flatten_legacy_nodes(rn)
        else:
            for k, v in rn.items():
                if isinstance(v, list):
                    base["nodes"][str(k)] = [x for x in v if isinstance(x, str)]
                elif isinstance(v, str):
                    base["nodes"][str(k)] = [v]

    # file_graphs
    fg = raw.get("file_graphs")
    if isinstance(fg, dict):
        for k, v in fg.items():
            if isinstance(v, str):
                base["file_graphs"][str(k)] = v

    # file_ids
    fid = raw.get("file_ids")
    if isinstance(fid, dict):
        for k, v in fid.items():
            if isinstance(v, str):
                base["file_ids"][str(k)] = v

    # --- Preserve extensions verbatim ---
    if isinstance(raw.get("by_record"), dict):
        base["by_record"] = raw["by_record"]

    return base

def load_state(path: Path) -> dict:
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return _normalize_state(data)
        except Exception:
            pass
    return _normalize_state({})  # ensures new keys exist

def save_state(path: Path, state: dict) -> None:
    # Make sure the directory exists
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
