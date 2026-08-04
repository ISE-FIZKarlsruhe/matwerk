# rdf_harvester/select.py
"""Decide which RDF files in a release are the data worth harvesting.

Fully automatic — there is no per-repository configuration. Two rules:

1. **Released artifacts, not build inputs.** If the release ships RDF *assets*,
   those are the intentional artifacts and we take them. Otherwise we look at the
   repository tree at the release tag and, when the repository has RDF at its
   root, take only the root files: the OBO convention is that the built release
   artifacts live at the repository root (``cto-full.ttl``, ``pmdco-full.ttl``)
   while ``src/ontology/`` holds the edit file, imports and other build inputs.

2. **One graph per artifact, not per serialisation.** OBO-style repos publish the
   same ontology many times over — ``cto.ttl``, ``cto-full.ttl``, ``cto-base.ttl``,
   ``cto-simple.ttl`` plus an ``.owl`` of each. Those are variants of a single
   artifact, so they are grouped by stem and the **full Turtle** version wins.
   Genuinely different RDF files (different stems) are left alone and each still
   becomes its own named graph.
"""
from __future__ import annotations

import posixpath
import re
from typing import Dict, List, Tuple

# Variant suffixes used by OBO-style releases, best first. "" = the plain
# `<ont>.ttl` release. Lower rank wins.
VARIANT_RANK: Dict[str, int] = {
    "full": 0,          # full axiomatisation incl. imports — what we want
    "": 1,              # plain <ont>.ttl (the main OBO release artifact)
    "merged": 2,
    "non-classified": 3,
    "simple": 4,
    "minimal": 5,
    "basic": 6,
    "base": 7,          # this ontology's own axioms only, no imports
}
# Serialisation preference, best first ("the full **ttl** version").
FORMAT_RANK: Dict[str, int] = {
    ".ttl": 0, ".owl": 1, ".rdf": 2, ".nt": 3, ".trig": 4,
    ".nq": 5, ".jsonld": 6, ".n3": 7,
}
# Tree directories that are build inputs / fixtures, never released data.
_BUILD_INPUT_RE = re.compile(
    r"(^|/)(imports|components|utils|scripts|test|tests|examples|node_modules|\.github)(/|$)",
    re.IGNORECASE,
)

_VARIANT_RE = re.compile(
    r"^(?P<stem>.+?)-(?P<variant>%s)$" % "|".join(v for v in VARIANT_RANK if v),
    re.IGNORECASE,
)


def split_variant(basename: str) -> Tuple[str, str, str]:
    """``pmdco-full.ttl`` -> ``("pmdco", "full", ".ttl")``;
    ``pmdco.ttl`` -> ``("pmdco", "", ".ttl")``;
    ``dataset-2024.ttl`` -> ``("dataset-2024", "", ".ttl")`` (not a known variant)."""
    stem_ext = posixpath.splitext(basename)
    name, ext = stem_ext[0], stem_ext[1].lower()
    m = _VARIANT_RE.match(name)
    if m:
        return m.group("stem"), m.group("variant").lower(), ext
    return name, "", ext


def _rank(path: str) -> Tuple[int, int, int]:
    _stem, variant, ext = split_variant(posixpath.basename(path))
    return (VARIANT_RANK.get(variant, 50), FORMAT_RANK.get(ext, 50), len(path))


def select_artifacts(candidates: List[dict]) -> List[dict]:
    """Pick the artifacts to harvest from a release's RDF candidates.

    ``candidates`` are dicts with ``path``/``source``/``download_url``
    (``source`` is ``"asset"`` or ``"tree"``). Returns the chosen subset.
    """
    if not candidates:
        return []

    # Rule 1: assets are the intentional release; else fall back to the tree.
    assets = [c for c in candidates if c.get("source") == "asset"]
    if assets:
        pool = assets
    else:
        tree = [c for c in candidates if not _BUILD_INPUT_RE.search(c["path"])]
        if not tree:
            tree = list(candidates)
        root = [c for c in tree if not posixpath.dirname(c["path"])]
        pool = root or tree

    # Rule 2: group variants of one artifact; keep the best (full ttl).
    groups: Dict[Tuple[str, str], List[dict]] = {}
    for c in pool:
        d = posixpath.dirname(c["path"])
        stem, _variant, _ext = split_variant(posixpath.basename(c["path"]))
        groups.setdefault((d, stem.lower()), []).append(c)

    chosen = [min(g, key=lambda c: _rank(c["path"])) for g in groups.values()]
    return sorted(chosen, key=lambda c: c["path"])
