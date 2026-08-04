# rdf_harvester/discover.py
"""Find the GitHub repositories registered in the MatWerk KG.

A repository is registered exactly like any URL-bearing entity: the subject is
denoted-by (``IAO_0000235``) a URL node whose ``NFDI_0001008`` value is a
github.com URL — the same shape the Zenodo harvester matches on. We normalise
each URL to ``owner/repo`` and keep the KG subject IRI: that subject becomes the
**repository node** the harvested file-graphs are linked to, so the connection
to the registered repository is preserved (bare ``github.com/<org>`` URLs, with
no repository, are skipped).
"""
from __future__ import annotations

import re
from typing import Dict, Optional

SPARQL_GITHUB_REPOS = """
PREFIX obo:  <http://purl.obolibrary.org/obo/>
PREFIX nfdi: <https://nfdi.fiz-karlsruhe.de/ontology/>
SELECT DISTINCT ?s ?u WHERE {
  ?s obo:IAO_0000235 ?urlNode .
  ?urlNode nfdi:NFDI_0001008 ?u .
  FILTER(isIRI(?s))
  FILTER(CONTAINS(LCASE(STR(?u)), "github.com"))
}
"""

# owner/repo out of any github.com URL form (https, git@, /tree/…, .git, …).
_GITHUB_RE = re.compile(
    r"github\.com[/:]+"
    r"([A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)"      # owner
    r"/([A-Za-z0-9._-]+?)"                              # repo (non-greedy)
    r"(?:\.git)?(?:[/#?].*)?$",
    re.IGNORECASE,
)
# path segments that are never a repository name (URL was an org/feature page)
_NOT_A_REPO = {"", "tree", "blob", "releases", "issues", "pull", "wiki",
               "actions", "settings", "orgs", "sponsors", "topics"}


def normalize_owner_repo(url: str) -> Optional[str]:
    """``https://github.com/ICAMS/calphy/tree/master`` -> ``ICAMS/calphy``;
    bare ``https://github.com/abinit`` (no repo) -> ``None``."""
    if not url:
        return None
    m = _GITHUB_RE.search(url.strip())
    if not m:
        return None
    owner, repo = m.group(1), m.group(2)
    repo = re.sub(r"\.git$", "", repo, flags=re.IGNORECASE)
    if not owner or repo.lower() in _NOT_A_REPO:
        return None
    return f"{owner}/{repo}"


def discover_repos_from_kg(kg_ttl_path: str) -> Dict[str, str]:
    """Parse the merged KG TTL and return ``{owner/repo: kg_subject_iri}``.

    If several KG subjects point at the same repository, the first one wins
    (deterministic by SPARQL DISTINCT ordering); duplicates are logged.
    """
    from rdflib import Graph

    g = Graph()
    g.parse(kg_ttl_path)
    out: Dict[str, str] = {}
    for row in g.query(SPARQL_GITHUB_REPOS):
        orr = normalize_owner_repo(str(row.u))
        if not orr:
            continue
        out.setdefault(orr, str(row.s))
    print(f"[discover] {len(out)} github repositories registered in the KG")
    return out
