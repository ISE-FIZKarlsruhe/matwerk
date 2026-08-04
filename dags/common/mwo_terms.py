# common/mwo_terms.py
"""Single source of truth for the ontology terms the DAGs write, per MWO version.

MWO 3.0.2 **deprecates ten of the classes the workbook and the harvesters use** and
states a replacement for nine of them (``IAO_0100001 term replaced by``). Writing a
deprecated class is not an error a reasoner catches — it silently produces data that
no longer matches the ontology — so the version must be an explicit input rather
than a constant scattered across DAGs.

Usage::

    from common.mwo_terms import terms
    T = terms()                 # reads the matwerk_mwo_version Airflow Variable
    T.DATASET                   # 3.0.1 -> nfdi:NFDI_0000009
                                # 3.0.2 -> obo:IAO_0001000

``matwerk_mwo_version`` (Airflow Variable) selects the vocabulary; it defaults to
``3.0.1``, which is what the server currently runs, so behaviour is unchanged until
the variable is set.

Verified against mwo-full.ttl v3.0.1 and v3.0.2 (see REPLACEMENTS below: every entry
was read from the ontology's own ``owl:deprecated`` + ``IAO_0100001`` assertions).
"""
from __future__ import annotations

from dataclasses import dataclass

NFDI = "https://nfdi.fiz-karlsruhe.de/ontology/"
OBO = "http://purl.obolibrary.org/obo/"
MWO = "http://purls.helmholtz-metadaten.de/mwo/"

DEFAULT_MWO_VERSION = "3.0.1"
MWO_ONTOLOGY_IRI = "http://purls.helmholtz-metadaten.de/mwo/mwo.owl"

# Classes deprecated in 3.0.2 -> their stated replacement (IAO_0100001).
# The row counts are how many production-spreadsheet rows use each today, so the
# migration effort is visible next to the mapping.
REPLACEMENTS_3_0_2: dict[str, str | None] = {
    NFDI + "NFDI_0000014": OBO + "IAO_0000444",   # publishing process   (530 rows)
    NFDI + "NFDI_0000190": OBO + "IAO_0000311",   # publication          (462 rows)
    NFDI + "NFDI_0000022": OBO + "IAO_0000310",   # document             ( 92 rows)
    NFDI + "NFDI_0000009": OBO + "IAO_0001000",   # dataset              ( 49 rows)
    NFDI + "NFDI_0010041": OBO + "IAO_0000595",   # software script      ( 21 rows)
    NFDI + "NFDI_0000218": OBO + "IAO_0000594",   # software application ( 20 rows)
    NFDI + "NFDI_0000140": OBO + "IAO_0000593",   # software library     ( 19 rows)
    NFDI + "NFDI_0010040": OBO + "IAO_0000592",   # software module      ( 18 rows)
    NFDI + "NFDI_0010039": OBO + "IAO_0000591",   # software method      ( 17 rows)
    NFDI + "NFDI_0001054": None,                  # metadata specification (20 rows) — NO
                                                  # replacement stated; needs a curation
                                                  # decision before upgrading.
}

# Terms REMOVED outright in 3.0.2 (not merely deprecated).
REMOVED_3_0_2 = {
    MWO + "MWO_0001119",    # measurement unit label of
    OBO + "OBI_0000011",    # planned process
}


@dataclass(frozen=True)
class Terms:
    """The class/property IRIs the DAGs write, resolved for one MWO version."""

    version: str

    # --- classes that CHANGE between versions ---
    DATASET: str
    PUBLICATION: str
    SOFTWARE_APPLICATION: str

    # --- classes stable across 3.0.1 / 3.0.2 (verified present in both) ---
    # The description node is `textual description`. It is emphatically NOT
    # NFDI_0000018 ("academic event"): that IRI is one digit away, sits on the
    # right-hand side of `denoted by` which requires information rather than an
    # event, and so makes every graph carrying it logically inconsistent.
    # See docs/shapes/graph-metadata/pattern.md.
    TEXTUAL_DESCRIPTION: str = NFDI + "NFDI_0001018"
    TITLE: str = NFDI + "NFDI_0001019"
    WEBSITE: str = NFDI + "NFDI_0000223"
    FILE: str = NFDI + "NFDI_0000027"
    DATA_REPOSITORY: str = NFDI + "NFDI_0001201"
    SPARQL_ENDPOINT: str = NFDI + "NFDI_0001095"
    IDENTIFIER: str = OBO + "IAO_0020000"

    # --- properties (stable) ---
    DENOTED_BY: str = OBO + "IAO_0000235"
    HAS_VALUE: str = NFDI + "NFDI_0001007"
    HAS_URL: str = NFDI + "NFDI_0001008"
    HAS_LICENSE: str = NFDI + "NFDI_0000142"
    HAS_PART: str = OBO + "BFO_0000051"
    PART_OF: str = OBO + "BFO_0000050"
    IS_OUTPUT_OF: str = OBO + "RO_0002353"
    HAS_INPUT: str = OBO + "RO_0002233"
    HAS_PARTICIPANT: str = OBO + "RO_0000057"

    # --- BFO process / time (stable) ---
    PROCESS: str = OBO + "BFO_0000015"
    OCCUPIES_TR: str = OBO + "BFO_0000199"
    TR_1D: str = OBO + "BFO_0000038"
    TR_0D: str = OBO + "BFO_0000148"
    HAS_FIRST_INSTANT: str = OBO + "BFO_0000222"
    HAS_LAST_INSTANT: str = OBO + "BFO_0000224"
    IN_XSD_DATETIMESTAMP: str = "http://www.w3.org/2006/time#inXSDDateTimeStamp"

    @property
    def version_iri(self) -> str:
        return f"{MWO_ONTOLOGY_IRI}/{self.version}"

    def migrate(self, iri: str) -> str:
        """Map a 3.0.1 class onto its 3.0.2 replacement when running 3.0.2."""
        if self.version == DEFAULT_MWO_VERSION:
            return iri
        return REPLACEMENTS_3_0_2.get(iri) or iri


_V301 = dict(
    DATASET=NFDI + "NFDI_0000009",
    PUBLICATION=NFDI + "NFDI_0000190",
    SOFTWARE_APPLICATION=NFDI + "NFDI_0000218",
)
_V302 = dict(
    DATASET=OBO + "IAO_0001000",
    PUBLICATION=OBO + "IAO_0000311",
    SOFTWARE_APPLICATION=OBO + "IAO_0000594",
)


def terms(version: str | None = None) -> Terms:
    """Resolve the vocabulary for ``version`` (default: the Airflow Variable)."""
    if version is None:
        version = _version_from_airflow()
    return Terms(version=version, **(_V302 if version.startswith("3.0.2") else _V301))


def _version_from_airflow() -> str:
    try:
        from airflow.sdk import Variable

        return Variable.get("matwerk_mwo_version") or DEFAULT_MWO_VERSION
    except Exception:
        return DEFAULT_MWO_VERSION
