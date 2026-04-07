# common/zenodo_deposit.py
from __future__ import annotations

import os
import requests

from airflow.exceptions import AirflowFailException


def _get_zenodo_config() -> tuple[str, str]:
    """Return (base_url, access_token) from Airflow Variables."""
    from airflow.sdk import Variable

    base_url = Variable.get("matwerk_zenodo_base_url", default="https://zenodo.org/api")
    token = Variable.get("matwerk_zenodo_token")
    if not token:
        raise AirflowFailException("Missing Airflow Variable: matwerk_zenodo_token")
    return base_url.rstrip("/"), token


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def get_or_create_deposit(concept_id: str | None) -> dict:
    """
    If concept_id is set, create a new version of that deposit.
    Otherwise create a brand-new deposit.
    Returns the deposit JSON (with 'id', 'links', 'metadata', etc.).
    """
    base_url, token = _get_zenodo_config()
    hdrs = _headers(token)

    if concept_id:
        # Create new version from latest published record under this concept
        url = f"{base_url}/deposit/depositions/{concept_id}/actions/newversion"
        r = requests.post(url, headers=hdrs, timeout=60)
        if r.status_code not in (200, 201):
            raise AirflowFailException(
                f"Zenodo newversion failed ({r.status_code}): {r.text[:500]}"
            )
        latest = r.json()
        # The new draft lives at the 'latest_draft' link
        draft_url = latest["links"]["latest_draft"]
        r2 = requests.get(draft_url, headers=hdrs, timeout=60)
        r2.raise_for_status()
        deposit = r2.json()
        # Delete old files from the new draft so we upload fresh
        for f in deposit.get("files", []):
            del_url = f"{deposit['links']['self']}/files/{f['id']}"
            requests.delete(del_url, headers=hdrs, timeout=60)
        return deposit
    else:
        url = f"{base_url}/deposit/depositions"
        r = requests.post(url, json={}, headers=hdrs, timeout=60)
        if r.status_code not in (200, 201):
            raise AirflowFailException(
                f"Zenodo create deposit failed ({r.status_code}): {r.text[:500]}"
            )
        return r.json()


def upload_file(deposit: dict, file_path: str, filename: str | None = None) -> dict:
    """Upload a single file to a Zenodo deposit draft."""
    _, token = _get_zenodo_config()
    hdrs = _headers(token)

    bucket_url = deposit["links"]["bucket"]
    fname = filename or os.path.basename(file_path)

    with open(file_path, "rb") as fp:
        r = requests.put(
            f"{bucket_url}/{fname}",
            data=fp,
            headers=hdrs,
            timeout=(10, 600),
        )
    if r.status_code not in (200, 201):
        raise AirflowFailException(
            f"Zenodo file upload failed ({r.status_code}): {r.text[:500]}"
        )
    return r.json()


def set_metadata(deposit: dict, metadata: dict) -> dict:
    """Update the deposit metadata (title, description, creators, etc.)."""
    base_url, token = _get_zenodo_config()
    hdrs = {**_headers(token), "Content-Type": "application/json"}

    url = deposit["links"]["self"]
    r = requests.put(url, json={"metadata": metadata}, headers=hdrs, timeout=60)
    if r.status_code != 200:
        raise AirflowFailException(
            f"Zenodo metadata update failed ({r.status_code}): {r.text[:500]}"
        )
    return r.json()


def publish(deposit: dict) -> dict:
    """Publish the deposit (mints a DOI)."""
    _, token = _get_zenodo_config()
    hdrs = _headers(token)

    url = deposit["links"]["publish"]
    r = requests.post(url, headers=hdrs, timeout=60)
    if r.status_code != 202:
        raise AirflowFailException(
            f"Zenodo publish failed ({r.status_code}): {r.text[:500]}"
        )
    return r.json()


def build_zenodo_metadata(
    version: str,
    publish_date: str,
    description_html: str,
) -> dict:
    """
    Build the metadata dict for a Zenodo deposit.
    """
    return {
        "title": f"Materials Science and Engineering (MSE) Knowledge Graph - RDF Dumps (v{version})",
        "upload_type": "dataset",
        "description": description_html,
        "creators": [
            {"name": "Sack, Harald", "affiliation": "FIZ Karlsruhe – Leibniz Institute for Information Infrastructure; Karlsruhe Institute of Technology", "orcid": "0000-0001-7069-9804"},
            {"name": "Waitelonis, Jörg", "affiliation": "FIZ Karlsruhe – Leibniz Institute for Information Infrastructure", "orcid": "0000-0001-7192-7143"},
            {"name": "Norouzi, Ebrahim", "affiliation": "FIZ Karlsruhe – Leibniz Institute for Information Infrastructure", "orcid": "0000-0002-2691-6995"},
            {"name": "Beygi Nasrabadi, Hossein", "affiliation": "FIZ Karlsruhe – Leibniz Institute for Information Infrastructure", "orcid": "0000-0002-3092-0532"},
        ],
        "contributors": [
            {"name": "NFDI-MatWerk Community", "type": "Other"},
        ],
        "access_right": "open",
        "license": "MIT",
        "version": version,
        "publication_date": publish_date,
        "keywords": [
            "NFDI", "NFDI-MatWerk", "knowledge graph", "RDF",
            "materials science and engineering", "linked data",
            "ontology", "MSE-KG", "SPARQL",
        ],
        "related_identifiers": [
            {
                "identifier": "https://nfdi.fiz-karlsruhe.de/matwerk/",
                "relation": "isSupplementTo",
                "resource_type": "dataset",
            },
            {
                "identifier": "https://github.com/ISE-FIZKarlsruhe/matwerk",
                "relation": "isSupplementTo",
                "resource_type": "software",
            },
            {
                "identifier": "http://purls.helmholtz-metadaten.de/mwo/mwo.owl",
                "relation": "isDocumentedBy",
                "resource_type": "other",
            },
            {
                "identifier": "https://ise-fizkarlsruhe.github.io/nfdicore/",
                "relation": "isDocumentedBy",
                "resource_type": "other",
            },
        ],
        "communities": [
            {"identifier": "nfdi-matwerk"},
        ],
    }
