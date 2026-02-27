import csv
import os
import re
import uuid
import zipfile
from datetime import datetime

# =========================
# CONFIG
# =========================
IN_CSV = "harvest_output/full_metadata.csv"
OUT_DIR = "robot_templates"
BASE_IRI = "https://nfdi.fiz-karlsruhe.de/matwerk/msekg/"
NS = uuid.UUID("12345678-1234-5678-1234-567812345678")  # keep constant across runs

# =========================
# IRIs
# =========================
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
RDFS_COMMENT = "http://www.w3.org/2000/01/rdf-schema#comment"

IAO_DENOTED_BY = "http://purl.obolibrary.org/obo/IAO_0000235"
IAO_EMAIL_ADDRESS = "http://purl.obolibrary.org/obo/IAO_0000429"
IAO_GIVEN_NAME = "http://purl.obolibrary.org/obo/IAO_0020016"
IAO_FAMILY_NAME = "http://purl.obolibrary.org/obo/IAO_0020017"
IAO_WRITTEN_NAME = "http://purl.obolibrary.org/obo/IAO_0000590"

BFO_HAS_CONTRIBUTOR = "http://purl.obolibrary.org/obo/BFO_0000178"
BFO_PARTICIPATES_IN = "http://purl.obolibrary.org/obo/BFO_0000056"

# Process–agent–role pattern
RO_HAS_PARTICIPANT = "http://purl.obolibrary.org/obo/RO_0000057"
BFO_REALIZES = "http://purl.obolibrary.org/obo/BFO_0000055"
BFO_OCCUPIES_TEMPORAL_REGION = "http://purl.obolibrary.org/obo/BFO_0000199"
BFO_INHERES_IN = "http://purl.obolibrary.org/obo/BFO_0000197"

# NFDI
NFDI_DATASET = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009"
NFDI_TITLE = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019"
NFDI_PERSON = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000004"
NFDI_HAS_VALUE = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007"
NFDI_HAS_URL = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008"
NFDI_WEBSITE = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000223"

# License (your requirement)
NFDI_HAS_LICENSE = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000142"
SWO_LICENSE = "http://www.ebi.ac.uk/swo/SWO_0000002"

# Publishing classes
NFDI_DATASET_PUBLISHING_PROCESS = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000014"
NFDI_CONTACT_POINT = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000102"
NFDI_CONTACT_POINT_ROLE = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000114"
NFDI_STANDARD_PROP = "https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000207"

# Other
OBI_ORG = "http://purl.obolibrary.org/obo/OBI_0000245"
BFO_TEMPORAL_REGION = "http://purl.obolibrary.org/obo/BFO_0000008"


# =========================
# HELPERS
# =========================
def sanitize(s):
    if s is None:
        return ""
    s = str(s).replace("\t", " ").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def split_semicolon(s):
    s = sanitize(s)
    if not s:
        return []
    return [p.strip() for p in s.split(";") if p.strip()]


def clean_person_name(raw: str) -> str:
    raw = sanitize(raw)
    if not raw:
        return ""
    # remove leading markers like "(a) " or "(b,c) "
    raw = re.sub(r"^\(?[A-Za-z](?:\s*,\s*[A-Za-z])*\)?\)?\s*", "", raw)
    return sanitize(raw)


def name_to_given_family(full_name: str):
    full_name = clean_person_name(full_name)
    tokens = [t for t in full_name.split() if t]
    if not tokens:
        return "", ""
    if len(tokens) == 1:
        return tokens[0], ""
    return " ".join(tokens[:-1]), tokens[-1]


def mint(key: str) -> str:
    return BASE_IRI + str(uuid.uuid5(NS, key))


def parse_date_mmddyyyy(s: str) -> str:
    s = sanitize(s)
    if not s:
        return ""
    try:
        z = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(z)
        return dt.strftime("%m/%d/%Y")
    except Exception:
        return s


def write_tsv(path, header, template_row, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)
        w.writerow(template_row)
        for r in rows:
            w.writerow([r.get(col, "") for col in header])


# =========================
# MAIN
# =========================
def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    orgs = {}
    titles = {}
    licenses = {}  # license_iri -> label/value

    people = {}        # person_iri -> {label, denoted_by[]}
    emails = {}        # email_iri -> email
    given_names = {}
    family_names = {}
    written_names = {} # wn_iri -> full name
    websites = {}      # url_iri -> url

    publ_processes = {}  # proc_iri -> row dict
    publ_agents = {}     # agent_iri -> row dict
    publ_roles = {}      # role_iri -> row dict
    publ_dates = {}      # date_iri -> date literal

    datasets = []

    def ensure_email(email: str) -> str:
        email = sanitize(email)
        if not email:
            return ""
        eiri = mint(email.lower().strip())
        emails[eiri] = email
        return eiri

    def ensure_url(url: str) -> str:
        url = sanitize(url)
        if not url:
            return ""
        uiri = mint(url)
        websites[uiri] = url
        return uiri

    def ensure_written_name(full_name: str, salt: str) -> str:
        full_name = clean_person_name(full_name)
        if not full_name:
            return ""
        wniri = mint(salt + "|" + full_name)
        written_names[wniri] = full_name
        return wniri

    def ensure_person(name: str, email: str, role: str) -> str:
        name = clean_person_name(name)
        email = sanitize(email)

        key = (email.lower().strip() if email else f"{name}|{role}").strip()
        if not key:
            return ""
        person_iri = mint(key)

        if person_iri not in people:
            people[person_iri] = {"label": name or email or key, "denoted_by": []}

        if email:
            people[person_iri]["denoted_by"].append(ensure_email(email))

        if name:
            given, family = name_to_given_family(name)
            if given:
                giri = mint(person_iri + "|" + given)
                given_names[giri] = given
                people[person_iri]["denoted_by"].append(giri)
            if family:
                firi = mint(person_iri + "|" + family)
                family_names[firi] = family
                people[person_iri]["denoted_by"].append(firi)

            wniri = mint(person_iri + "|" + name)
            written_names[wniri] = name
            people[person_iri]["denoted_by"].append(wniri)

        return person_iri

    def ensure_license(lic_id: str, lic_title: str = "") -> str:
        lic_id = sanitize(lic_id)
        lic_title = sanitize(lic_title)
        if not lic_id and not lic_title:
            return ""
        label = lic_title or lic_id
        liri = mint((lic_id or label).lower())
        licenses[liri] = label
        return liri

    def build_publishing_pattern(ds_iri: str, ds_label: str, created: str,
                                publisher_name: str, publisher_email: str, publisher_uri: str) -> str:
        """
        Creates/updates:
          - one publishing process per dataset
          - contact point agent(s)
          - contact point role(s) per dataset+agent
          - publishing date node per dataset (from metadata_created)
        Returns process IRI.
        """
        proc_iri = mint(ds_iri + "/publishing-process")
        date_iri = mint(ds_iri + "/publishing-date")

        date_val = parse_date_mmddyyyy(created)
        if date_val:
            publ_dates[date_iri] = date_val

        if proc_iri not in publ_processes:
            publ_processes[proc_iri] = {
                "ID": proc_iri,
                "TYPE": NFDI_DATASET_PUBLISHING_PROCESS,
                "label": f"dataset publishing process for {ds_label or ds_iri}",
                "has_participant": "",
                "realizes_role": "",
                "occupies_temporal_region": date_iri if date_val else "",
                "standard": "",
            }

        if not (sanitize(publisher_name) or sanitize(publisher_email) or sanitize(publisher_uri)):
            return proc_iri

        # ContactPoint agent (reuse across datasets by stable key)
        agent_key = (sanitize(publisher_email).lower().strip()
                     if sanitize(publisher_email)
                     else sanitize(publisher_name).strip() or sanitize(publisher_uri).strip())
        agent_iri = mint(agent_key)
        wn_iri = ensure_written_name(publisher_name or publisher_email or agent_key, agent_iri)

        if agent_iri not in publ_agents:
            publ_agents[agent_iri] = {
                "ID": agent_iri,
                "TYPE": NFDI_CONTACT_POINT,
                "label": f"ContactPoint: {sanitize(publisher_name) or sanitize(publisher_email) or agent_key}",
                "denoted_by_written_name": wn_iri,
            }

        # Role is dataset-contextual (per process+agent)
        role_iri = mint(proc_iri + "/" + agent_iri)

        publ_roles[role_iri] = {
            "ID": role_iri,
            "TYPE": NFDI_CONTACT_POINT_ROLE,
            "label": f"ContactPointRole for {ds_label or ds_iri}",
            "role_email": ensure_email(publisher_email),
            "role_website": ensure_url(publisher_uri),
            "written_name": wn_iri,
            "inheres_in": agent_iri,
        }

        # Attach agent+role to process
        proc = publ_processes[proc_iri]
        participants = [p for p in proc["has_participant"].split("|") if p] if proc["has_participant"] else []
        roles = [p for p in proc["realizes_role"].split(",") if p] if proc["realizes_role"] else []

        if agent_iri not in participants:
            participants.append(agent_iri)
        if role_iri not in roles:
            roles.append(role_iri)

        proc["has_participant"] = "|".join(participants)
        proc["realizes_role"] = ",".join(roles)

        return proc_iri

    # -------------------------
    # Read CSV + build rows
    # -------------------------
    with open(IN_CSV, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            ds_id = sanitize(r.get("id", ""))
            if not ds_id:
                continue

            ds_iri = BASE_IRI + ds_id
            title = sanitize(r.get("title", ""))
            desc = sanitize(r.get("description", ""))
            created = sanitize(r.get("metadata_created", ""))

            # title node for denoted_by
            title_node = ""
            if title:
                title_node = mint(ds_iri + "/title")
                titles[title_node] = title

            # org
            org_id = sanitize(r.get("organization_id", ""))
            org_iri = ""
            if org_id:
                org_iri = BASE_IRI + org_id
                if org_iri not in orgs:
                    orgs[org_iri] = {
                        "label": sanitize(r.get("organization_title", "")) or org_id,
                        "desc": sanitize(r.get("organization_description", "")),
                    }

            # license node
            license_iri = ensure_license(
                r.get("license_id", ""),
                r.get("license_title", "")  # if you add it later
            )

            # resource URLs as literals
            resource_urls = [sanitize(u) for u in split_semicolon(r.get("resource_urls", ""))]
            url_joined = ";".join([u for u in resource_urls if u])

            # people from contact/contributor
            contributors = []
            for role, nfield, efield in [
                ("contact", "contact_name", "contact_email"),
                ("contributor", "contributor_name", "contributor_email"),
            ]:
                names = split_semicolon(r.get(nfield, ""))
                ems = split_semicolon(r.get(efield, ""))
                for i in range(max(len(names), len(ems))):
                    nm = names[i] if i < len(names) else ""
                    em = ems[i] if i < len(ems) else ""
                    piri = ensure_person(nm, em, role)
                    if piri:
                        contributors.append(piri)

            if org_iri:
                contributors.append(org_iri)
            contributors = sorted(set([c for c in contributors if c]))

            # always create base process+date
            build_publishing_pattern(ds_iri, title, created, "", "", "")

            # create publisher-based process-agent-role
            pub_names = split_semicolon(r.get("publisher_name", ""))
            pub_emails = split_semicolon(r.get("publisher_email", ""))
            pub_uris = split_semicolon(r.get("publisher_uri", ""))
            for i in range(max(len(pub_names), len(pub_emails), len(pub_uris))):
                nm = pub_names[i] if i < len(pub_names) else ""
                em = pub_emails[i] if i < len(pub_emails) else ""
                uri = pub_uris[i] if i < len(pub_uris) else ""
                build_publishing_pattern(ds_iri, title, created, nm, em, uri)

            proc_iri = mint(ds_iri + "/publishing-process")

            datasets.append({
                "ID": ds_iri,
                "TYPE": NFDI_DATASET,
                "label": title,
                "denoted_by_title": title_node,
                "description": desc,
                "license": license_iri,
                "publishing_process": proc_iri,
                "has_contributor": ";".join(contributors),
                "has_url": url_joined,
            })

    # de-dupe denoted_by lists
    for piri, pdata in people.items():
        pdata["denoted_by"] = sorted(set([x for x in pdata["denoted_by"] if x]))

    # =========================
    # WRITE TSVs
    # =========================

    # datasets.tsv (license + publishing process links)
    write_tsv(
        os.path.join(OUT_DIR, "datasets.tsv"),
        ["ID","TYPE","label","denoted_by_title","description","license","publishing_process","has_contributor","has_url"],
        [
            "ID","TYPE",
            f"A {RDFS_LABEL}",
            f"I {IAO_DENOTED_BY}",
            f"A {RDFS_COMMENT}",
            f"I {NFDI_HAS_LICENSE}",
            f"I {BFO_PARTICIPATES_IN}",
            f"I {BFO_HAS_CONTRIBUTOR} SPLIT=;",
            f"AT {NFDI_HAS_URL}^^xsd:anyURI SPLIT=;"
        ],
        datasets
    )

    # titles.tsv
    write_tsv(
        os.path.join(OUT_DIR, "titles.tsv"),
        ["ID","TYPE","label","value"],
        ["ID","TYPE", f"A {RDFS_LABEL}", f"A {NFDI_HAS_VALUE}"],
        [{"ID": iri, "TYPE": NFDI_TITLE, "label": val, "value": val} for iri, val in titles.items()]
    )

    # organizations.tsv
    write_tsv(
        os.path.join(OUT_DIR, "organizations.tsv"),
        ["ID","TYPE","label","description"],
        ["ID","TYPE", f"A {RDFS_LABEL}", f"A {RDFS_COMMENT}"],
        [{"ID": iri, "TYPE": OBI_ORG, "label": v["label"], "description": v["desc"]} for iri, v in orgs.items()]
    )

    # persons.tsv
    write_tsv(
        os.path.join(OUT_DIR, "persons.tsv"),
        ["ID","TYPE","label","denoted_by"],
        ["ID","TYPE", f"A {RDFS_LABEL}", f"I {IAO_DENOTED_BY} SPLIT=;"],
        [{"ID": iri, "TYPE": NFDI_PERSON, "label": v["label"], "denoted_by": ";".join(v["denoted_by"])} for iri, v in people.items()]
    )

    # emails.tsv
    write_tsv(
        os.path.join(OUT_DIR, "emails.tsv"),
        ["ID","TYPE","label","value"],
        ["ID","TYPE", f"A {RDFS_LABEL}", f"A {NFDI_HAS_VALUE}"],
        [{"ID": iri, "TYPE": IAO_EMAIL_ADDRESS, "label": val, "value": val} for iri, val in emails.items()]
    )

    # websites.tsv
    write_tsv(
        os.path.join(OUT_DIR, "websites.tsv"),
        ["ID","TYPE","label","url"],
        ["ID","TYPE", f"A {RDFS_LABEL}", f"AT {NFDI_HAS_URL}^^xsd:anyURI"],
        [{"ID": iri, "TYPE": NFDI_WEBSITE, "label": val, "url": val} for iri, val in websites.items()]
    )

    # given/family (optional, kept)
    write_tsv(
        os.path.join(OUT_DIR, "given_names.tsv"),
        ["ID","TYPE","label","value"],
        ["ID","TYPE", f"A {RDFS_LABEL}", f"A {NFDI_HAS_VALUE}"],
        [{"ID": iri, "TYPE": IAO_GIVEN_NAME, "label": val, "value": val} for iri, val in given_names.items()]
    )
    write_tsv(
        os.path.join(OUT_DIR, "family_names.tsv"),
        ["ID","TYPE","label","value"],
        ["ID","TYPE", f"A {RDFS_LABEL}", f"A {NFDI_HAS_VALUE}"],
        [{"ID": iri, "TYPE": IAO_FAMILY_NAME, "label": val, "value": val} for iri, val in family_names.items()]
    )

    # written_names.tsv
    write_tsv(
        os.path.join(OUT_DIR, "written_names.tsv"),
        ["ID","TYPE","label","value"],
        ["ID","TYPE", f"A {RDFS_LABEL}", f"A {NFDI_HAS_VALUE}"],
        [{"ID": iri, "TYPE": IAO_WRITTEN_NAME, "label": val, "value": val} for iri, val in written_names.items()]
    )

    # license.tsv (NEW)
    write_tsv(
        os.path.join(OUT_DIR, "license.tsv"),
        ["ID","TYPE","label","value"],
        ["ID","TYPE", f"A {RDFS_LABEL}", f"A {NFDI_HAS_VALUE}"],
        [{"ID": iri, "TYPE": SWO_LICENSE, "label": val, "value": val} for iri, val in licenses.items()]
    )

    # process.tsv
    write_tsv(
        os.path.join(OUT_DIR, "process.tsv"),
        ["ID","TYPE","label","has_participant","realizes_role","occupies_temporal_region","standard"],
        [
            "ID","TYPE",
            f"A {RDFS_LABEL}",
            f"I {RO_HAS_PARTICIPANT} SPLIT=|",
            f"I {BFO_REALIZES} SPLIT=,",
            f"I {BFO_OCCUPIES_TEMPORAL_REGION}",
            f"I {NFDI_STANDARD_PROP} SPLIT=,"
        ],
        list(publ_processes.values())
    )

    # agent.tsv
    write_tsv(
        os.path.join(OUT_DIR, "agent.tsv"),
        ["ID","TYPE","label","denoted_by_written_name"],
        ["ID","TYPE", f"A {RDFS_LABEL}", f"I {IAO_DENOTED_BY}"],
        list(publ_agents.values())
    )

    # role.tsv
    write_tsv(
        os.path.join(OUT_DIR, "role.tsv"),
        ["ID","TYPE","label","role_email","role_website","written_name","inheres_in"],
        [
            "ID","TYPE",
            f"A {RDFS_LABEL}",
            f"I {IAO_DENOTED_BY}",
            f"I {IAO_DENOTED_BY}",
            f"I {IAO_DENOTED_BY}",
            f"I {BFO_INHERES_IN} SPLIT=|"
        ],
        list(publ_roles.values())
    )

    # dates.tsv
    write_tsv(
        os.path.join(OUT_DIR, "dates.tsv"),
        ["ID","TYPE","label","value"],
        ["ID","TYPE", f"A {RDFS_LABEL}", f"A {NFDI_HAS_VALUE}"],
        [{"ID": iri, "TYPE": BFO_TEMPORAL_REGION, "label": "DatasetPublishingDate", "value": val} for iri, val in publ_dates.items()]
    )


    print("Done. TSVs in:", OUT_DIR)


if __name__ == "__main__":
    main()