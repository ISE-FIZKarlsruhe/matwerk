from __future__ import annotations
import os, sys

print('getcwd:      ', os.getcwd())
print('__file__:    ', __file__)
local_path = os.path.dirname(__file__)
print('adding local path', local_path)
sys.path.append(local_path)

import json
import base64
from datetime import datetime, timezone

import requests
from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from common.graph_metadata import compute_rdf_stats


DAG_ID = "dump_and_archive"
GRAPH_ROOT = "https://nfdi.fiz-karlsruhe.de/matwerk"
MWO_ONTOLOGY_IRI = "http://purls.helmholtz-metadaten.de/mwo/mwo.owl"
DEFAULT_MWO_VERSION = "3.0.1"

# Same sources as publish_to_virtuoso
DUMP_SOURCES = [
    ("spreadsheets_assertions", "matwerk_last_successful_merge_run", "spreadsheets_asserted.ttl"),
    ("spreadsheets_inferences", "matwerk_last_successful_reason_run", "spreadsheets_inferences.ttl"),
    ("spreadsheets_validated", "matwerk_last_successful_validated_run", "spreadsheets_merged_for_validation.ttl"),
    ("zenodo_validated", "matwerk_last_successful_harvester_zenodo_run", "zenodo_merged_for_validation.ttl"),
    ("endpoints_validated", "matwerk_last_successful_harvester_endpoints_run", "endpoints_merged_for_validation.ttl"),
]


def _build_dump_metadata_ttl(
    graph_name: str,
    version: str,
    publish_date: str,
    stats,
    mwo_version: str,
    mwo_version_iri: str,
    zenodo_doi: str | None,
) -> str:
    """
    Build metadata TTL for a single dump file using MWO/nfdicore vocabulary.
    Stats are embedded as description text via nfdicore:NFDI_0001007 (option B).
    """
    dataset_iri = f"{GRAPH_ROOT}/{graph_name}"
    desc_iri = f"{dataset_iri}/dump-description"
    version_iri = f"{dataset_iri}/dump-version/{version}"
    id_iri = f"{dataset_iri}/dump-identifier/{version}"
    process_iri = f"{dataset_iri}/dump-process/{version}"
    temporal_iri = f"{dataset_iri}/dump-temporal/{version}"
    begin_iri = f"{dataset_iri}/dump-begin/{version}"
    end_iri = f"{dataset_iri}/dump-end/{version}"
    file_iri = f"{dataset_iri}/dump-file/{version}"
    license_iri = "https://purls.helmholtz-metadaten.de/msekg/17453312603732"
    creator_iri = "https://purls.helmholtz-metadaten.de/msekg/17458299010501"

    desc_text = (
        f"RDF dump of named graph: {dataset_iri}\\n"
        f"Version: {version}\\n"
        f"Date: {publish_date}\\n"
        f"MWO ontology version: {mwo_version} ({mwo_version_iri})\\n"
        f"Statistics:\\n"
        f"  triples: {stats.triples}\\n"
        f"  distinct subjects: {stats.subjects}\\n"
        f"  distinct predicates: {stats.predicates}\\n"
        f"  distinct objects: {stats.objects}\\n"
        f"  rdf:type assertions: {stats.type_assertions}\\n"
        f"  distinct types (classes used): {stats.distinct_type_objects}"
    )

    lines = [
        '@prefix obo:      <http://purl.obolibrary.org/obo/> .',
        '@prefix nfdicore:  <https://nfdi.fiz-karlsruhe.de/ontology/> .',
        '@prefix time:     <http://www.w3.org/2006/time#> .',
        '@prefix xsd:      <http://www.w3.org/2001/XMLSchema#> .',
        '@prefix dcat:     <http://www.w3.org/ns/dcat#> .',
        '@prefix dct:      <http://purl.org/dc/terms/> .',
        '',
        f'<{dataset_iri}>',
        f'  a nfdicore:NFDI_0000009 ;',
        f'  nfdicore:NFDI_0000142 <{license_iri}> ;',
        f'  nfdicore:NFDI_0001027 <{creator_iri}> ;',
        f'  nfdicore:NFDI_0001006 <{id_iri}> ;',
        f'  obo:IAO_0000235 <{desc_iri}> ;',
        f'  obo:RO_0002353 <{process_iri}> ;',
        f'  dct:conformsTo <{mwo_version_iri}> .',
        '',
        f'<{desc_iri}>',
        f'  a nfdicore:NFDI_0000018 ;',
        f'  nfdicore:NFDI_0001007 "{desc_text}" .',
        '',
        f'<{id_iri}>',
        f'  a obo:IAO_0020000 ;',
        f'  nfdicore:NFDI_0001007 "v{version}" .',
        '',
        f'<{version_iri}>',
        f'  a nfdicore:NFDI_0001053 ;',
        f'  nfdicore:NFDI_0001007 "{version}" .',
        '',
        f'<{file_iri}>',
        f'  a nfdicore:NFDI_0000027 ;',
        f'  nfdicore:NFDI_0001007 "{graph_name}_v{version}.ttl" ;',
    ]

    if zenodo_doi:
        lines.append(f'  dcat:downloadURL <https://doi.org/{zenodo_doi}> ;')
        lines.append(f'  nfdicore:NFDI_0001008 "https://doi.org/{zenodo_doi}"^^xsd:anyURI .')
    else:
        lines.append(f'  nfdicore:NFDI_0001008 "{dataset_iri}"^^xsd:anyURI .')

    lines += [
        '',
        f'<{process_iri}>',
        f'  a obo:BFO_0000015 ;',
        f'  obo:BFO_0000199 <{temporal_iri}> .',
        '',
        f'<{temporal_iri}>',
        f'  a obo:BFO_0000038 ;',
        f'  obo:BFO_0000222 <{begin_iri}> ;',
        f'  obo:BFO_0000224 <{end_iri}> .',
        '',
        f'<{begin_iri}>',
        f'  a obo:BFO_0000148 ;',
        f'  time:inXSDDateTimeStamp "{publish_date}T00:00:00Z"^^xsd:dateTimeStamp .',
        '',
        f'<{end_iri}>',
        f'  a obo:BFO_0000148 ;',
        f'  time:inXSDDateTimeStamp "{publish_date}T00:00:00Z"^^xsd:dateTimeStamp .',
        '',
    ]
    return "\n".join(lines)


def _push_manifest_to_github(manifest: dict, repo: str, path: str, branch: str = "main") -> None:
    """Push docs/dumps.json to GitHub via Contents API."""
    token = Variable.get("matwerk_github_token", default="")
    if not token:
        print("[WARN] matwerk_github_token not set, skipping GitHub push")
        return

    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }

    # Check if file exists (to get sha for update)
    sha = None
    r = requests.get(url, headers=headers, params={"ref": branch}, timeout=30)
    if r.status_code == 200:
        sha = r.json().get("sha")

    content_b64 = base64.b64encode(
        json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    ).decode("ascii")

    body = {
        "message": f"chore: update RDF dumps manifest (v{manifest.get('latest_version', '?')})",
        "content": content_b64,
        "branch": branch,
    }
    if sha:
        body["sha"] = sha

    r = requests.put(url, json=body, headers=headers, timeout=60)
    if r.status_code not in (200, 201):
        print(f"[WARN] GitHub push failed ({r.status_code}): {r.text[:500]}")
    else:
        print(f"[INFO] Pushed {path} to GitHub ({r.status_code})")


@dag(
    schedule=None,
    catchup=False,
    dag_id=DAG_ID,
    tags=["matwerk"],
)
def dump_and_archive():

    @task
    def init_data_dir(ti=None):
        ctx = get_current_context()
        sharedfs = Variable.get("matwerk_sharedfs")
        if not sharedfs or not os.path.isdir(sharedfs):
            raise AirflowFailException(f"sharedfs missing/not a dir: {sharedfs}")

        rid = ctx["dag_run"].run_id
        run_dir = os.path.join(sharedfs, "runs", ctx["dag"].dag_id, rid)
        os.makedirs(run_dir, exist_ok=True)
        ti.xcom_push(key="datadir", value=run_dir)

    @task
    def generate_dumps(ti=None):
        """
        Copy each named graph TTL into the run dir as a versioned dump file,
        compute stats, and generate per-graph metadata TTL.
        """
        import shutil

        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        ctx = get_current_context()
        conf = ctx["dag_run"].conf or {}

        version = conf.get("version", "0.0.0")
        publish_date = conf.get("publish_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        mwo_version = conf.get("mwo_version", DEFAULT_MWO_VERSION)
        mwo_version_iri = f"{MWO_ONTOLOGY_IRI}/{mwo_version}"

        dumps_dir = os.path.join(run_dir, "dumps")
        os.makedirs(dumps_dir, exist_ok=True)

        dump_entries = []

        for stage, var_name, ttl_name in DUMP_SOURCES:
            try:
                source_dir = Variable.get(var_name)
            except Exception:
                print(f"[WARN] Variable {var_name} not found, skipping {stage}")
                continue

            ttl_path = os.path.join(source_dir, ttl_name)
            if not os.path.exists(ttl_path) or os.path.getsize(ttl_path) == 0:
                print(f"[WARN] TTL missing/empty for {stage}: {ttl_path}")
                continue

            # Copy as versioned dump
            dump_filename = f"{stage}_v{version}.ttl"
            dump_path = os.path.join(dumps_dir, dump_filename)
            shutil.copy2(ttl_path, dump_path)
            print(f"[INFO] Copied {ttl_path} -> {dump_path}")

            # Compute stats
            stats = compute_rdf_stats(ttl_path)
            print(f"[INFO] {stage}: {stats.triples} triples, {stats.subjects} subjects, "
                  f"{stats.type_assertions} type assertions, {stats.distinct_type_objects} types")

            # Generate metadata TTL
            meta_ttl = _build_dump_metadata_ttl(
                graph_name=stage,
                version=version,
                publish_date=publish_date,
                stats=stats,
                mwo_version=mwo_version,
                mwo_version_iri=mwo_version_iri,
                zenodo_doi=None,  # filled after Zenodo upload
            )
            meta_path = os.path.join(dumps_dir, f"{stage}_v{version}_metadata.ttl")
            with open(meta_path, "w", encoding="utf-8") as f:
                f.write(meta_ttl)

            dump_entries.append({
                "graph_name": stage,
                "graph_uri": f"{GRAPH_ROOT}/{stage}",
                "dump_file": dump_filename,
                "dump_path": dump_path,
                "metadata_file": f"{stage}_v{version}_metadata.ttl",
                "version": version,
                "publish_date": publish_date,
                "mwo_version": mwo_version,
                "stats": {
                    "triples": stats.triples,
                    "subjects": stats.subjects,
                    "predicates": stats.predicates,
                    "objects": stats.objects,
                    "type_assertions": stats.type_assertions,
                    "distinct_types": stats.distinct_type_objects,
                },
            })

        if not dump_entries:
            raise AirflowFailException("No dump files generated — all sources missing or empty")

        ti.xcom_push(key="dump_entries", value=json.dumps(dump_entries))
        ti.xcom_push(key="version", value=version)
        ti.xcom_push(key="publish_date", value=publish_date)
        ti.xcom_push(key="dumps_dir", value=dumps_dir)
        ti.xcom_push(key="mwo_version", value=mwo_version)
        ti.xcom_push(key="mwo_version_iri", value=mwo_version_iri)

    @task
    def upload_to_zenodo(ti=None):
        """
        Upload all dump files to Zenodo and publish.
        Reads conf 'skip_zenodo' to allow dry-run without uploading.
        """
        from common.zenodo_deposit import (
            get_or_create_deposit,
            upload_file,
            set_metadata,
            publish,
            build_zenodo_metadata,
        )

        ctx = get_current_context()
        conf = ctx["dag_run"].conf or {}

        if conf.get("skip_zenodo", False):
            print("[INFO] skip_zenodo=True, skipping Zenodo upload")
            ti.xcom_push(key="zenodo_result", value=json.dumps({"skipped": True}))
            return

        dump_entries = json.loads(ti.xcom_pull(task_ids="generate_dumps", key="dump_entries"))
        version = ti.xcom_pull(task_ids="generate_dumps", key="version")
        publish_date = ti.xcom_pull(task_ids="generate_dumps", key="publish_date")
        dumps_dir = ti.xcom_pull(task_ids="generate_dumps", key="dumps_dir")
        mwo_version = ti.xcom_pull(task_ids="generate_dumps", key="mwo_version")

        # Get concept_id from Variable (None for first-ever deposit)
        concept_id = Variable.get("matwerk_zenodo_concept_id", default="")
        deposit = get_or_create_deposit(concept_id if concept_id else None)

        print(f"[INFO] Zenodo deposit id={deposit['id']}")

        # Upload each dump + metadata file
        graph_names = []
        for entry in dump_entries:
            upload_file(deposit, entry["dump_path"], entry["dump_file"])
            meta_path = os.path.join(dumps_dir, entry["metadata_file"])
            upload_file(deposit, meta_path, entry["metadata_file"])
            graph_names.append(entry["graph_name"])

        # Build description HTML for Zenodo
        stats_rows = ""
        for entry in dump_entries:
            s = entry["stats"]
            stats_rows += (
                f"<tr><td>{entry['graph_name']}</td>"
                f"<td>{s['triples']:,}</td>"
                f"<td>{s['subjects']:,}</td>"
                f"<td>{s['distinct_types']}</td></tr>\n"
            )

        desc_html = (
            f"<p>RDF dumps of the Materials Science and Engineering (MSE) Knowledge Graph (v{version}).</p>"
            f"<p>Semantic integration of distributed materials science resources — enabling structured "
            f"discovery, cross-resource linkage, and machine-actionable reuse across datasets, "
            f"publications, software, and domain actors.</p>"
            f"<h4>Ontological Foundation</h4>"
            f'<ul><li><a href="https://ise-fizkarlsruhe.github.io/mwo/">MWO</a> '
            f"(MatWerk Ontology) v{mwo_version} — domain-specific extensions for materials science and engineering</li>"
            f'<li><a href="https://ise-fizkarlsruhe.github.io/nfdicore/">NFDIcore</a> '
            f"— shared vocabulary for all NFDI consortia resources</li></ul>"
            f"<h4>Named Graph Statistics</h4>"
            f"<table><tr><th>Graph</th><th>Triples</th><th>Subjects</th><th>Types</th></tr>"
            f"{stats_rows}</table>"
            f"<p>Published: {publish_date}</p>"
            f'<p>SPARQL endpoint: <a href="https://nfdi.fiz-karlsruhe.de/matwerk/shmarql/">'
            f"https://nfdi.fiz-karlsruhe.de/matwerk/shmarql/</a></p>"
            f'<p>Source code: <a href="https://github.com/ISE-FIZKarlsruhe/matwerk">'
            f"https://github.com/ISE-FIZKarlsruhe/matwerk</a></p>"
            f"<hr><p><em>This work is part of the consortium NFDI-MatWerk, funded by the "
            f"Deutsche Forschungsgemeinschaft (DFG, German Research Foundation) under the "
            f"National Research Data Infrastructure – NFDI 38/1 – project number 460247524.</em></p>"
        )

        metadata = build_zenodo_metadata(version, publish_date, desc_html)
        set_metadata(deposit, metadata)

        published = publish(deposit)
        doi = published.get("doi", "")
        record_id = published.get("id", "")
        concept_doi = published.get("conceptdoi", "")
        concept_rec = published.get("conceptrecid", "")

        print(f"[INFO] Published to Zenodo: DOI={doi}, record={record_id}")

        # Persist concept_id for future versions
        if concept_rec:
            Variable.set("matwerk_zenodo_concept_id", str(concept_rec))

        zenodo_result = {
            "skipped": False,
            "doi": doi,
            "record_id": record_id,
            "concept_doi": concept_doi,
            "concept_record_id": concept_rec,
            "deposit_url": published.get("links", {}).get("html", ""),
        }
        ti.xcom_push(key="zenodo_result", value=json.dumps(zenodo_result))

    @task
    def update_manifest(ti=None):
        """
        Build docs/dumps.json manifest and push to GitHub.
        """
        dump_entries = json.loads(ti.xcom_pull(task_ids="generate_dumps", key="dump_entries"))
        version = ti.xcom_pull(task_ids="generate_dumps", key="version")
        publish_date = ti.xcom_pull(task_ids="generate_dumps", key="publish_date")
        mwo_version = ti.xcom_pull(task_ids="generate_dumps", key="mwo_version")
        mwo_version_iri = ti.xcom_pull(task_ids="generate_dumps", key="mwo_version_iri")
        zenodo_result = json.loads(ti.xcom_pull(task_ids="upload_to_zenodo", key="zenodo_result"))
        run_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")

        doi = zenodo_result.get("doi", "")
        deposit_url = zenodo_result.get("deposit_url", "")

        # Build per-graph entries for the manifest
        graphs = []
        for entry in dump_entries:
            graphs.append({
                "graph_name": entry["graph_name"],
                "graph_uri": entry["graph_uri"],
                "dump_file": entry["dump_file"],
                "metadata_file": entry["metadata_file"],
                "stats": entry["stats"],
            })

        release = {
            "version": version,
            "publish_date": publish_date,
            "mwo_version": mwo_version,
            "mwo_version_iri": mwo_version_iri,
            "zenodo_doi": doi,
            "zenodo_url": deposit_url,
            "graphs": graphs,
        }

        # Load existing manifest or start fresh
        repo = Variable.get("matwerk_github_repo", default="ISE-FIZKarlsruhe/matwerk")
        manifest_path = "docs/dumps.json"

        token = Variable.get("matwerk_github_token", default="")
        existing_manifest = {"latest_version": version, "releases": []}

        if token:
            url = f"https://api.github.com/repos/{repo}/contents/{manifest_path}"
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                content = base64.b64decode(r.json()["content"]).decode("utf-8")
                existing_manifest = json.loads(content)

        # Prepend new release (latest first)
        existing_manifest["latest_version"] = version
        existing_manifest["releases"].insert(0, release)

        # Save locally
        local_manifest = os.path.join(run_dir, "dumps.json")
        with open(local_manifest, "w", encoding="utf-8") as f:
            json.dump(existing_manifest, f, indent=2, ensure_ascii=False)
        print(f"[INFO] Wrote local manifest: {local_manifest}")

        # Push to GitHub
        _push_manifest_to_github(existing_manifest, repo, manifest_path)

    init = init_data_dir()
    dumps = generate_dumps()
    zenodo = upload_to_zenodo()
    manifest = update_manifest()

    init >> dumps >> zenodo >> manifest


dump_and_archive()
