# Dump and Archive

**DAG ID:** `dump_and_archive`
**Schedule:** Manual trigger only
**File:** `dags/dump_and_archive.py`

## What it does

Creates versioned RDF dumps of all published named graphs, generates per-graph metadata using MWO/nfdicore vocabulary, uploads everything to Zenodo (minting a DOI), and pushes the updated manifest to GitHub for the documentation site.

## When to use

Trigger this DAG when you want to create a new **release** of the Knowledge Graph. This is typically after:

1. Running the full pipeline (spreadsheets -> merge -> reason -> validate -> publish_to_virtuoso)
2. Verifying the data looks correct in the SPARQL endpoint / dashboard

!!! warning "Not every publish needs a release"
    Not every `publish_to_virtuoso` run needs a Zenodo release. Only create a release when you consider the KG state to be a stable version worth archiving.

---

## Step-by-step: Releasing a new dump

### Prerequisites

Before your first release, set these Airflow Variables (one-time setup):

| Variable | How to get it |
|----------|---------------|
| `matwerk_zenodo_token` | Create at [zenodo.org/account/settings/applications/](https://zenodo.org/account/settings/applications/) with scopes `deposit:actions` and `deposit:write` |
| `matwerk_github_token` | GitHub PAT with Contents read/write for `ISE-FIZKarlsruhe/matwerk` |

!!! info "Auto-managed variables"
    The following variables are set automatically -- do not create them manually:

    | Variable | Description |
    |----------|-------------|
    | `matwerk_zenodo_concept_id` | Auto-set after first publish, links future versions |
    | `matwerk_zenodo_base_url` | Defaults to `https://zenodo.org/api` |
    | `matwerk_github_repo` | Defaults to `ISE-FIZKarlsruhe/matwerk` |

### Step 1: Decide the version

Use semantic versioning. Examples:

- New data added -> `2.1.1` -> `2.2.0`
- Ontology update -> `2.2.0` -> `3.0.0`
- Small fix -> `2.1.1` -> `2.1.2`

### Step 2: (Optional) Dry-run

Trigger the DAG from the Airflow UI (Trigger DAG w/ config):

```json
{"version": "2.2.0", "skip_zenodo": true}
```

!!! tip "Dry-run"
    This generates dump files and manifest locally without uploading to Zenodo. Check the logs to verify everything looks correct.

### Step 3: Publish to Zenodo

Trigger the DAG with the real config:

```json
{"version": "2.2.0"}
```

Or if the MWO ontology version has changed:

```json
{"version": "2.2.0", "mwo_version": "3.1.0"}
```

### Step 4: Verify

1. Check the DAG logs for success
2. Visit the Zenodo record (URL in the logs) to verify the deposit
3. Check that `docs/dumps.json` was pushed to GitHub

### Step 5: Rebuild the documentation site

Trigger the "Docker Image CI" GitHub Actions workflow to rebuild the Docker image with the updated `dumps.json`. The RDF Dumps page will show the new release.

---

## Configuration parameters

Pass these in the DAG config JSON when triggering:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `version` | `0.0.0` | Semantic version for this release |
| `publish_date` | Today (YYYY-MM-DD) | Override the publish date |
| `mwo_version` | `3.0.1` | MWO ontology version used |
| `skip_zenodo` | `false` | Set `true` for dry-run (no Zenodo upload) |

## Input

| Source | Description |
|--------|-------------|
| `matwerk_sharedfs` (Variable) | Shared filesystem |
| Same 5 source Variables as `publish_to_virtuoso` | Points to latest successful pipeline runs |

**Files read (same 5 as publish_to_virtuoso):**

| Graph | Variable | TTL File |
|-------|----------|----------|
| spreadsheets_assertions | `matwerk_last_successful_merge_run` | `spreadsheets_asserted.ttl` |
| spreadsheets_inferences | `matwerk_last_successful_reason_run` | `spreadsheets_inferences.ttl` |
| spreadsheets_validated | `matwerk_last_successful_validated_run` | `spreadsheets_merged_for_validation.ttl` |
| zenodo_validated | `matwerk_last_successful_harvester_zenodo_run` | `zenodo_merged_for_validation.ttl` |
| endpoints_validated | `matwerk_last_successful_harvester_endpoints_run` | `endpoints_merged_for_validation.ttl` |

## Output

### Local files

| Output | Location |
|--------|----------|
| `dumps/{stage}_v{version}.ttl` | Versioned dump file per graph |
| `dumps/{stage}_v{version}_metadata.ttl` | Metadata TTL per graph (MWO/nfdicore) |
| `dumps.json` | Local manifest copy |

### Zenodo

- All dump + metadata files uploaded
- Published as a versioned record with a new DOI
- Linked to previous versions via concept DOI

### GitHub

- `docs/dumps.json` updated with new release entry (pushed via GitHub API)

### Variables set

- `matwerk_zenodo_concept_id` -- updated after first publish (used for future versioning)

## Task chain

```
init_data_dir -> generate_dumps -> upload_to_zenodo -> update_manifest
```

### What each task does

1. **init_data_dir** -- creates the run directory on shared filesystem
2. **generate_dumps** -- copies each graph's TTL as a versioned dump, computes RDF stats (triples, subjects, predicates, objects, type assertions, distinct types), generates metadata TTL
3. **upload_to_zenodo** -- creates/updates Zenodo deposit, uploads all files, sets metadata (title, authors, description with stats table, related identifiers, community), publishes (mints DOI)
4. **update_manifest** -- builds `dumps.json` manifest, fetches existing manifest from GitHub (to append), pushes updated manifest to GitHub

## Zenodo record contents

Each Zenodo release contains:

- **Title:** Materials Science and Engineering (MSE) Knowledge Graph - RDF Dumps (v{version})
- **Authors:** Sack, Waitelonis, Norouzi, Beygi Nasrabadi (from MWO ontology)
- **Contributors:** NFDI-MatWerk Community
- **Community:** nfdi-matwerk
- **License:** MIT
- **Related:** KG website, GitHub repo, MWO ontology, NFDIcore ontology
- **Description:** Stats table per graph, ontology info, SPARQL endpoint link

## Metadata TTL per graph

Each dump file has a companion `_metadata.ttl` describing it using:

- `nfdicore:NFDI_0000009` (Dataset)
- `nfdicore:NFDI_0000027` (File Data Item)
- `nfdicore:NFDI_0001053` (Ontology Version IRI)
- `dct:conformsTo` pointing to MWO version IRI
- BFO temporal region for publication timestamps
- Statistics embedded as description text via `nfdicore:NFDI_0001007`
