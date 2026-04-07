# Publish to Virtuoso

**DAG ID:** `publish_to_virtuoso`
**Schedule:** Manual trigger only
**File:** `dags/publish_to_virtuoso.py`

## What it does

Publishes validated RDF graphs to Virtuoso. For each graph:

1. Drops the existing named graph (clean slate)
2. Uploads the TTL data (chunked for large files)
3. Computes RDF statistics
4. Generates metadata TTL (using nfdicore/BFO vocabulary) and appends it to the same graph
5. Writes a publish report JSON

!!! warning "Destructive operation"
    This DAG drops existing named graphs before uploading. Make sure the source data is correct before triggering.

## Input

| Source | Description |
|--------|-------------|
| `matwerk_sharedfs` (Variable) | Shared filesystem |
| `matwerk-virtuoso_crud` (Variable) | Virtuoso CRUD endpoint URL |
| `matwerk-virtuoso_sparql` (Variable) | Virtuoso SPARQL endpoint URL |
| `matwerk-virtuoso_user` (Variable) | Virtuoso username |
| `matwerk-virtuoso_pass` (Variable) | Virtuoso password |
| `virtuoso_chunk_bytes` (Variable, optional) | Chunk size for uploads (default: 5 MiB) |

**Published sources (5 graphs):**

| Named Graph | Variable | TTL File |
|-------------|----------|----------|
| `matwerk/spreadsheets_assertions` | `matwerk_last_successful_merge_run` | `spreadsheets_asserted.ttl` |
| `matwerk/spreadsheets_inferences` | `matwerk_last_successful_reason_run` | `spreadsheets_inferences.ttl` |
| `matwerk/spreadsheets_validated` | `matwerk_last_successful_validated_run` | `spreadsheets_merged_for_validation.ttl` |
| `matwerk/zenodo_validated` | `matwerk_last_successful_harvester_zenodo_run` | `zenodo_merged_for_validation.ttl` |
| `matwerk/endpoints_validated` | `matwerk_last_successful_harvester_endpoints_run` | `endpoints_merged_for_validation.ttl` |

## Output

| Output | Location |
|--------|----------|
| `publish_report.json` | Detailed status report for each graph |
| `{stage}__metadata.ttl` | Per-graph metadata TTL |
| Named graphs in Virtuoso | `https://nfdi.fiz-karlsruhe.de/matwerk/{stage}` |

## Task chain

```
init_publish_dir -> publish_all
```

## Downstream

None. After this succeeds, optionally trigger `dump_and_archive` to create a Zenodo release.
