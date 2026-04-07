# CQ Tester

**DAG ID:** `matwerk_cq_tester`
**Schedule:** Manual trigger only
**File:** `dags/cq-test.py`

## What it does

Tests competency questions by extracting SPARQL queries from the `general_queries.md` documentation file and executing each query against the read-only Virtuoso endpoint. Validates that every query returns non-empty results.

!!! warning "Requires published data"
    This DAG queries the live Virtuoso endpoint. Make sure `publish_to_virtuoso` has been run successfully before triggering this DAG.

## Input

| Source | Description |
|--------|-------------|
| `matwerk_sharedfs` (Variable) | Shared filesystem for run directory |
| `matwerk-virtuoso_sparql_ro` (Variable) | Read-only Virtuoso SPARQL endpoint |
| `general_queries.md` | Downloaded from GitHub (`ISE-FIZKarlsruhe/matwerk`, main branch) |

## Output

| Output | Location |
|--------|----------|
| `markdown.md` | Downloaded queries file |
| Console logs | Pass/fail per query |

!!! tip "Interpreting results"
    Check the task logs for per-query pass/fail status. Any query returning zero results is flagged as a failure.

## Task chain

```
init_data_dir -> test_queries
```

## Downstream

None. This is a quality check DAG.
