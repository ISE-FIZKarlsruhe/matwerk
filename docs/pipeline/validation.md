# Validation Checks

**DAG ID:** `validation_checks`
**Schedule:** Manual trigger only
**File:** `dags/validation_checks.py`

## What it does

Comprehensive validation suite that:

1. Merges asserted triples + inferred triples + base ontology into a single graph
2. Runs HermIT inconsistency checking
3. Runs ROBOT SPARQL verify queries
4. Validates against SHACL shapes (downloaded from GitHub)

## Input

| Source | Description |
|--------|-------------|
| `matwerk_sharedfs` (Variable) | Shared filesystem |
| `matwerk_last_successful_merge_run` (Variable) | Asserted TTL source |
| `matwerk_last_successful_reason_run` (Variable) | Inferences TTL source |
| `matwerk_ontology` (Variable) | URL to base ontology |
| `robotcmd` (Variable) | ROBOT command |
| SHACL shapes | Downloaded from `ISE-FIZKarlsruhe/matwerk` repo, `shapes/` directory |

**Conf parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `artifact` | `spreadsheets` | Artifact name |
| `target_run_dir` | (auto-created) | Custom output directory |
| `asserted_ttl` | `spreadsheets_asserted.ttl` | Asserted TTL filename |
| `asserted_source_dir` | (from Variable) | Custom asserted source directory |
| `inferences_ttl` | `spreadsheets_inferences.ttl` | Inferences TTL filename |
| `reason_source_dir` | (from Variable) | Custom reason source directory |

## Output

| Output | Location |
|--------|----------|
| `{artifact}_merged_for_validation.ttl` | Merged graph for validation |
| `validation/hermit_inconsistency.md` | Inconsistency report |
| `validation/robot_verify/summary.txt` | SPARQL verify results |
| `validation/shacl/summary.txt` | SHACL validation summary |

!!! tip "Success variables"
    - `matwerk_last_successful_validated_run` (if artifact is "spreadsheets")
    - `matwerk_last_successful_validated_run__{artifact}` (always)

## Task chain

```
init_data_dir -> [pull_merge_reason_output, fetch_shapes]
  pull_merge_reason_output -> robot_merge_for_validation
    robot_merge_for_validation -> [robot_hermit_explain, robot_verify_sparql, shacl_validate]
      robot_hermit_explain -> robot_hermit_valid
  fetch_shapes -> [robot_verify_sparql, shacl_validate]
    [all validation tasks] -> mark_validated_successful
```

## Downstream

None. Trigger `publish_to_virtuoso` after this DAG succeeds.
