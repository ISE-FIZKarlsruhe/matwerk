# Merge

**DAG ID:** `merge`
**Schedule:** Manual trigger only
**File:** `dags/merge.py`

## What it does

Merges all OWL modules from the `process_spreadsheets` DAG into a single asserted Turtle file using ROBOT merge. Validates the merged result for inconsistencies using the HermIT reasoner.

## Input

| Source | Description |
|--------|-------------|
| `matwerk_sharedfs` (Variable) | Base shared filesystem path |
| `matwerk_last_successful_spreadsheet_run` (Variable) | Directory containing OWL modules |
| `robotcmd` (Variable) | Path to ROBOT command |

**Files read:**

- All `*.owl` files from the spreadsheet run directory

## Output

| Output | Location |
|--------|----------|
| `spreadsheets_asserted.ttl` | Merged Turtle file |
| `hermit_inconsistency.md` | Inconsistency explanation report |

!!! tip "Success variable"
    On success, sets `matwerk_last_successful_merge_run` pointing to the run directory.

## Task chain

```
init_data_dir -> wait_for_inputs -> robot_merge_and_convert -> robot_hermit_explain -> robot_hermit_valid -> mark_merge_successful
```

## Downstream

None. Trigger `reason` or `reason_openllet_new` manually after this DAG succeeds.
