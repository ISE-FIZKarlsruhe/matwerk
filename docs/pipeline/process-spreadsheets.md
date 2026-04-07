# Process Spreadsheets

**DAG ID:** `process_spreadsheets`
**Schedule:** Manual trigger only
**File:** `dags/spreadsheets.py`

## What it does

Builds ontology modules from 27 Google Sheets TSV templates using the ROBOT tool. Each module is validated with ROBOT explain (HermIT reasoner) for inconsistency detection.

!!! info "Build order matters"
    Modules are built in a specific dependency order (e.g., `organization` must be built before `dataset`, `agent` before `publication`).

## Input

| Source | Description |
|--------|-------------|
| `matwerk_sharedfs` (Variable) | Base directory for shared filesystem |
| `matwerk_ontology` (Variable) | URL to the base ontology OWL file |
| 27 Google Sheets | Public TSV exports containing ontology templates |

## Output

| Output | Location |
|--------|----------|
| Individual OWL modules | `{sharedfs}/runs/process_spreadsheets/{run_id}/*.owl` |
| Validation reports | `{sharedfs}/runs/process_spreadsheets/{run_id}/*.md` |

!!! tip "Success variable"
    On success, sets `matwerk_last_successful_spreadsheet_run` pointing to the run directory.

## Task chain

```
init_data_dir
  -> retrieve_ontology + 27x retrieve_csv_*
    -> waitForCsv
      -> robot_req_1 -> robot_req_1_valid
      -> robot_req_2 -> robot_req_2_valid
      -> robot_agent -> robot_agent_valid
      -> ... (27 modules in dependency order)
      -> tear_down (marks success)
```

## Downstream

None. Trigger `merge` manually after this DAG succeeds.
