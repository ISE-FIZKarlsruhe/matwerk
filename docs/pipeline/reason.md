# Reason

**DAG IDs:** `reason` (Sunlet), `reason_openllet_new` (OpenLlet)
**Schedule:** Manual trigger only
**Files:** `dags/reason-spreadsheets.py`, `dags/reason_openlletnew.py`

## What it does

Performs OWL reasoning on filtered TTL to produce inferred triples. Filters out problematic axioms before reasoning, runs the reasoner, then converts output to Turtle format.

!!! info "Two reasoner variants"
    Two variants exist using different reasoners. `reason_openllet_new` is the current default used by harvesters.

## Input

| Source | Description |
|--------|-------------|
| `matwerk_sharedfs` (Variable) | Shared filesystem path |
| `matwerk_last_successful_merge_run` (Variable) | Source directory (if `source_run_dir` not in conf) |
| `sunletcmd` / `openlletnewcmd` (Variable) | Reasoner command |

**Conf parameters (from triggering DAG or UI):**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `artifact` | `spreadsheets` | Name of the artifact being reasoned |
| `in_ttl` | `spreadsheets_asserted.ttl` | Input TTL filename |
| `source_run_dir` | (from Variable) | Custom source directory |
| `target_run_dir` | (auto-created) | Custom target directory |

## Output

| Output | Location |
|--------|----------|
| `{artifact}-filtered.ttl` | Pre-processed TTL |
| `{artifact}_inferences.owl` | Reasoner output |
| `{artifact}_inferences.ttl` | Converted Turtle |

**Variables set on success:**

- `matwerk_last_successful_reason_run` (if artifact is "spreadsheets")
- `matwerk_last_successful_reason_run__{artifact}` (always)

## Task chain

```
init_data_dir -> pre_filter -> sunlet_reasoning -> robot_convert_to_ttl -> mark_reason_success
```

## Downstream

None. Trigger `validation_checks` after this DAG succeeds.
