# Dashboard

**DAG ID:** `dashboard`
**Schedule:** Daily at 00:00 UTC (`0 0 * * *`)
**File:** `dags/dashboard.py`

## What it does

Computes Knowledge Graph statistics by executing SPARQL queries against Virtuoso and stores results in a dashboard database (SQLite). Provides timestamped snapshots and `*_latest` views for the Superset dashboard.

!!! info "Automatic scheduling"
    This DAG runs automatically every day at midnight UTC. No manual triggering is required.

## Input

| Source | Description |
|--------|-------------|
| `matwerk-virtuoso_sparql` (Variable) | Virtuoso SPARQL endpoint |
| `matwerk-virtuoso_user` (Variable) | Virtuoso username |
| `matwerk-virtuoso_pass` (Variable) | Virtuoso password |
| `matwerk_dashboard_db` (Variable) | Database connection string (SQLite DSN) |

## Output

Database tables (timestamped, with `*_latest` views):

| Table | Content |
|-------|---------|
| `kg_graph_stats` | Triple/subject counts per graph |
| `kg_graph_class_counts` | Class usage per graph |
| `kg_graph_property_counts` | Property usage per graph |
| `kg_sankey_class_property` | Class-property relationships (for Sankey diagrams) |
| `kg_entity_type_counts` | Entity type instance counts |
| `kg_dataset_type_counts` | Dataset type counts |
| `kg_content_counts` | Summary (datasets, publications, events) |
| `kg_datasets` | Dataset list with metadata |
| `kg_org_city_counts` | Organizations by city |
| `kg_top_org_by_people` | Top organizations by people count |
| `kg_top_concepts` | Top 10 concepts per graph |

## Task chain

```
preflight -> ensure_tables -> list_mse_graphs
  -> [11 parallel write tasks]
    -> ensure_latest_views
```

## Downstream

None. Runs automatically every day.
