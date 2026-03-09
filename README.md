# Retail Sales — dbt Semantic Layer POC

Replicates the Databricks metric view for retail sales using the **dbt Semantic Layer** powered by MetricFlow.

**Zero new schemas created** — all semantic models point directly to existing source tables via `source()`. The only physical object is a lightweight `metricflow_time_spine` view (created in your existing target schema) required by MetricFlow for cumulative and offset metrics.

---

## Concept Mapping: Databricks Metric View → dbt Semantic Layer

| Databricks Metric View | dbt Semantic Layer | File(s) |
|---|---|---|
| `source:` (fact table) | `model: source(...)` on semantic model | `sem_fact_sales_transactions.yml` |
| `joins:` (star-schema dims) | `entities:` with `type: foreign` (auto-join) | Same semantic model file |
| `dimensions:` (categorical) | `dimensions:` with `type: categorical` | Semantic model YAMLs |
| `dimensions:` (date-derived expr) | `dimensions:` with `type: time` + `expr:` | `sem_fact_sales_transactions.yml` |
| `dimensions:` (CASE expr) | `dimensions:` with `type: categorical` + `expr:` | `sem_fact_sales_transactions.yml` |
| `measures:` (SUM, COUNT DISTINCT) | `measures:` with `agg: sum/count_distinct` | `sem_fact_sales_transactions.yml` |
| `measures:` with `FILTER` clause | Metric-level `filter:` using Jinja | `_metrics.yml` |
| Composed via `MEASURE()` | `type: derived` metric with `expr:` | `_metrics.yml` |
| `window: trailing 7 day` | `type: cumulative` with `window: 7 days` | `_metrics.yml` |
| `window: cumulative` | `type: cumulative` (no window = all-time) | `_metrics.yml` |
| `window: current` + composed | `type: derived` with `offset_window` | `_metrics.yml` |
| `grain_to_date: year` (YTD) | `type: cumulative` with `grain_to_date: year` | `_metrics.yml` |
| `display_name` / `synonyms` / `format` | `label` / `description` / `meta:` | Metric/measure YAML |

---

## Project Structure

```
retail_sales_semantic_poc/
├── dbt_project.yml                          # Project config (no model materializations)
├── profiles.yml                             # Databricks connection (local dev only)
├── packages.yml                             # dbt_utils dependency
├── .gitignore
│
├── models/
│   ├── _sources.yml                         # Source definitions → existing catalog tables
│   ├── metricflow_time_spine.sql            # View on dim_date (required for cumulative metrics)
│   │
│   ├── semantic_models/                     # MetricFlow semantic model definitions
│   │   ├── sem_fact_sales_transactions.yml  #   Entities + Dimensions + Measures
│   │   ├── sem_dim_store.yml                #   Store dimension attributes
│   │   └── sem_dim_product.yml              #   Product dimension attributes
│   │
│   └── metrics/                             # Metric definitions
│       ├── _metrics.yml                     #   Derived, cumulative, period-over-period
│       └── _saved_queries.yml               #   Pre-built dashboard queries
│
├── macros/
├── seeds/
└── tests/
```

---

## What Gets Created Physically

| Object | Type | Schema | Notes |
|---|---|---|---|
| `metricflow_time_spine` | view | `dev_cf_ebi_semantic_poc` (existing) | Selects from dim_date |

Everything else is **pure YAML metadata** — semantic models, metrics, and saved queries generate SQL on the fly at query time. No tables, no new schemas.

---

## Key Differences from Databricks

### 1. Joins Are Implicit via Entities
No explicit `joins:` block. Define entities with matching names across semantic models — MetricFlow discovers join paths automatically.

### 2. Measures vs. Metrics (Two-Layer System)
- **Measures** = raw aggregation building blocks (inside `semantic_models:`). Set `create_metric: true` to auto-expose.
- **Metrics** = business-facing calculations (`derived`, `ratio`, `cumulative`).

### 3. MEASURE() Composability → Derived Metrics
`MEASURE(total_net_revenue) - MEASURE(total_cogs)` becomes `type: derived` with `expr: total_net_revenue - total_cogs`.

### 4. Window Measures → Cumulative + Derived Metrics
- Trailing windows → `type: cumulative` with `window: N days`
- Running totals → `type: cumulative` without a window
- YTD → `type: cumulative` with `grain_to_date: year`
- Day-over-day → `type: derived` with `offset_window: 1 day`

---

## Setup in dbt Cloud

1. Push this repo to GitHub.
2. Create a new project in dbt Cloud connected to the repo.
3. Configure Databricks connection (host, HTTP path, token) in the UI.
4. Set target schema to `dev_cf_ebi_semantic_poc`.
5. Enable the Semantic Layer under Project Settings → Semantic Layer.
6. Run `dbt deps` then `dbt build` (only creates the time spine view).
7. Validate: `dbt sl list metrics`
8. Test: `dbt sl query --metrics total_net_revenue --group-by transaction__transaction_date`

## Setup in dbt Core (Local)

```bash
pip install dbt-databricks dbt-metricflow[databricks]
export DBT_DATABRICKS_HOST=<your-host>
export DBT_DATABRICKS_HTTP_PATH=<your-http-path>
export DBT_DATABRICKS_TOKEN=<your-token>
dbt deps
dbt build
mf list metrics
mf query --metrics total_net_revenue --group-by transaction__transaction_date
```

---

## Querying Metrics

```bash
# Simple metric
dbt sl query --metrics total_net_revenue --group-by transaction__transaction_date

# Multi-metric with joined dimensions
dbt sl query \
  --metrics total_net_revenue,gross_margin_pct \
  --group-by store__region_name,product__category_name

# Cumulative metric
dbt sl query --metrics ytd_revenue --group-by transaction__transaction_date

# Saved query
dbt sl query --saved-query daily_sales_summary
```

---

## Extending This Project

To add more dimension tables (channel, customer, promotion):
1. Add the source table to `_sources.yml`.
2. Create a semantic model YAML with a primary entity matching the foreign entity name in the fact semantic model.
3. MetricFlow auto-discovers the join path — no SQL needed.
