# Retail Sales — dbt Semantic Layer POC

Replicates the Databricks metric view for retail sales using the **dbt Semantic Layer** powered by MetricFlow.

---

## Concept Mapping: Databricks Metric View → dbt Semantic Layer

| Databricks Metric View | dbt Semantic Layer | File(s) |
|---|---|---|
| `source:` (fact table) | `semantic_model.model: ref(...)` | `sem_fact_sales_transactions.yml` |
| `joins:` (star-schema dims) | `entities:` with `type: foreign` (auto-join) | Same semantic model file |
| `dimensions:` (categorical) | `dimensions:` with `type: categorical` | Semantic model YAMLs |
| `dimensions:` (date-derived) | `dimensions:` with `type: time` | `sem_fact_sales_transactions.yml` |
| `dimensions:` (CASE expr) | Derived column in staging SQL | `stg_fact_sales_transactions.sql` |
| `measures:` (SUM, COUNT DISTINCT) | `measures:` with `agg: sum/count_distinct` | `sem_fact_sales_transactions.yml` |
| `measures:` with `FILTER` clause | Metric-level `filter:` using Jinja | `_metrics.yml` |
| Composed via `MEASURE()` | `type: derived` metric with `expr:` | `_metrics.yml` |
| Ratio (revenue/count) | `type: derived` metric | `_metrics.yml` |
| `window: trailing 7 day` | `type: cumulative` with `window: 7 days` | `_metrics.yml` |
| `window: cumulative` | `type: cumulative` (no window = all-time) | `_metrics.yml` |
| `window: current` + composed | `type: derived` with `offset_window` | `_metrics.yml` |
| `grain_to_date: year` (YTD) | `type: cumulative` with `grain_to_date: year` | `_metrics.yml` |
| `display_name`, `synonyms`, `format` | `label`, `description`, `meta:` | Metric/measure YAML |

---

## Project Structure

```
retail_sales_semantic_poc/
├── dbt_project.yml                       # Project config
├── profiles.yml                          # Databricks connection (local dev only)
├── packages.yml                          # dbt_utils dependency
├── .gitignore
│
├── models/
│   ├── staging/                          # Thin views over source tables
│   │   ├── _sources.yml                  # Source definitions (catalog.schema.table)
│   │   ├── _staging.yml                  # Model docs
│   │   ├── stg_fact_sales_transactions.sql
│   │   ├── stg_dim_store.sql
│   │   └── stg_dim_product.sql
│   │
│   ├── semantic_models/                  # MetricFlow semantic model definitions
│   │   ├── sem_fact_sales_transactions.yml  # Entities + Dimensions + Measures
│   │   ├── sem_dim_store.yml               # Store dimension attributes
│   │   └── sem_dim_product.yml             # Product dimension attributes
│   │
│   ├── metrics/                          # Metric definitions
│   │   ├── _metrics.yml                  # Derived, ratio, cumulative metrics
│   │   └── _saved_queries.yml            # Pre-built dashboard queries
│   │
│   └── marts/retail_sales/
│       └── metricflow_time_spine.sql     # Required for cumulative/offset metrics
│
├── macros/
├── seeds/
└── tests/
```

---

## Key Differences to Understand

### 1. Joins Are Implicit via Entities
In Databricks you write explicit `joins:` blocks. In dbt, you define **entities** (join keys) on each semantic model — MetricFlow discovers join paths automatically. The shared entity name (e.g., `store`) across fact and dimension semantic models is the link.

### 2. Measures vs. Metrics (Two-Layer System)
- **Measures** = raw aggregation building blocks defined inside `semantic_models:`. Not directly queryable unless `create_metric: true`.
- **Metrics** = business-facing calculations defined in `metrics:`. Can be `simple` (wraps a measure), `derived` (expression over metrics), `ratio`, or `cumulative`.

### 3. MEASURE() Composability → Derived Metrics
Databricks uses `MEASURE(total_net_revenue) - MEASURE(total_cogs)` for gross margin. In dbt this becomes a `type: derived` metric with `expr: total_net_revenue - total_cogs`.

### 4. Window Measures → Cumulative + Derived Metrics
- **Trailing windows** → `type: cumulative` with `window: N days`
- **Running totals** → `type: cumulative` without a window
- **YTD** → `type: cumulative` with `grain_to_date: year`
- **Day-over-day** → `type: derived` with `offset_window: 1 day`

### 5. Semantic Metadata
Databricks `display_name`/`synonyms`/`format` maps to dbt `label`/`description`/`meta:`. The dbt Semantic Layer doesn't have native `format` or `synonyms` keys — use `meta:` for custom metadata that downstream BI tools can consume.

---

## Setup in dbt Cloud

1. **Create a new project** in dbt Cloud pointing to your GitHub repo.
2. **Configure the connection** to your Databricks workspace (host, HTTP path, token) — this replaces `profiles.yml`.
3. **Enable the Semantic Layer** under Project Settings → Semantic Layer.
4. Run `dbt deps` to install packages.
5. Run `dbt build` to create staging views and the time spine table.
6. Run `dbt sl list metrics` to verify all metrics are registered.
7. Run `dbt sl query --metrics total_net_revenue --group-by transaction__transaction_date` to test.

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
# Simple metric query
dbt sl query --metrics total_net_revenue --group-by transaction__transaction_date

# Multi-metric with dimension from joined table
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

To add more dimension tables (channel, customer, promotion), follow the same pattern:
1. Add a staging SQL model (`stg_dim_<name>.sql`).
2. Add a semantic model YAML with a primary entity matching the foreign entity name in the fact semantic model.
3. MetricFlow auto-discovers the join path.
