-- =============================================================================
-- metricflow_time_spine
--
-- Required by MetricFlow for cumulative, grain_to_date, and offset_window
-- metrics. Reuses the existing dim_date table — creates only a lightweight
-- view in the same target schema (no new schema created).
-- =============================================================================

{{
  config(
    materialized = 'view',
  )
}}

select
    cast(full_date as date) as date_day
from {{ source('ebi_semantic_poc', 'sem_poc_dim_date') }}
