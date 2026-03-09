-- =============================================================================
-- metricflow_time_spine
--
-- Required by MetricFlow for cumulative metrics, offset_window, and
-- grain_to_date calculations. Generates a row per day.
-- If you have a dim_date table, you can replace this with a select from it.
-- =============================================================================

{{
  config(
    materialized = 'table',
  )
}}

with days as (
    {{
        dbt_utils.date_spine(
            'day',
            "date '2018-01-01'",
            "date '2030-12-31'"
        )
    }}
)

select
    cast(date_day as date) as date_day
from days
