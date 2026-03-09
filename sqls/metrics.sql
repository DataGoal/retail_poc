-------------------------------------------------------------------------------
-- Sample SQL Queries for retail_sales_metric_view
-- All queries use MEASURE() function against the metric view
-- Replace '`databricks-poc-updated`.nike_poc.star_schema_metrics' with your actual path
-------------------------------------------------------------------------------


-- =============================================================================
-- 1. BASIC ATOMIC MEASURES — Revenue, Units, Transactions by Region & Category
-- =============================================================================
SELECT
    region_name,
    category_name,
    MEASURE(total_net_revenue)   AS net_revenue,
    MEASURE(total_gross_revenue) AS gross_revenue,
    MEASURE(total_quantity)      AS units_sold,
    MEASURE(transaction_count)   AS txn_count,
    MEASURE(unique_customers)    AS customers
FROM `databricks-poc-updated`.nike_poc.star_schema_metrics
WHERE transaction_date >= DATE'2024-01-01'
GROUP BY ALL;


-- =============================================================================
-- 2. COMPOSED MEASURES — Margins, AOV, ASP, Discount & Return Rates
-- =============================================================================
SELECT
    division_name,
    category_name,
    MEASURE(total_net_revenue) AS net_revenue,
    MEASURE(total_cogs)        AS cogs,
    MEASURE(gross_margin)      AS gross_margin,
    MEASURE(gross_margin_pct)  AS margin_pct,
    MEASURE(avg_order_value)   AS aov,
    MEASURE(avg_selling_price) AS asp,
    MEASURE(discount_rate)     AS discount_rate,
    MEASURE(return_rate)       AS return_rate
FROM `databricks-poc-updated`.nike_poc.star_schema_metrics
WHERE transaction_date >= DATE'2024-01-01'
GROUP BY ALL;


-- =============================================================================
-- 3. WINDOW MEASURE — Trailing 7-Day Revenue & Customers by Store
-- =============================================================================
SELECT
    transaction_date,
    store_name,
    MEASURE(trailing_7d_revenue)   AS t7d_revenue,
    MEASURE(trailing_7d_customers) AS t7d_customers
FROM `databricks-poc-updated`.nike_poc.star_schema_metrics
WHERE transaction_date >= DATE'2024-06-01'
GROUP BY ALL;


-- =============================================================================
-- 4. WINDOW MEASURE — Day-over-Day Growth (Period-over-Period)
-- =============================================================================
SELECT
    transaction_date,
    region_name,
    MEASURE(current_day_revenue)  AS today_revenue,
    MEASURE(previous_day_revenue) AS yesterday_revenue,
    MEASURE(day_over_day_growth)  AS dod_growth_pct
FROM `databricks-poc-updated`.nike_poc.star_schema_metrics
WHERE transaction_date >= DATE'2024-06-01'
GROUP BY ALL;


-- =============================================================================
-- 5. WINDOW MEASURE — Cumulative (Running Total) Revenue
-- =============================================================================
SELECT
    transaction_date,
    MEASURE(total_net_revenue)  AS daily_revenue,
    MEASURE(cumulative_revenue) AS running_total
FROM `databricks-poc-updated`.nike_poc.star_schema_metrics
WHERE transaction_date >= DATE'2024-01-01'
GROUP BY ALL;


-- =============================================================================
-- 6. WINDOW MEASURE — Year-to-Date Revenue (Period-to-Date, dual window)
-- =============================================================================
SELECT
    transaction_date,
    DATE_TRUNC('month', transaction_date) AS month,
    MEASURE(ytd_revenue) AS ytd_revenue
FROM `databricks-poc-updated`.nike_poc.star_schema_metrics
WHERE transaction_date >= DATE'2024-01-01'
GROUP BY ALL;


-- =============================================================================
-- 7. JOINS — Slicing fact measures by dimension attributes from joined tables
-- =============================================================================
SELECT
    region_name,
    country_name,
    store_format,
    division_name,
    gender_target,
    MEASURE(total_net_revenue) AS net_revenue,
    MEASURE(gross_margin_pct)  AS margin_pct,
    MEASURE(avg_order_value)   AS aov
FROM `databricks-poc-updated`.nike_poc.star_schema_metrics
WHERE transaction_date >= DATE'2024-01-01'
GROUP BY ALL;


-- =============================================================================
-- 8. DERIVED DIMENSION — Using the CASE-based revenue_tier dimension
-- =============================================================================
SELECT
    revenue_tier,
    MEASURE(transaction_count)  AS txn_count,
    MEASURE(total_net_revenue)  AS net_revenue,
    MEASURE(avg_selling_price)  AS asp,
    MEASURE(return_rate)        AS return_rate
FROM `databricks-poc-updated`.nike_poc.star_schema_metrics
WHERE transaction_date >= DATE'2024-01-01'
GROUP BY ALL;


-- =============================================================================
-- 9. COMBINED — Trailing window + Composed + Join dimensions (full KPI board)
-- =============================================================================
SELECT
    transaction_date,
    region_name,
    category_name,
    MEASURE(total_net_revenue)     AS daily_revenue,
    MEASURE(trailing_7d_revenue)   AS t7d_revenue,
    MEASURE(cumulative_revenue)    AS running_total,
    MEASURE(ytd_revenue)           AS ytd_revenue,
    MEASURE(day_over_day_growth)   AS dod_growth_pct,
    MEASURE(gross_margin_pct)      AS margin_pct,
    MEASURE(avg_order_value)       AS aov,
    MEASURE(discount_rate)         AS discount_rate,
    MEASURE(trailing_7d_customers) AS t7d_customers
FROM `databricks-poc-updated`.nike_poc.star_schema_metrics
WHERE transaction_date >= DATE'2024-06-01'
GROUP BY ALL;