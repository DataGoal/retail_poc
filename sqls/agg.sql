-- KPI: Total Net Revenue (ADDITIVE — safe to SUM everywhere)

SELECT
    d.year_num,
    d.quarter_name,
    d.month_name,
    p.division_name,
    p.category_name,
    s.region_name,
    s.country_name,
    SUM(f.net_revenue_usd)                       AS total_net_revenue,
    SUM(f.quantity_sold)                         AS total_units_sold,
    SUM(f.gross_revenue)                         AS total_gross_revenue,
    SUM(f.discount_amount)                       AS total_discount,
    SUM(f.cost_of_goods_sold)                    AS total_cogs,
    SUM(f.gross_margin)                          AS total_gross_margin
FROM `databricks-poc-updated`.nike_poc.fact_sales_transactions  f   -- TERADATA
JOIN `databricks-poc-updated`.nike_poc.dim_date                 d   ON d.date_sk      = f.transaction_date_sk
JOIN `databricks-poc-updated`.nike_poc.dim_product              p   ON p.product_sk   = f.product_sk
JOIN `databricks-poc-updated`.nike_poc.dim_store                s   ON s.store_sk     = f.store_sk
WHERE f.return_flag = 'N'
GROUP BY ROLLUP(d.year_num, d.quarter_name, d.month_name,
                p.division_name, p.category_name,
                s.region_name, s.country_name)
;

-- KPI: Period-End Inventory (SEMI-ADDITIVE — filter to one date)

SELECT
    d.month_year,
    p.category_name,
    p.subcategory_name,
    s.region_name,
    -- Correct: Sum across products and stores on period-END date
    SUM(i.on_hand_qty)                           AS period_end_on_hand,
    SUM(i.available_to_sell_qty)                 AS period_end_ats,
    SUM(i.inventory_cost_value)                  AS period_end_cost_value,
    SUM(i.inventory_retail_value)                AS period_end_retail_value,
    -- Average daily inventory (used for turn rate — additive base)
    AVG(i.on_hand_qty)                           AS avg_daily_on_hand
FROM `databricks-poc-updated`.nike_poc.fact_inventory_snapshot   i  -- TERADATA
JOIN `databricks-poc-updated`.nike_poc.dim_date                  d  ON d.date_sk    = i.snapshot_date_sk
JOIN `databricks-poc-updated`.nike_poc.dim_product               p  ON p.product_sk = i.product_sk
JOIN `databricks-poc-updated`.nike_poc.dim_store                 s  ON s.store_sk   = i.store_sk
WHERE d.full_date = LAST_DAY(d.full_date)  -- Period-end (last day of month)
GROUP BY d.month_year, p.category_name, p.subcategory_name, s.region_name
;


-- KPI: Non-Additive Metrics (Margins, Rates, Ratios)
WITH base AS (
    SELECT
        d.year_num,
        d.quarter_name,
        p.category_name,
        s.region_name,
        -- Additive components (safe to aggregate)
        SUM(f.net_revenue_usd)                   AS net_revenue,
        SUM(f.gross_revenue)                     AS gross_revenue,
        SUM(f.gross_margin)                      AS gross_margin,
        SUM(f.cost_of_goods_sold)                AS cogs,
        SUM(f.discount_amount)                   AS total_discount,
        COUNT(DISTINCT f.transaction_id)         AS num_transactions,
        SUM(f.quantity_sold)                     AS units_sold
    FROM `databricks-poc-updated`.nike_poc.fact_sales_transactions  f
    JOIN `databricks-poc-updated`.nike_poc.dim_date                 d   ON d.date_sk    = f.transaction_date_sk
    JOIN `databricks-poc-updated`.nike_poc.dim_product              p   ON p.product_sk = f.product_sk
    JOIN `databricks-poc-updated`.nike_poc.dim_store                s   ON s.store_sk   = f.store_sk
    WHERE f.return_flag = 'N'
    GROUP BY d.year_num, d.quarter_name, p.category_name, s.region_name
)
SELECT
    *,
    -- NON-ADDITIVE: Always compute from aggregated additive measures
    ROUND(gross_margin / NULLIF(net_revenue, 0) * 100, 2)   AS gross_margin_pct,
    ROUND(total_discount / NULLIF(gross_revenue, 0) * 100, 2) AS discount_rate_pct,
    ROUND(net_revenue / NULLIF(num_transactions, 0), 2)      AS avg_transaction_value,
    ROUND(net_revenue / NULLIF(units_sold, 0), 2)            AS avg_selling_price,
    ROUND(units_sold / NULLIF(num_transactions, 0), 2)       AS avg_units_per_transaction
FROM base
;

-- KPI: Revenue by Full Product & Date Hierarchy (GROUPING SETS)
-- Enables drill: Year > Quarter > Month > Week > Day
--           and: Division > Category > Subcategory > SKU
SELECT
    -- Date hierarchy flags
    GROUPING(d.year_num)           AS is_year_total,
    GROUPING(d.quarter_name)       AS is_quarter_total,
    GROUPING(d.month_name)         AS is_month_total,
    GROUPING(d.full_date)          AS is_day_level,
    -- Product hierarchy flags
    GROUPING(p.division_name)      AS is_division_total,
    GROUPING(p.category_name)      AS is_category_total,
    GROUPING(p.subcategory_name)   AS is_subcategory_total,
    GROUPING(p.product_line_name)  AS is_product_line_total,
    -- Dimensions
    d.year_num,
    d.quarter_name,
    d.month_name,
    d.full_date,
    p.division_name,
    p.category_name,
    p.subcategory_name,
    p.product_line_name,
    -- Measures
    SUM(f.net_revenue_usd)                      AS net_revenue,
    SUM(f.quantity_sold)                        AS units_sold,
    SUM(f.gross_margin)                         AS gross_margin,
    ROUND(SUM(f.gross_margin)/
          NULLIF(SUM(f.net_revenue_usd),0)*100,2) AS gm_pct
FROM `databricks-poc-updated`.nike_poc.fact_sales_transactions  f
JOIN `databricks-poc-updated`.nike_poc.dim_date     d  ON d.date_sk    = f.transaction_date_sk
JOIN `databricks-poc-updated`.nike_poc.dim_product  p  ON p.product_sk = f.product_sk
WHERE f.return_flag = 'N'
GROUP BY GROUPING SETS (
    -- Date rolls
    (d.year_num),
    (d.year_num, d.quarter_name),
    (d.year_num, d.quarter_name, d.month_name),
    (d.year_num, d.quarter_name, d.month_name, d.full_date),
    -- Product rolls
    (p.division_name),
    (p.division_name, p.category_name),
    (p.division_name, p.category_name, p.subcategory_name),
    (p.division_name, p.category_name, p.subcategory_name, p.product_line_name),
    -- Combined
    (d.year_num, p.division_name, p.category_name)
)
;


-- KPI: Net Revenue After Returns + Return Rate
WITH sales AS (
    SELECT product_sk, transaction_date_sk,
           SUM(net_revenue_usd) AS gross_net_revenue,
           SUM(quantity_sold)   AS units_sold
    FROM `databricks-poc-updated`.nike_poc.fact_sales_transactions
    WHERE return_flag = 'N'
    GROUP BY product_sk, transaction_date_sk
),
returns AS (
    SELECT product_sk, return_date_sk,
           SUM(refund_amount)      AS total_refunds,
           SUM(quantity_returned)  AS units_returned
    FROM `databricks-poc-updated`.nike_poc.fact_returns
    GROUP BY product_sk, return_date_sk
)
SELECT
    p.category_name,
    d.year_num,
    d.quarter_name,
    SUM(s.gross_net_revenue)                      AS gross_net_revenue,
    SUM(COALESCE(r.total_refunds, 0))             AS total_refunds,
    SUM(s.gross_net_revenue) -
      SUM(COALESCE(r.total_refunds, 0))           AS net_revenue_after_returns,
    SUM(s.units_sold)                             AS units_sold,
    SUM(COALESCE(r.units_returned, 0))            AS units_returned,
    -- NON-ADDITIVE
    ROUND(SUM(COALESCE(r.units_returned,0)) /
          NULLIF(SUM(s.units_sold),0)*100, 2)     AS return_rate_pct
FROM sales s
JOIN `databricks-poc-updated`.nike_poc.dim_product p  ON p.product_sk   = s.product_sk
JOIN `databricks-poc-updated`.nike_poc.dim_date    d  ON d.date_sk       = s.transaction_date_sk
LEFT JOIN returns          r  ON r.product_sk    = s.product_sk
                             AND r.return_date_sk = s.transaction_date_sk
GROUP BY p.category_name, d.year_num, d.quarter_name
;


