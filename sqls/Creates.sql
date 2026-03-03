-- ============================================================
-- DROP STATEMENTS (uncomment to drop tables)
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.fact_sales_transactions;
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.fact_inventory_snapshot;
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.dim_customer;
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.dim_promotion;
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.dim_product;
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.dim_store;
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.dim_date;
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.dim_channel;
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.dim_employee;
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.dim_geography;
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.fact_returns;
DROP TABLE IF EXISTS `databricks-poc-updated`.nike_poc.fact_web_sessions;
-- ============================================================


-- ============================================================
-- TABLE: fact_sales_transactions
-- GRAIN: One row per sales line item
-- ============================================================
CREATE TABLE `databricks-poc-updated`.nike_poc.fact_sales_transactions (
    transaction_sk            BIGINT,
    transaction_id            STRING NOT NULL,
    line_number               SMALLINT NOT NULL,
    transaction_date_sk       INT NOT NULL,
    customer_sk               BIGINT,
    product_sk                BIGINT NOT NULL,
    store_sk                  INT NOT NULL,
    channel_sk                SMALLINT NOT NULL,
    promotion_sk              INT,
    employee_sk               INT,

    -- Measures
    quantity_sold             SMALLINT NOT NULL DEFAULT 1,
    unit_retail_price         DECIMAL(12,4) NOT NULL,
    unit_cost_price           DECIMAL(12,4) NOT NULL,
    gross_revenue             DECIMAL(14,4) NOT NULL,
    discount_amount           DECIMAL(12,4) NOT NULL DEFAULT 0,
    net_revenue               DECIMAL(14,4) NOT NULL,
    tax_amount                DECIMAL(12,4) NOT NULL DEFAULT 0,
    total_transaction_amount  DECIMAL(14,4) NOT NULL,
    cost_of_goods_sold        DECIMAL(14,4) NOT NULL,
    gross_margin              DECIMAL(14,4) NOT NULL,
    gross_margin_pct          DECIMAL(7,4),
    return_flag               STRING NOT NULL DEFAULT 'N',
    currency_code             STRING NOT NULL DEFAULT 'USD',
    exchange_rate             DECIMAL(10,6) NOT NULL DEFAULT 1.0,
    net_revenue_usd           DECIMAL(14,4) NOT NULL,

    -- Metadata
    load_timestamp            TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    source_system             STRING NOT NULL,

    CONSTRAINT pk_fact_sales PRIMARY KEY (transaction_sk)
)
USING DELTA
PARTITIONED BY (transaction_date_sk)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- ============================================================
-- TABLE: fact_inventory_snapshot
-- GRAIN: One row per product/store/day (daily snapshot)
-- ============================================================

CREATE TABLE `databricks-poc-updated`.nike_poc.fact_inventory_snapshot (
    inventory_sk              BIGINT,
    snapshot_date_sk          INT NOT NULL,
    product_sk                BIGINT NOT NULL,
    store_sk                  INT NOT NULL,

    -- Semi-Additive Measures
    on_hand_qty               INT NOT NULL DEFAULT 0,
    in_transit_qty            INT NOT NULL DEFAULT 0,
    on_order_qty              INT NOT NULL DEFAULT 0,
    reserved_qty              INT NOT NULL DEFAULT 0,
    available_to_sell_qty     INT NOT NULL DEFAULT 0,

    -- Additive Measures
    units_received_today      INT NOT NULL DEFAULT 0,
    units_sold_today          INT NOT NULL DEFAULT 0,
    units_returned_today      INT NOT NULL DEFAULT 0,
    units_shrinkage_today     INT NOT NULL DEFAULT 0,

    -- Valuation
    inventory_cost_value      DECIMAL(16,4) NOT NULL DEFAULT 0,
    inventory_retail_value    DECIMAL(16,4) NOT NULL DEFAULT 0,

    -- Thresholds
    reorder_point             INT,
    days_of_supply            DECIMAL(8,2),
    stock_turn_rate           DECIMAL(8,4),
    stockout_flag             STRING NOT NULL DEFAULT 'N',

    load_timestamp            TIMESTAMP NOT NULL DEFAULT current_timestamp(),

    CONSTRAINT pk_inv_snapshot PRIMARY KEY (inventory_sk)
)
USING DELTA
PARTITIONED BY (snapshot_date_sk)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- ============================================================
-- TABLE: dim_customer
-- SCD Type 2 (history preserved via date range)
-- ============================================================

CREATE TABLE `databricks-poc-updated`.nike_poc.dim_customer (
    customer_sk                BIGINT,
    customer_nk                STRING NOT NULL,

    -- Identity
    first_name                 STRING,
    last_name                  STRING,
    email_address_hash         STRING,
    phone_hash                 STRING,
    date_of_birth_sk           INT,
    gender_code                STRING,

    -- Loyalty
    loyalty_member_flag        BOOLEAN NOT NULL DEFAULT false,
    loyalty_tier               STRING,
    loyalty_points_balance     INT DEFAULT 0,
    loyalty_enroll_date_sk     INT,

    -- Segmentation
    customer_segment           STRING,
    preferred_sport            STRING,
    lifetime_value_band        STRING,
    acquisition_channel        STRING,

    -- Geography
    city                       STRING,
    state_province             STRING,
    country_code               STRING,
    postal_code                STRING,

    -- SCD2 Tracking
    effective_date             DATE NOT NULL,
    expiry_date                DATE NOT NULL DEFAULT DATE '9999-12-31',
    is_current_flag            BOOLEAN NOT NULL DEFAULT true,
    record_checksum            STRING,

    created_timestamp          TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    updated_timestamp          TIMESTAMP NOT NULL DEFAULT current_timestamp(),

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- ============================================================
-- TABLE: dim_promotion
-- ============================================================

CREATE TABLE `databricks-poc-updated`.nike_poc.dim_promotion (
    promotion_sk               INT,
    promotion_nk               STRING NOT NULL,
    promotion_name             STRING NOT NULL,
    promotion_type             STRING,
    discount_type              STRING,
    discount_value             DECIMAL(8,4),
    min_purchase_amount        DECIMAL(12,4),
    max_discount_cap           DECIMAL(12,4),

    -- Campaign Info
    campaign_name              STRING,
    campaign_category          STRING,
    marketing_channel          STRING,

    -- Validity
    start_date_sk              INT NOT NULL,
    end_date_sk                INT NOT NULL,

    -- Applicability
    applicable_product_category STRING,
    applicable_store_region    STRING,
    applicable_channel         STRING,
    stackable_flag             BOOLEAN NOT NULL DEFAULT false,

    -- Finance
    estimated_cost             DECIMAL(16,4),
    actual_cost                DECIMAL(16,4),
    funded_by                  STRING,

    -- Status
    promotion_status           STRING NOT NULL DEFAULT 'ACTIVE',

    created_timestamp          TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    updated_timestamp          TIMESTAMP NOT NULL DEFAULT current_timestamp(),

    CONSTRAINT pk_dim_promotion PRIMARY KEY (promotion_sk)
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- ============================================================
-- PLATFORM: DATABRICKS DELTA
-- TABLE: dim_product
-- HIERARCHY: Division > Category > Subcategory > Product_Line > SKU
-- ============================================================

CREATE TABLE `databricks-poc-updated`.nike_poc.dim_product (
    product_sk               BIGINT         NOT NULL,
    product_nk               STRING         NOT NULL,   -- Source SKU code
    upc_code                 STRING,
    -- Hierarchy Level 1: Division
    division_code            STRING         NOT NULL,
    division_name            STRING         NOT NULL,   -- e.g. Footwear, Apparel, Equipment
    -- Hierarchy Level 2: Category
    category_code            STRING         NOT NULL,
    category_name            STRING         NOT NULL,   -- e.g. Running, Basketball, Training
    -- Hierarchy Level 3: Subcategory
    subcategory_code         STRING         NOT NULL,
    subcategory_name         STRING         NOT NULL,   -- e.g. Road Running, Trail Running
    -- Hierarchy Level 4: Product Line
    product_line_code        STRING         NOT NULL,
    product_line_name        STRING         NOT NULL,   -- e.g. Air Max, Air Force 1, Pegasus
    -- Hierarchy Level 5: SKU
    product_name             STRING         NOT NULL,
    product_description      STRING,
    style_code               STRING,
    colorway                 STRING,
    size                     STRING,
    gender_target            STRING,        -- MENS / WOMENS / UNISEX / KIDS
    age_group                STRING,        -- ADULT / YOUTH / TODDLER
    -- Attributes
    material_composition     STRING,
    technology_features      STRING,        -- e.g. Air Cushioning, Flyknit
    sport_occasion           STRING,        -- PRIMARY sport use case
    is_hero_product          BOOLEAN        DEFAULT FALSE,
    is_exclusive             BOOLEAN        DEFAULT FALSE,
    is_collaboration         BOOLEAN        DEFAULT FALSE,
    collab_partner           STRING,
    -- Commercial
    standard_cost            DECIMAL(12,4),
    standard_retail_price    DECIMAL(12,4),
    launch_date_sk           INTEGER,
    discontinue_date_sk      INTEGER,
    is_active                BOOLEAN        DEFAULT TRUE,
    -- Metadata
    effective_date           DATE           NOT NULL,
    expiry_date              DATE           NOT NULL DEFAULT '9999-12-31',
    is_current_flag          BOOLEAN        DEFAULT TRUE,
    created_timestamp        TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp        TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (division_code)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- ============================================================
-- PLATFORM: DATABRICKS DELTA
-- TABLE: dim_store
-- HIERARCHY: Region > Country > State > Market > Store
-- ============================================================


CREATE TABLE `databricks-poc-updated`.nike_poc.dim_store (
    store_sk                 INT            NOT NULL,
    store_nk                 STRING         NOT NULL COMMENT 'Source system store ID',
    store_name               STRING         NOT NULL,
    store_type               STRING         NOT NULL,  -- FLAGSHIP / FACTORY / CONCEPT / PARTNER / ECOM
    -- Geographic Hierarchy
    region_code              STRING         NOT NULL,  -- AMER / EMEA / APAC
    region_name              STRING         NOT NULL,
    country_code             STRING         NOT NULL,
    country_name             STRING         NOT NULL,
    state_province_code      STRING,
    state_province_name      STRING,
    metro_market             STRING,
    city                     STRING,
    store_address            STRING,
    postal_code              STRING,
    latitude                 DECIMAL(10,7),
    longitude                DECIMAL(10,7),
    -- Store Attributes
    square_footage           INT,
    num_floors               SMALLINT,
    store_format             STRING,        -- FULL_LINE / SPORT / RUN / BASKETBALL
    channel_type             STRING,        -- BRICK_MORTAR / ECOMMERCE / WHOLESALE / APP
    is_digital               BOOLEAN        DEFAULT FALSE,
    -- Operational
    open_date_sk             INTEGER,
    close_date_sk            INTEGER,
    is_active                BOOLEAN        DEFAULT TRUE,
    store_manager_employee_sk INT,
    -- Ownership
    ownership_model          STRING,        -- OWNED / FRANCHISE / PARTNER / DIRECT
    partner_account_id       STRING,
    created_timestamp        TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (region_code)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- ============================================================
-- PLATFORM: DATABRICKS DELTA
-- TABLE: dim_date
-- HIERARCHY: Day > Week > Month > Quarter > Year (+ Fiscal)
-- ============================================================

CREATE TABLE `databricks-poc-updated`.nike_poc.dim_date (
    date_sk                  INT            NOT NULL,   -- YYYYMMDD integer key
    full_date                DATE           NOT NULL,
    -- Gregorian Hierarchy
    day_of_week_num          TINYINT        NOT NULL,   -- 1=Sun, 7=Sat
    day_of_week_name         STRING         NOT NULL,
    day_of_week_abbr         STRING         NOT NULL,
    day_of_month             TINYINT        NOT NULL,
    day_of_year              SMALLINT       NOT NULL,
    week_of_year             TINYINT        NOT NULL,
    iso_week_number          TINYINT        NOT NULL,
    month_num                TINYINT        NOT NULL,
    month_name               STRING         NOT NULL,
    month_abbr               STRING         NOT NULL,
    month_year               STRING         NOT NULL,   -- e.g. 'Jan-2024'
    quarter_num              TINYINT        NOT NULL,
    quarter_name             STRING         NOT NULL,   -- e.g. 'Q1-2024'
    year_num                 SMALLINT       NOT NULL,
    first_day_of_month       DATE           NOT NULL,
    last_day_of_month        DATE           NOT NULL,
    first_day_of_quarter     DATE           NOT NULL,
    last_day_of_quarter      DATE           NOT NULL,
    -- Fiscal Calendar (Nike FY ends May 31)
    fiscal_week_num          TINYINT,
    fiscal_month_num         TINYINT,
    fiscal_quarter_num       TINYINT,
    fiscal_year_num          SMALLINT,
    fiscal_quarter_name      STRING,        -- e.g. 'FQ1-FY2025'
    -- Retail Calendar (4-5-4 week)
    retail_period_num        TINYINT,
    retail_week_num          SMALLINT,
    retail_year_num          SMALLINT,
    -- Flags
    is_weekday               BOOLEAN        NOT NULL,
    is_weekend               BOOLEAN        NOT NULL,
    is_holiday               BOOLEAN        NOT NULL DEFAULT FALSE,
    holiday_name             STRING,
    is_black_friday          BOOLEAN        NOT NULL DEFAULT FALSE,
    is_cyber_monday          BOOLEAN        NOT NULL DEFAULT FALSE,
    is_back_to_school        BOOLEAN        NOT NULL DEFAULT FALSE,
    season                   STRING,        -- SPRING / SUMMER / FALL / HOLIDAY
    CONSTRAINT pk_dim_date PRIMARY KEY (date_sk)
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- ============================================================
-- PLATFORM: DATABRICKS DELTA
-- TABLE: dim_channel
-- ============================================================

CREATE TABLE `databricks-poc-updated`.nike_poc.dim_channel (
    channel_sk               SMALLINT       NOT NULL,
    channel_nk               STRING         NOT NULL,
    channel_name             STRING         NOT NULL,
    channel_category         STRING         NOT NULL,  -- DIRECT / WHOLESALE / MARKETPLACE
    channel_type             STRING         NOT NULL,  -- STORE / WEB / APP / PHONE / WHOLESALE
    is_digital               BOOLEAN        NOT NULL DEFAULT FALSE,
    is_owned                 BOOLEAN        NOT NULL DEFAULT FALSE,  -- Nike-owned vs partner
    platform_name            STRING,        -- Nike.com / SNKRS App / Foot Locker / Amazon
    CONSTRAINT pk_dim_channel PRIMARY KEY (channel_sk)
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- ============================================================
-- PLATFORM: DATABRICKS DELTA
-- TABLE: dim_employee
-- ============================================================

CREATE TABLE `databricks-poc-updated`.nike_poc.dim_employee (
    employee_sk              INT            NOT NULL,
    employee_nk              STRING         NOT NULL,
    full_name_hash           STRING,        -- Hashed PII
    job_title                STRING,
    department               STRING,
    store_sk                 INT,
    hire_date_sk             INTEGER,
    termination_date_sk      INTEGER,
    is_active                BOOLEAN        DEFAULT TRUE,
    effective_date           DATE           NOT NULL,
    expiry_date              DATE           NOT NULL DEFAULT '9999-12-31',
    is_current_flag          BOOLEAN        DEFAULT TRUE,
    CONSTRAINT pk_dim_employee PRIMARY KEY (employee_sk)
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- ============================================================
-- PLATFORM: DATABRICKS DELTA
-- TABLE: dim_geography
-- HIERARCHY: World > Region > Sub-Region > Country > State > Metro
-- ============================================================

CREATE TABLE `databricks-poc-updated`.nike_poc.dim_geography (
    geo_sk                   INT            NOT NULL,
    geo_level                STRING         NOT NULL,  -- REGION/COUNTRY/STATE/METRO/CITY
    geo_code                 STRING         NOT NULL,
    geo_name                 STRING         NOT NULL,
    parent_geo_sk            INT,
    world_region             STRING,        -- e.g. AMER
    sub_region               STRING,        -- e.g. NORTH_AMERICA
    country_code             STRING,
    country_name             STRING,
    state_code               STRING,
    state_name               STRING,
    metro_market             STRING,
    city                     STRING,
    population               BIGINT,
    gdp_per_capita_usd       DECIMAL(14,2),
    nike_market_priority     STRING,        -- TIER_1 / TIER_2 / TIER_3
    CONSTRAINT pk_dim_geo PRIMARY KEY (geo_sk)
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- ============================================================
-- PLATFORM: DATABRICKS DELTA
-- TABLE: fact_returns
-- GRAIN: One row per return line item
-- ============================================================

CREATE TABLE `databricks-poc-updated`.nike_poc.fact_returns (
    return_sk                BIGINT         NOT NULL,
    return_id                STRING         NOT NULL,
    return_line_number       SMALLINT       NOT NULL,
    return_date_sk           INTEGER        NOT NULL,   -- FK -> dim_date
    original_transaction_sk  BIGINT,                    -- FK -> fact_sales_transactions
    original_transaction_date_sk INTEGER,
    customer_sk              BIGINT,                    -- FK -> dim_customer
    product_sk               BIGINT         NOT NULL,   -- FK -> dim_product
    store_sk                 INT            NOT NULL,   -- FK -> dim_store
    channel_sk               SMALLINT       NOT NULL,   -- FK -> dim_channel
    return_reason_code       STRING,
    return_reason_desc       STRING,
    return_condition         STRING,        -- SALEABLE / DAMAGED / DEFECTIVE
    -- Measures
    quantity_returned        SMALLINT       NOT NULL,
    return_retail_amount     DECIMAL(14,4)  NOT NULL,
    return_cost_amount       DECIMAL(14,4)  NOT NULL,
    refund_amount            DECIMAL(14,4)  NOT NULL,
    restocking_fee           DECIMAL(10,4)  NOT NULL DEFAULT 0,
    return_shipping_cost     DECIMAL(10,4)  NOT NULL DEFAULT 0,
    days_since_purchase      SMALLINT,
    is_within_return_window  BOOLEAN        NOT NULL DEFAULT TRUE,
    load_timestamp           TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (return_date_sk)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- ============================================================
-- PLATFORM: DATABRICKS DELTA
-- TABLE: fact_web_sessions
-- GRAIN: One row per digital session
-- ============================================================

CREATE TABLE `databricks-poc-updated`.nike_poc.fact_web_sessions (
    session_sk               BIGINT         NOT NULL,
    session_id               STRING         NOT NULL,
    session_date_sk          INTEGER        NOT NULL,  -- FK -> dim_date
    customer_sk              BIGINT,
    channel_sk               SMALLINT,
    -- Session Attributes
    device_type              STRING,        -- DESKTOP / MOBILE / TABLET / APP
    os_platform              STRING,
    browser_name             STRING,
    entry_page               STRING,
    exit_page                STRING,
    traffic_source           STRING,        -- ORGANIC / PAID / EMAIL / SOCIAL / DIRECT
    campaign_id              STRING,
    -- Measures
    session_duration_seconds INT,
    pages_viewed             SMALLINT,
    products_viewed          SMALLINT,
    products_added_to_cart   SMALLINT,
    cart_value               DECIMAL(12,4),
    checkout_initiated_flag  BOOLEAN        NOT NULL DEFAULT FALSE,
    purchase_completed_flag  BOOLEAN        NOT NULL DEFAULT FALSE,
    purchase_transaction_sk  BIGINT,
    purchase_revenue         DECIMAL(14,4),
    bounce_flag              BOOLEAN        NOT NULL DEFAULT FALSE,
    load_timestamp           TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (session_date_sk)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);
