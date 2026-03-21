"""
DataMind — Warehouse Schema (DuckDB Star Schema)

Dimensional model:
  FACT:   fact_sales
  DIMS:   dim_date, dim_product, dim_customer, dim_geography

This mirrors production Snowflake/BigQuery patterns — same SQL, 
DuckDB is the local analytical engine.
"""

SCHEMA_DDL = """
-- ── Dimension: Date ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,   -- surrogate key YYYYMMDD
    full_date       DATE        NOT NULL,
    year            SMALLINT    NOT NULL,
    quarter         TINYINT     NOT NULL,
    month           TINYINT     NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    week            TINYINT     NOT NULL,
    day_of_month    TINYINT     NOT NULL,
    day_of_week     TINYINT     NOT NULL,
    day_name        VARCHAR(10) NOT NULL,
    is_weekend      BOOLEAN     NOT NULL
);

-- ── Dimension: Product ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_key     INTEGER PRIMARY KEY,   -- surrogate key
    stock_code      VARCHAR(20) NOT NULL UNIQUE,
    description     TEXT        NOT NULL,
    price_band      VARCHAR(10) NOT NULL,  -- LOW / MID / HIGH
    unit_price      DOUBLE      NOT NULL   -- latest known price
);

-- ── Dimension: Customer ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    INTEGER PRIMARY KEY,
    customer_id     VARCHAR(20) NOT NULL UNIQUE,
    first_seen      DATE,
    last_seen       DATE,
    country         VARCHAR(50),
    customer_segment VARCHAR(10)           -- HIGH / MID / LOW (RFM-based)
);

-- ── Dimension: Geography ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_geography (
    geo_key         INTEGER PRIMARY KEY,
    country         VARCHAR(50) NOT NULL UNIQUE,
    region          VARCHAR(50)            -- Europe / Americas / Asia-Pacific / Other
);

-- ── Fact: Sales ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id         BIGINT PRIMARY KEY,
    invoice         VARCHAR(20) NOT NULL,
    date_key        INTEGER     REFERENCES dim_date(date_key),
    product_key     INTEGER     REFERENCES dim_product(product_key),
    customer_key    INTEGER     REFERENCES dim_customer(customer_key),
    geo_key         INTEGER     REFERENCES dim_geography(geo_key),
    quantity        INTEGER     NOT NULL,
    unit_price      DOUBLE      NOT NULL,
    revenue         DOUBLE      NOT NULL,
    invoice_date    TIMESTAMP   NOT NULL
);

-- ── Aggregate: Daily Sales (materialised-view pattern) ───────────────────────
CREATE TABLE IF NOT EXISTS agg_daily_sales (
    date_key        INTEGER     NOT NULL,
    product_key     INTEGER     NOT NULL,
    total_quantity  INTEGER     NOT NULL,
    total_revenue   DOUBLE      NOT NULL,
    num_orders      INTEGER     NOT NULL,
    PRIMARY KEY (date_key, product_key)
);
"""

DROP_DDL = """
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS agg_daily_sales;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_geography;
"""
