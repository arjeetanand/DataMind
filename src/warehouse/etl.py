"""
DataMind — ETL Pipeline
Parquet Data Lake → DuckDB Star Schema (Dimensional Model)

Pipeline:
  1. Read Parquet lake
  2. Build dimension tables (SCD Type 1)
  3. Build fact_sales with FK lookups
  4. Materialise agg_daily_sales
"""

import duckdb
import pandas as pd
import numpy as np
import logging
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))
from config.settings import DB_PATH, PARQUET_DIR
from src.warehouse.schema import SCHEMA_DDL, DROP_DDL

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ── Region mapping ────────────────────────────────────────────────────────────
REGION_MAP = {
    "United Kingdom": "Europe", "Germany": "Europe", "France": "Europe",
    "Netherlands": "Europe", "Belgium": "Europe", "Switzerland": "Europe",
    "Spain": "Europe", "Portugal": "Europe", "Italy": "Europe",
    "Denmark": "Europe", "Finland": "Europe", "Norway": "Europe",
    "Sweden": "Europe", "Poland": "Europe", "Austria": "Europe",
    "Cyprus": "Europe", "Greece": "Europe", "Malta": "Europe",
    "Australia": "Asia-Pacific", "Japan": "Asia-Pacific",
    "Singapore": "Asia-Pacific", "Bahrain": "Asia-Pacific",
    "USA": "Americas", "Canada": "Americas", "Brazil": "Americas",
    "EIRE": "Europe", "Channel Islands": "Europe",
}


def _build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Generate the Date Dimension table from invoice timestamps.
    Extracts granular attributes like year, quarter, month, and weekend flags."""
    dates = df["InvoiceDate"].dt.normalize().unique()
    rows = []
    for d in sorted(dates):
        rows.append({
            "date_key"    : int(pd.Timestamp(d).strftime("%Y%m%d")),
            "full_date"   : pd.Timestamp(d).date(),
            "year"        : d.year,
            "quarter"     : (d.month - 1) // 3 + 1,
            "month"       : d.month,
            "month_name"  : d.strftime("%B"),
            "week"        : d.isocalendar()[1],
            "day_of_month": d.day,
            "day_of_week" : d.weekday(),
            "day_name"    : d.strftime("%A"),
            "is_weekend"  : d.weekday() >= 5,
        })
    return pd.DataFrame(rows)


def _price_band(price: float) -> str:
    """Categorize a product's unit price into defined market bands.
    Labels items as LOW, MID, or HIGH based on threshold values."""
    if price < 2.0:   return "LOW"
    if price < 10.0:  return "MID"
    return "HIGH"


def _build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    """Construct the Product Dimension table with latest descriptions and price bands.
    Ensures unique stock codes are mapped to surrogate keys for the star schema."""
    latest = (
        df.sort_values("InvoiceDate")
          .groupby("StockCode")
          .last()
          .reset_index()[["StockCode", "Description", "Price"]]
    )
    latest["price_band"] = latest["Price"].apply(_price_band)
    latest["product_key"] = range(1, len(latest) + 1)
    latest.rename(columns={"Price": "unit_price", "Description": "description",
                            "StockCode": "stock_code"}, inplace=True)
    return latest[["product_key", "stock_code", "description", "price_band", "unit_price"]]


def _rfm_segment(rfm_score: float) -> str:
    """Assign a customer segment label based on their normalized RFM score.
    Maps numeric scores to HIGH, MID, or LOW engagement buckets."""
    if rfm_score >= 0.66: return "HIGH"
    if rfm_score >= 0.33: return "MID"
    return "LOW"


def _build_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """Create the Customer Dimension table and perform RFM analysis.
    Calculates recency, frequency, and monetary scores to segment the customer base."""
    today = df["InvoiceDate"].max()
    rfm = df.groupby("CustomerID").agg(
        recency   = ("InvoiceDate", lambda x: (today - x.max()).days),
        frequency = ("Invoice",     "nunique"),
        monetary  = ("Revenue",     "sum"),
        first_seen= ("InvoiceDate", "min"),
        last_seen = ("InvoiceDate", "max"),
        country   = ("Country",     "last"),
    ).reset_index()

    # Normalise RFM scores (0–1)
    for col in ["recency", "frequency", "monetary"]:
        mn, mx = rfm[col].min(), rfm[col].max()
        if mx > mn:
            rfm[f"{col}_norm"] = (rfm[col] - mn) / (mx - mn)
        else:
            rfm[f"{col}_norm"] = 0.5

    # Recency: lower is better → invert
    rfm["rfm_score"] = (
        (1 - rfm["recency_norm"]) * 0.4 +
        rfm["frequency_norm"]     * 0.3 +
        rfm["monetary_norm"]      * 0.3
    )
    rfm["customer_segment"] = rfm["rfm_score"].apply(_rfm_segment)
    rfm["customer_key"] = range(1, len(rfm) + 1)
    rfm.rename(columns={"CustomerID": "customer_id"}, inplace=True)
    
    # ADDED: Special surrogate for Guest transactions (missing CustomerID)
    guest_key = len(rfm) + 1
    guest_row = pd.DataFrame([{
        "customer_key": guest_key,
        "customer_id": "GUEST",
        "first_seen": None,
        "last_seen": None,
        "country": "Unknown",
        "customer_segment": "LOW"
    }])
    rfm = pd.concat([rfm, guest_row], ignore_index=True)

    return rfm[["customer_key", "customer_id", "first_seen", "last_seen",
                "country", "customer_segment"]]


def _build_dim_geography(df: pd.DataFrame) -> pd.DataFrame:
    """Build the Geography Dimension by mapping countries to continental regions.
    Ensures standard geographic groupings for regional sales analysis."""
    countries = df["Country"].dropna().unique()
    rows = [{"geo_key": i + 1, "country": c,
              "region": REGION_MAP.get(c, "Other")}
             for i, c in enumerate(sorted(countries))]
    return pd.DataFrame(rows)


def _build_fact_sales(df: pd.DataFrame, dim_date, dim_product,
                      dim_customer, dim_geography) -> pd.DataFrame:
    """Construct the central Sales Fact table by mapping raw events to dimension keys.
    Integrates all dimensions and calculates row-level sales metrics."""
    # Build lookup maps
    date_map    = {d.date(): k for k, d in
                   zip(dim_date["date_key"], pd.to_datetime(dim_date["full_date"]))}
    prod_map    = dict(zip(dim_product["stock_code"],   dim_product["product_key"]))
    cust_map    = dict(zip(dim_customer["customer_id"], dim_customer["customer_key"]))
    geo_map     = dict(zip(dim_geography["country"],    dim_geography["geo_key"]))

    fact = df.copy()
    fact["date_key"]     = fact["InvoiceDate"].dt.date.map(date_map)
    fact["product_key"]  = fact["StockCode"].map(prod_map)
    
    # Handle Guest Transactions: Map null/empty CustomerIDs to 'GUEST'
    fact["CustomerID"]   = fact["CustomerID"].fillna("GUEST").replace({"": "GUEST"})
    fact["customer_key"] = fact["CustomerID"].map(cust_map)
    
    fact["geo_key"]      = fact["Country"].map(geo_map)

    # REFINED: Drop only if critical keys (date/product) are missing.
    # Customer and Geo are now more robustly handled.
    fact.dropna(subset=["date_key", "product_key"], inplace=True)

    for col in ["date_key", "product_key", "customer_key", "geo_key"]:
        fact[col] = fact[col].astype(int)

    fact["sale_id"] = range(1, len(fact) + 1)
    fact.rename(columns={"Quantity": "quantity", "Price": "unit_price",
                         "Invoice": "invoice", "InvoiceDate": "invoice_date", "Revenue": "revenue"}, inplace=True)

    return fact[["sale_id", "invoice", "date_key", "product_key", "customer_key",
                 "geo_key", "quantity", "unit_price", "revenue", "invoice_date"]]


def run_etl(df: pd.DataFrame, db_path: Path = DB_PATH, recreate: bool = False) -> duckdb.DuckDBPyConnection:
    """Execute the full ETL pipeline to populate the DuckDB star schema.
    Coordinates dimension building, fact loading, and daily aggregation materialization."""
    conn = duckdb.connect(str(db_path))

    if recreate:
        log.info("Dropping existing tables...")
        conn.execute(DROP_DDL)

    log.info("Creating schema...")
    conn.execute(SCHEMA_DDL)

    log.info("Building dimension tables...")
    dim_date      = _build_dim_date(df)
    dim_product   = _build_dim_product(df)
    dim_customer  = _build_dim_customer(df)
    dim_geography = _build_dim_geography(df)

    for name, frame in [("dim_date", dim_date), ("dim_product", dim_product),
                        ("dim_customer", dim_customer), ("dim_geography", dim_geography)]:
        conn.execute(f"DELETE FROM {name}")
        conn.execute(f"INSERT INTO {name} SELECT * FROM frame")
        log.info(f"  {name}: {len(frame):,} rows loaded")

    log.info("Building fact_sales...")
    fact = _build_fact_sales(df, dim_date, dim_product, dim_customer, dim_geography)
    conn.execute("DELETE FROM fact_sales")
    conn.execute("INSERT INTO fact_sales SELECT * FROM fact")
    log.info(f"  fact_sales: {len(fact):,} rows loaded")

    log.info("Materialising agg_daily_sales...")
    conn.execute("""
        DELETE FROM agg_daily_sales;
        INSERT INTO agg_daily_sales
        SELECT  date_key,
                product_key,
                SUM(quantity)  AS total_quantity,
                SUM(revenue)   AS total_revenue,
                COUNT(DISTINCT invoice) AS num_orders
        FROM    fact_sales
        GROUP BY date_key, product_key;
    """)
    agg_count = conn.execute("SELECT COUNT(*) FROM agg_daily_sales").fetchone()[0]
    log.info(f"  agg_daily_sales: {agg_count:,} rows materialised")

    log.info("ETL complete ✓")
    return conn


if __name__ == "__main__":
    from src.ingestion.data_loader import run_ingestion_pipeline
    df = run_ingestion_pipeline()
    run_etl(df, recreate=True)
