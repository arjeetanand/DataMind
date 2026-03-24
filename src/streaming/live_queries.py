import duckdb
import pandas as pd
import redis
import clickhouse_connect
import json
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config.settings import DB_PATH

# Infrastructure Connectors
REDIS_HOST = "localhost"
REDIS_PORT = 6379
CH_HOST = "localhost"
CH_PORT = 8123

def get_duckdb_conn(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Create a read-only connection to the DuckDB database.
    Used for retrieving stream status and forecast results."""
    return duckdb.connect(str(db_path), read_only=True)

def get_ch_client():
    """Initialize and return a ClickHouse connection client.
    Primary interface for high-speed analytical queries on live data."""
    return clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)

def get_redis_client():
    """Create a connection to the local Redis instance for fast KPI access.
    Returns a client configured for decoding responses to strings."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


def get_recent_transactions(n: int = 25, conn=None) -> pd.DataFrame:
    """Retrieve the 'n' most recent transactions from Redis with ClickHouse fallback.
    Provides the fastest possible data source for the live dashboard ticker."""
    r = get_redis_client()
    try:
        txns = r.lrange("live:transactions", 0, n-1)
        if txns:
            data = [json.loads(t) for t in txns]
            return pd.DataFrame(data)
    except Exception:
        pass
    
    # Fallback to ClickHouse
    client = get_ch_client()
    try:
        df = client.query_df(f"""
            SELECT
                invoice,
                stock_code,
                description,
                quantity,
                price,
                round(revenue, 2) AS revenue,
                customer_id,
                country,
                toString(simulated_day) AS simulated_day,
                toString(invoice_date) AS invoice_date
            FROM retail_events_hot
            ORDER BY ingested_at DESC
            LIMIT {n}
        """)
        return df
    finally:
        client.close()

def get_forecast_vs_actual(conn=None) -> pd.DataFrame:
    """Cross-reference DuckDB ML forecasts with ClickHouse real-time actuals.
    Calculates Absolute Percentage Error (APE) for each streamed day."""
    _conn = conn or get_duckdb_conn()
    
    # Get actuals from ClickHouse
    client = get_ch_client()
    try:
        actuals_df = client.query_df("""
            SELECT simulated_day, SUM(revenue) AS actual_revenue
            FROM retail_events_hot
            GROUP BY simulated_day
        """)
    finally:
        client.close()
    
    # Register actuals in DuckDB for joining
    _conn.register("ch_actuals", actuals_df)
    
    return _conn.execute("""
        SELECT
            CAST(f.simulated_day AS VARCHAR)  AS day,
            ROUND(f.predicted_revenue, 2)     AS predicted,
            ROUND(f.lower_ci, 2)              AS lower_ci,
            ROUND(f.upper_ci, 2)              AS upper_ci,
            ROUND(COALESCE(a.actual_revenue, 0), 2) AS actual,
            CASE
                WHEN a.actual_revenue IS NOT NULL AND a.actual_revenue > 0
                THEN ROUND(ABS(f.predicted_revenue - a.actual_revenue)
                     / a.actual_revenue * 100, 1)
                ELSE NULL
            END AS ape_pct
        FROM live_forecasts f
        LEFT JOIN ch_actuals a ON a.simulated_day = f.simulated_day
        ORDER BY f.simulated_day
    """).df()

def get_live_forecast_outlook(conn=None) -> pd.DataFrame:
    """Fetch the latest 7-day revenue projection from DuckDB.
    Returns the most recently generated forecast horizon and confidence intervals."""
    _conn = conn or get_duckdb_conn()
    return _conn.execute("""
        SELECT
            CAST(forecast_date AS VARCHAR) AS day,
            ROUND(predicted_revenue, 2) AS predicted,
            ROUND(lower_ci, 2) AS lower_ci,
            ROUND(upper_ci, 2) AS upper_ci
        FROM live_forecast_outlook
        WHERE generated_day = (SELECT MAX(generated_day) FROM live_forecast_outlook)
        ORDER BY forecast_date
    """).df()

def get_stream_status(conn=None) -> dict:
    """Merge Redis real-time status details with DuckDB historical metadata.
    Provides a comprehensive view of the stream's current health and progress."""
    r = get_redis_client()
    day = r.get("live:status:day")
    rows = r.get("live:status:rows")
    
    _conn = conn or get_duckdb_conn()
    try:
        row = _conn.execute("SELECT days_streamed, mape, started_at FROM live_stream_status WHERE id = 1").fetchone()
        if not row: return _empty_status()
        
        return {
            "current_day"  : day,
            "total_rows"   : int(rows) if rows else 0,
            "speed_mode"   : "normal", # Source of truth is control file anyway
            "is_running"   : True if day else False,
            "days_streamed": row[0],
            "mape"         : round(row[1], 1) if row[1] is not None else None,
            "started_at"   : str(row[2]),
        }
    except Exception:
        return _empty_status()

def _empty_status() -> dict:
    """Generate a default empty status dictionary for fallback scenarios.
    Ensures the API returns consistent keys even when the stream is reset."""
    return {"current_day": None, "total_rows": 0, "speed_mode": "normal", "is_running": False, "days_streamed": 0, "mape": None, "started_at": None}

def get_live_kpis(conn=None) -> dict:
    """Fetch global performance indicators (Revenue, Orders, TPS) from Redis.
    Optimized for sub-millisecond frontend polling loops."""
    r = get_redis_client()
    try:
        rev = r.get("live:kpi:revenue") or 0
        txns = r.get("live:kpi:orders") or 0
        units = r.get("live:kpi:units") or 0
        unique_cust = r.pfcount("live:unique_customers") or 0
        tps = r.get("live:tps") or 0
        
        return {
            "total_live_revenue": round(float(rev), 2),
            "total_txns"        : int(txns),
            "total_units"       : int(units),
            "unique_customers"  : int(unique_cust),
            "avg_txn_value"     : round(float(rev)/max(1, int(txns)), 2),
            "countries"         : 0,
            "tps"               : int(tps),
        }
    except Exception:
        return {"total_live_revenue": 0, "total_txns": 0, "unique_customers": 0, "avg_txn_value": 0, "countries": 0, "tps": 0}

def get_live_top_products(n: int = 5, conn=None) -> pd.DataFrame:
    """Query ClickHouse to find the highest revenue-generating products.
    Returns the top 'n' products based on real-time streaming data."""
    client = get_ch_client()
    try:
        return client.query_df(f"""
            SELECT
                stock_code,
                description,
                SUM(quantity)  AS total_units,
                round(SUM(revenue), 2) AS total_revenue,
                COUNT(*)       AS txn_count
            FROM retail_events_hot
            GROUP BY stock_code, description
            ORDER BY total_revenue DESC
            LIMIT {n}
        """)
    finally:
        client.close()

def get_live_geo_revenue(n: int = 10, conn=None) -> pd.DataFrame:
    """Aggregate revenue and unique customer counts by country in ClickHouse.
    Identifies the top 'n' geographic markets currently active in the stream."""
    client = get_ch_client()
    try:
        return client.query_df(f"""
            SELECT
                country,
                SUM(revenue)  AS total_revenue,
                COUNT(DISTINCT customer_id) AS unique_customers,
                COUNT(*)       AS txn_count
            FROM retail_events_hot
            GROUP BY country
            ORDER BY total_revenue DESC
            LIMIT {n}
        """)
    finally:
        client.close()
