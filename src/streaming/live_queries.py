"""
DataMind — Live Query Library
SQL queries over live_sales, live_forecasts, live_stream_status.
Mirrors src/warehouse/queries.py but targets streaming data.
"""

import duckdb
import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))
from config.settings import DB_PATH


def get_conn(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path), read_only=True)


def get_rolling_revenue(window_txns: int = 500, conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
    return _conn.execute(f"""
        WITH recent AS (
            SELECT *
            FROM live_sales
            ORDER BY ingested_at DESC
            LIMIT {window_txns}
        )
        SELECT
            simulated_day,
            SUM(revenue)       AS daily_revenue,
            COUNT(*)           AS txn_count,
            SUM(SUM(revenue)) OVER (ORDER BY simulated_day) AS cumulative_revenue
        FROM recent
        GROUP BY simulated_day
        ORDER BY simulated_day
    """).df()


def get_recent_transactions(n: int = 25, conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
    return _conn.execute(f"""
        SELECT
            id,
            invoice,
            stock_code,
            description,
            quantity,
            price,
            ROUND(revenue, 2)   AS revenue,
            customer_id,
            country,
            CAST(simulated_day AS VARCHAR) AS simulated_day,
            CAST(ingested_at AS VARCHAR)   AS ingested_at
        FROM live_sales
        ORDER BY id DESC
        LIMIT {n}
    """).df()


def get_forecast_vs_actual(conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
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
        LEFT JOIN (
            SELECT simulated_day, SUM(revenue) AS actual_revenue
            FROM   live_sales
            GROUP  BY simulated_day
        ) a ON a.simulated_day = f.simulated_day
        ORDER BY f.simulated_day
    """).df()


def get_live_forecast_outlook(conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
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
    _conn = conn or get_conn()
    try:
        row = _conn.execute("""
            SELECT
                CAST(current_day AS VARCHAR) AS current_day,
                total_rows,
                speed_mode,
                is_running,
                days_streamed,
                ROUND(mape, 2) AS mape,
                CAST(started_at AS VARCHAR) AS started_at
            FROM live_stream_status WHERE id = 1
        """).fetchone()

        if not row:
            return _empty_status()

        return {
            "current_day"  : row[0],
            "total_rows"   : row[1],
            "speed_mode"   : row[2],
            "is_running"   : row[3],
            "days_streamed": row[4],
            "mape"         : round(row[5], 1) if row[5] is not None else None,
            "started_at"   : row[6],
        }
    except Exception:
        return _empty_status()


def _empty_status() -> dict:
    return {
        "current_day"  : None,
        "total_rows"   : 0,
        "speed_mode"   : "normal",
        "is_running"   : False,
        "days_streamed": 0,
        "mape"         : None,
        "started_at"   : None,
    }


def get_live_kpis(conn=None) -> dict:
    _conn = conn or get_conn()
    try:
        row = _conn.execute("""
            SELECT
                ROUND(SUM(revenue), 2)            AS total_live_revenue,
                COUNT(*)                           AS total_txns,
                COUNT(DISTINCT simulated_day)      AS days_complete,
                COUNT(DISTINCT customer_id)        AS unique_customers,
                ROUND(AVG(revenue), 2)             AS avg_txn_value,
                COUNT(DISTINCT country)            AS countries
            FROM live_sales
        """).fetchone()

        return {
            "total_live_revenue": row[0] or 0,
            "total_txns"        : row[1] or 0,
            "days_complete"     : row[2] or 0,
            "unique_customers"  : row[3] or 0,
            "avg_txn_value"     : row[4] or 0,
            "countries"         : row[5] or 0,
        }
    except Exception:
        return {
            "total_live_revenue": 0, "total_txns": 0, "days_complete": 0,
            "unique_customers": 0, "avg_txn_value": 0, "countries": 0,
        }


def get_live_top_products(n: int = 5, conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
    return _conn.execute(f"""
        SELECT
            stock_code,
            description,
            SUM(quantity)  AS total_units,
            ROUND(SUM(revenue), 2) AS total_revenue,
            COUNT(*)       AS txn_count
        FROM live_sales
        GROUP BY stock_code, description
        ORDER BY total_revenue DESC
        LIMIT {n}
    """).df()


def get_live_geo_revenue(n: int = 10, conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
    return _conn.execute(f"""
        SELECT
            country,
            SUM(revenue)  AS total_revenue,
            COUNT(DISTINCT customer_id) AS unique_customers,
            COUNT(*)       AS txn_count
        FROM live_sales
        GROUP BY country
        ORDER BY total_revenue DESC
        LIMIT {n}
    """).df()
