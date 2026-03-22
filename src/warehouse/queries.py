"""
DataMind — Advanced SQL Query Library
Production-grade analytical queries over the DuckDB star schema.
All queries use window functions, CTEs, and dimensional joins.
"""

import duckdb
import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))
from config.settings import DB_PATH


def get_conn(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path), read_only=True)


# ── 1. Revenue Trend (MoM with growth %) ─────────────────────────────────────
def monthly_revenue_trend(conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
    return _conn.execute("""
        WITH monthly AS (
            SELECT  d.year,
                    d.month,
                    d.month_name,
                    SUM(f.revenue)          AS revenue,
                    COUNT(DISTINCT f.invoice) AS orders
            FROM    fact_sales f
            JOIN    dim_date d   ON f.date_key = d.date_key
            GROUP BY d.year, d.month, d.month_name
        )
        SELECT  *,
                LAG(revenue) OVER (ORDER BY year, month)  AS prev_revenue,
                ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY year, month))
                      / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0), 2)
                      AS mom_growth_pct
        FROM    monthly
        ORDER BY year, month
    """).df()


# ── 2. Top-N Products by Revenue ──────────────────────────────────────────────
def top_products(n: int = 20, conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
    return _conn.execute(f"""
        SELECT  p.stock_code,
                p.description,
                p.price_band,
                SUM(f.quantity)  AS total_units,
                SUM(f.revenue)   AS total_revenue,
                COUNT(DISTINCT f.invoice) AS order_count,
                RANK() OVER (ORDER BY SUM(f.revenue) DESC) AS revenue_rank
        FROM    fact_sales f
        JOIN    dim_product p ON f.product_key = p.product_key
        GROUP BY p.stock_code, p.description, p.price_band
        ORDER BY total_revenue DESC
        LIMIT   {n}
    """).df()


# ── 3. Customer RFM Summary ───────────────────────────────────────────────────
def customer_rfm_summary(conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
    return _conn.execute("""
        SELECT  c.customer_segment,
                COUNT(*)            AS num_customers,
                ROUND(AVG(f.revenue), 2) AS avg_order_value,
                SUM(f.revenue)      AS total_revenue,
                ROUND(100.0 * SUM(f.revenue) /
                      SUM(SUM(f.revenue)) OVER (), 2) AS revenue_share_pct
        FROM    fact_sales f
        JOIN    dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.customer_segment
        ORDER BY total_revenue DESC
    """).df()


# ── 4. Geographic Revenue Breakdown ───────────────────────────────────────────
def geo_revenue(conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
    return _conn.execute("""
        SELECT  g.region,
                g.country,
                SUM(f.revenue)          AS total_revenue,
                COUNT(DISTINCT f.customer_key) AS unique_customers,
                RANK() OVER (PARTITION BY g.region ORDER BY SUM(f.revenue) DESC)
                    AS rank_in_region
        FROM    fact_sales f
        JOIN    dim_geography g ON f.geo_key = g.geo_key
        GROUP BY g.region, g.country
        ORDER BY total_revenue DESC
    """).df()


# ── 5. Daily Sales Series (for PyTorch forecasting) ──────────────────────────
def daily_sales_series(stock_code: str = None, conn=None) -> pd.DataFrame:
    """Returns ordered daily revenue — feed directly into LSTM."""
    _conn = conn or get_conn()
    where = f"AND p.stock_code = '{stock_code}'" if stock_code else ""
    return _conn.execute(f"""
        SELECT  d.full_date                 AS ds,
                SUM(a.total_revenue)        AS y,
                SUM(a.total_quantity)       AS units,
                SUM(a.num_orders)           AS orders,
                EXTRACT(DOW FROM d.full_date) AS day_of_week,
                EXTRACT(MONTH FROM d.full_date) AS month
        FROM    agg_daily_sales a
        JOIN    dim_date d        ON a.date_key    = d.date_key
        JOIN    dim_product p     ON a.product_key = p.product_key
        WHERE   1=1 {where}
        GROUP BY d.full_date
        ORDER BY d.full_date
    """).df()


# ── 6. Reorder Signals — products trending down ───────────────────────────────
def reorder_signals(lookback_days: int = 30, conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
    return _conn.execute(f"""
        WITH recent AS (
            SELECT  a.product_key,
                    SUM(a.total_quantity)  AS recent_units,
                    SUM(a.total_revenue)   AS recent_revenue
            FROM    agg_daily_sales a
            JOIN    dim_date d ON a.date_key = d.date_key
            WHERE   d.full_date >= (SELECT MAX(full_date) - INTERVAL '{lookback_days} days'
                                    FROM dim_date)
            GROUP BY a.product_key
        ),
        prior AS (
            SELECT  a.product_key,
                    SUM(a.total_quantity)  AS prior_units
            FROM    agg_daily_sales a
            JOIN    dim_date d ON a.date_key = d.date_key
            WHERE   d.full_date < (SELECT MAX(full_date) - INTERVAL '{lookback_days} days'
                                   FROM dim_date)
              AND   d.full_date >= (SELECT MAX(full_date) - INTERVAL '{lookback_days * 2} days'
                                    FROM dim_date)
            GROUP BY a.product_key
        )
        SELECT  p.stock_code,
                p.description,
                r.recent_units,
                pr.prior_units,
                ROUND(100.0 * (r.recent_units - pr.prior_units)
                      / NULLIF(pr.prior_units, 0), 2) AS unit_change_pct,
                CASE WHEN r.recent_units < pr.prior_units * 0.7
                     THEN 'REORDER' ELSE 'OK' END      AS signal
        FROM    recent r
        JOIN    prior pr  ON r.product_key = pr.product_key
        JOIN    dim_product p ON r.product_key = p.product_key
        WHERE   r.recent_units < pr.prior_units * 0.7
        ORDER BY unit_change_pct ASC
        LIMIT   50
    """).df()


# ── 7. Cohort Retention ────────────────────────────────────────────────────────
def cohort_retention(conn=None) -> pd.DataFrame:
    _conn = conn or get_conn()
    return _conn.execute("""
        WITH cohorts AS (
            SELECT  customer_key,
                    DATE_TRUNC('month', MIN(invoice_date)) AS cohort_month
            FROM    fact_sales
            GROUP BY customer_key
        ),
        activity AS (
            SELECT  f.customer_key,
                    DATE_TRUNC('month', f.invoice_date) AS activity_month,
                    c.cohort_month,
                    DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', f.invoice_date))
                        AS month_number
            FROM    fact_sales f
            JOIN    cohorts c ON f.customer_key = c.customer_key
        )
        SELECT  cohort_month,
                month_number,
                COUNT(DISTINCT customer_key) AS active_customers
        FROM    activity
        GROUP BY cohort_month, month_number
        ORDER BY cohort_month, month_number
    """).df()


if __name__ == "__main__":
    print(monthly_revenue_trend().head())
    print(top_products(5))
