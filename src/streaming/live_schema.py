"""
DataMind — Live Streaming Schema
DuckDB tables for Kafka-fed live data.
Completely separate from warehouse tables — safe to reset anytime.
"""

LIVE_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS live_sales (
    id          INTEGER,
    invoice     VARCHAR(20),
    stock_code  VARCHAR(20),
    description TEXT,
    quantity    FLOAT,
    price       FLOAT,
    revenue     FLOAT,
    customer_id VARCHAR(20),
    country     VARCHAR(50),
    invoice_date TIMESTAMP,
    simulated_day DATE,
    ingested_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS live_forecasts (
    simulated_day    DATE PRIMARY KEY,
    predicted_revenue FLOAT,
    lower_ci         FLOAT,
    upper_ci         FLOAT,
    generated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS live_stream_status (
    id           INTEGER PRIMARY KEY,
    current_day  DATE,
    total_rows   INTEGER DEFAULT 0,
    speed_mode   VARCHAR(10) DEFAULT 'normal',
    is_running   BOOLEAN DEFAULT false,
    started_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    days_streamed INTEGER DEFAULT 0,
    mape         FLOAT DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS live_forecast_outlook (
    generated_day    DATE,
    forecast_date    DATE,
    predicted_revenue FLOAT,
    lower_ci         FLOAT,
    upper_ci         FLOAT,
    PRIMARY KEY (generated_day, forecast_date)
);
"""

DELETE_LIVE_DATA_SQL = """
DELETE FROM live_sales;
DELETE FROM live_forecasts;
DELETE FROM live_forecast_outlook;
DELETE FROM live_stream_status;
"""
