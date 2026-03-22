"""
DataMind — ClickHouse Schema
Hot-path storage for real-time events.
"""

# The main table for querying
# Using MergeTree engine for high-performance OLAP
CREATE_HOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS retail_events_hot (
    invoice      String,
    stock_code   String,
    description  String,
    quantity     Float64,
    price        Float64,
    revenue      Float64,
    customer_id  String,
    country      String,
    invoice_date DateTime,
    simulated_day Date,
    ingested_at  DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (simulated_day, invoice_date, invoice);
"""

# The Kafka Engine table (Queue)
# This table doesn't store data; it polls Kafka
CREATE_KAFKA_QUEUE_SQL = """
CREATE TABLE IF NOT EXISTS kafka_queue (
    invoice      String,
    stock_code   String,
    description  String,
    quantity     Float64,
    price        Float64,
    revenue      Float64,
    customer_id  String,
    country      String,
    invoice_date String,
    simulated_day String
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'retail-events-v1',
    kafka_group_name = 'clickhouse-ingestor',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;
"""

# The Materialized View
# Automatically moves data from the queue to the hot table
CREATE_MV_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_mv TO retail_events_hot AS
SELECT
    invoice,
    stock_code,
    description,
    quantity,
    price,
    revenue,
    customer_id,
    country,
    parseDateTimeBestEffort(invoice_date) AS invoice_date,
    toDate(simulated_day) AS simulated_day,
    now() AS ingested_at
FROM kafka_queue;
"""

DROP_ALL_SQL = """
DROP TABLE IF EXISTS kafka_mv;
DROP TABLE IF EXISTS kafka_queue;
DROP TABLE IF EXISTS retail_events_hot;
"""
