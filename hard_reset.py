import duckdb
from pathlib import Path
import sys

# Define root and db path clearly
DB_PATH = r"c:\Users\Arjeet\Downloads\datamind\datamind\data\datamind.duckdb"

SCHEMA = [
    "CREATE TABLE IF NOT EXISTS live_sales (transaction_id VARCHAR, transaction_time TIMESTAMP, product_id VARCHAR, product_name VARCHAR, category VARCHAR, quantity INTEGER, price DOUBLE, total_revenue DOUBLE, store_location VARCHAR, payment_method VARCHAR)",
    "CREATE TABLE IF NOT EXISTS live_inventory (product_id VARCHAR, product_name VARCHAR, current_stock INTEGER, last_updated TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS live_stream_status (is_running BOOLEAN, speed_mode VARCHAR, current_day INTEGER, total_rows INTEGER)",
    "CREATE TABLE IF NOT EXISTS live_forecasts (prediction_id VARCHAR, window_start TIMESTAMP, window_end TIMESTAMP, predicted_revenue DOUBLE, actual_revenue DOUBLE, margin_error DOUBLE)"
]

def main():
    print(f"Connecting to {DB_PATH}...")
    try:
        conn = duckdb.connect(DB_PATH)
        print("Connected. Dropping tables...")
        tables = ["live_sales", "live_inventory", "live_stream_status", "live_forecasts"]
        for t in tables:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {t}")
                print(f"Dropped {t}")
            except Exception as e:
                print(f"Error dropping {t}: {e}")
        
        print("Re-creating schema...")
        for ddl in SCHEMA:
            conn.execute(ddl)
            print("Executed DDL")
        
        print("Initializing status...")
        conn.execute("""
            INSERT INTO live_stream_status (is_running, speed_mode, current_day, total_rows)
            VALUES (false, 'normal', 0, 0)
        """)
        
        conn.close()
        print("Reset compete.")
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
