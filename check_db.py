import duckdb
import time
from pathlib import Path

DB_PATH = Path(r'c:\Users\Arjeet\Downloads\datamind\datamind\data\datamind.duckdb')

def check():
    conn = None
    for i in range(20):
        try:
            conn = duckdb.connect(str(DB_PATH), read_only=True)
            print("Connected to DB.")
            break
        except Exception as e:
            print(f"Connection attempt {i+1} failed: {e}")
            time.sleep(0.5)
    
    if conn:
        sales = conn.execute("SELECT count(*) FROM live_sales").fetchone()[0]
        status = conn.execute("SELECT * FROM live_stream_status").fetchone()
        print(f"Total Sales: {sales}")
        print(f"Stream Status: {status}")
        conn.close()
    else:
        print("Could not connect to database after retries.")

if __name__ == "__main__":
    check()
