import duckdb
import os
import sys
from pathlib import Path

# Add project root to sys.path
root = Path(__file__).resolve().parents[1]
sys.path.append(str(root))

from src.streaming.live_schema import LIVE_SCHEMA_DDL, DROP_LIVE_DDL
from config.settings import DB_PATH

def wipe():
    print(f"Connecting to {DB_PATH}...")
    try:
        conn = duckdb.connect(str(DB_PATH))
        print("Dropping live tables...")
        conn.execute(DROP_LIVE_DDL)
        print("Recreating live tables...")
        conn.execute(LIVE_SCHEMA_DDL)
        # Initialize status row
        conn.execute("INSERT INTO live_stream_status (id, speed_mode, is_running) VALUES (1, 'normal', false)")
        conn.close()
        print("DuckDB Wipe Successful.")
    except Exception as e:
        print(f"DuckDB Wipe Failed: {e}")

    print("\nAttempting to clear Kafka topic 'retail-events'...")
    # We use docker-compose to delete and recreate the topic
    try:
        os.system("docker-compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic retail-events")
        os.system("docker-compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic retail-events --partitions 1 --replication-factor 1")
        print("Kafka Topic Reset Successful.")
    except Exception as e:
        print(f"Kafka Reset Failed: {e}")

if __name__ == "__main__":
    wipe()
