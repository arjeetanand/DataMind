"""
DataMind — ClickHouse Setup Script
Connects to ClickHouse and creates tables for the hot-path.
"""

import time
import logging
import clickhouse_connect
from src.streaming.clickhouse_schema import (
    CREATE_HOT_TABLE_SQL,
    CREATE_KAFKA_QUEUE_SQL,
    CREATE_MV_SQL,
    DROP_ALL_SQL
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("datamind.clickhouse_setup")

CH_HOST = "localhost"
CH_PORT = 8123

def setup_clickhouse(reset=False):
    log.info(f"Connecting to ClickHouse at {CH_HOST}:{CH_PORT}...")
    
    # Retry loop for ClickHouse to start up
    client = None
    for i in range(10):
        try:
            client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)
            break
        except Exception as e:
            log.warning(f"Connection attempt {i+1} failed: {e}")
            time.sleep(5)
    
    if not client:
        log.error("Could not connect to ClickHouse. Is it running?")
        return

    try:
        if reset:
            log.info("Resetting ClickHouse tables...")
            for cmd in DROP_ALL_SQL.strip().split(";"):
                if cmd.strip():
                    client.command(cmd)

        log.info("Creating Hot Table...")
        client.command(CREATE_HOT_TABLE_SQL)

        log.info("Creating Kafka Queue (this might fail if Kafka is down)...")
        try:
            client.command(CREATE_KAFKA_QUEUE_SQL)
        except Exception as e:
            log.warning(f"Kafka Queue creation failed (expected if Kafka is initializing): {e}")

        log.info("Creating Materialized View...")
        client.command(CREATE_MV_SQL)

        log.info("ClickHouse Hot-Path Setup Complete!")
        
    except Exception as e:
        log.error(f"Setup failed: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Drop existing tables first")
    args = parser.parse_args()
    
    setup_clickhouse(reset=args.reset)
