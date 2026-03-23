"""
DataMind — Kafka Producer
Streams Online Retail II CSV to Kafka topic 'retail-events'.

Speed modes (set via data/speed_control.json or CLI arg):
  normal  — 10s sleep between days  (good for live demos)
  fast    — 1s sleep between days   (quick iteration)
  burst   — no sleep                (max throughput for forecast overlay)

Start: python -m src.streaming.producer --speed normal
"""

import json
import time
import logging
import signal
import sys
import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config.settings import KAGGLE_CSV, DB_PATH
import duckdb

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "retail-events-v1"
STREAM_CONTROL_FILE = Path(__file__).resolve().parents[2] / "data" / "stream_control.json"

SPEED_DELAYS = {"normal": 10.0, "fast": 1.0, "burst": 0.0}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("datamind.producer")


def _read_stream_control() -> dict:
    """Read the simulation control JSON file from disk.
    Returns the parsed configuration or default 'stopped' state."""
    try:
        if STREAM_CONTROL_FILE.exists():
            return json.loads(STREAM_CONTROL_FILE.read_text())
    except Exception:
        pass
    return {"is_running": False, "speed_mode": "normal"}


def _write_stream_control(is_running: bool, speed: str) -> None:
    """Atomic write to the simulation control file.
    Updates running state and speed mode for both producer and consumer."""
    STREAM_CONTROL_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STREAM_CONTROL_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({
        "is_running": is_running,
        "speed_mode": speed,
        "updated_at": datetime.now().isoformat()
    }))
    tmp.replace(STREAM_CONTROL_FILE)


def _load_and_clean(csv_path: Path) -> pd.DataFrame:
    """Load the raw retail dataset and perform initial cleaning.
    Removes cancellations, null IDs, and filters for positive revenue rows."""
    log.info(f"Loading {csv_path} ...")
    df = pd.read_csv(
        csv_path,
        dtype={"Customer ID": "str", "StockCode": "str", "Invoice": "str"},
        parse_dates=["InvoiceDate"],
        encoding="utf-8",
        on_bad_lines="skip",
    )
    df.rename(columns={"Customer ID": "CustomerID"}, inplace=True)
    df = df[~df["Invoice"].str.startswith("C", na=False)]
    df = df.dropna(subset=["CustomerID", "Description", "InvoiceDate"])
    df = df[(df["Quantity"] > 0) & (df["Price"] > 0)]
    df["simulated_day"] = df["InvoiceDate"].dt.date.astype(str)
    df = df.sort_values("InvoiceDate").reset_index(drop=True)
    log.info(f"Cleaned dataset: {len(df):,} rows across {df['simulated_day'].nunique()} days")
    return df


import asyncio
from aiokafka import AIOKafkaProducer

class RetailProducer:
    def __init__(self, initial_speed: str = "normal"):
        """Initialize the Kafka producer and setup signal handlers.
        Ensures the simulation control begins in a stopped state."""
        self._producer = None
        self._running = True
        _write_stream_control(False, initial_speed)
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_):
        """Signal handler to gracefully stop the streaming loop.
        Ensures the producer is flushed and stopped before exit."""
        log.info("Shutdown signal received — draining producer...")
        self._running = False

    async def stream(self, csv_path: Path = KAGGLE_CSV):
        """Main streaming loop that groups data by day and emits Kafka events.
        Respects speed delays and simulation control pauses between days."""
        df = _load_and_clean(csv_path)
        grouped = df.groupby("simulated_day")
        all_days = sorted(grouped.groups.keys())
        total_rows = 0

        log.info(f"Starting stream (Async) | days={len(all_days)} | topic={TOPIC}")
        self._producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
        )
        await self._producer.start()

        try:
            for day_idx, day in enumerate(all_days):
                # Check if simulation is paused in Control File
                while self._running:
                    ctrl = _read_stream_control()
                    if ctrl.get("is_running", False):
                        break
                    await asyncio.sleep(1.0)

                if not self._running:
                    break

                day_df = grouped.get_group(day)
                records = day_df.to_dict(orient="records")
                n = len(records)

                for i, row in enumerate(records):
                    if not self._running:
                        break

                    event = {
                        "invoice": str(row.get("Invoice", "")),
                        "stock_code": str(row.get("StockCode", "")),
                        "description": str(row.get("Description", ""))[:120],
                        "quantity": float(row.get("Quantity", 0)),
                        "price": float(row.get("Price", 0)),
                        "revenue": round(float(row.get("Quantity", 0)) * float(row.get("Price", 0)), 2),
                        "customer_id": str(row.get("CustomerID", "")),
                        "country": str(row.get("Country", "")),
                        "invoice_date": str(row.get("InvoiceDate", "")),
                        "simulated_day": str(day),
                        "day_index": day_idx,
                        "is_day_start": i == 0,
                        "is_day_end": i == n - 1,
                        "row_in_day": i,
                        "total_in_day": n,
                    }
                    await self._producer.send(TOPIC, event)
                    total_rows += 1

                await self._producer.flush()

                ctrl = _read_stream_control()
                speed_mode = ctrl.get("speed_mode", "normal")
                delay = SPEED_DELAYS.get(speed_mode, 10.0)
                day_revenue = day_df["Quantity"].mul(day_df["Price"]).sum()

                log.info(
                    f"[{day_idx+1:4d}/{len(all_days)}] {day} | "
                    f"txns={n:4d} | rev=£{day_revenue:,.0f} | "
                    f"total_rows={total_rows:,} | speed={speed_mode} | delay={delay}s"
                )

                if delay > 0 and self._running:
                    # Sleep in increments to allow for quick shutdown
                    for _ in range(int(delay * 10)):
                        if not self._running:
                            break
                        await asyncio.sleep(0.1)
        finally:
            await self._producer.stop()
            log.info(f"Stream complete. Total rows published: {total_rows:,}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataMind Kafka Producer")
    parser.add_argument("--speed", choices=["normal", "fast", "burst"], default="normal",
                        help="Streaming speed mode")
    parser.add_argument("--csv", type=str, default=None, help="Path to CSV (overrides settings)")
    args = parser.parse_args()

    csv_path = Path(args.csv) if args.csv else KAGGLE_CSV

    if not csv_path.exists():
        log.error(f"CSV not found: {csv_path}")
        sys.exit(1)

    producer = RetailProducer(initial_speed=args.speed)
    try:
        asyncio.run(producer.stream(csv_path))
    except KeyboardInterrupt:
        pass
