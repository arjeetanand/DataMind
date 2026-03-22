"""
DataMind — Kafka Consumer (High Performance Async)
Subscribes to 'retail-events-v1', writes to DuckDB live tables,
and triggers demand forecasts in a BACKGROUND THREAD.

Start: python -m src.streaming.consumer
"""

import json
import time
import logging
import signal
import sys
import threading
import queue
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config.settings import DB_PATH
from src.streaming.live_schema import LIVE_SCHEMA_DDL

STREAM_CONTROL_FILE = Path(__file__).resolve().parents[2] / "data" / "stream_control.json"

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "retail-events-v1"
GROUP_ID = "datamind-consumer-hp"
BATCH_SIZE = 500  # Larger batches for higher throughput
BATCH_TIMEOUT = 1.0 # Flush every second if not full
FORECAST_THROTTLE_SEC = 2.0 # Allow outlook updates every 2s

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("datamind.consumer")

def _read_stream_control() -> dict:
    try:
        if STREAM_CONTROL_FILE.exists():
            return json.loads(STREAM_CONTROL_FILE.read_text())
    except Exception:
        pass
    return {"is_running": False, "speed_mode": "normal"}

def _connect_duckdb_with_retry(path: Path, read_only: bool = False):
    import time
    max_retries = 60
    for i in range(max_retries):
        try:
            return duckdb.connect(str(path), read_only=read_only)
        except Exception as e:
            err_msg = str(e).lower()
            if i < max_retries - 1 and ("locked" in err_msg or "already open" in err_msg or "used by another process" in err_msg):
                time.sleep(0.3)
                continue
            raise

def _run_forecast_logic(day: str):
    """Heavy AI logic, intended for background thread."""
    try:
        from src.ml.forecaster import predict, MODEL_PATH, SCALER_PATH
        from src.warehouse.queries import daily_sales_series

        if not MODEL_PATH.exists() or not SCALER_PATH.exists():
            return None

        with _connect_duckdb_with_retry(DB_PATH, read_only=False) as h_conn:
            df_hist_base = daily_sales_series(conn=h_conn)
            df_live = h_conn.execute("""
                SELECT  simulated_day                AS ds,
                        SUM(revenue)                AS y,
                        SUM(quantity)               AS units,
                        COUNT(DISTINCT invoice)    AS orders,
                        EXTRACT(DOW FROM simulated_day) AS day_of_week,
                        EXTRACT(MONTH FROM simulated_day) AS month
                FROM live_sales
                GROUP BY simulated_day
                ORDER BY simulated_day
            """).df()

            if not df_live.empty:
                df_hist = pd.concat([df_hist_base, df_live]).drop_duplicates(subset=["ds"], keep="last")
                df_hist = df_hist.sort_values("ds")
            else:
                df_hist = df_hist_base

        if len(df_hist) < 2: return None
        
        df_hist = df_hist.set_index("ds")
        result = predict(df_hist)
        return {
            "dates": result["dates"],
            "forecast": result["forecast"],
            "lower_ci": result["lower_ci"],
            "upper_ci": result["upper_ci"],
        }
    except Exception as e:
        log.warning(f"Background forecast failed: {e}")
        return None

class LiveWriter:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        with self._conn_with_retry() as conn:
            conn.execute(LIVE_SCHEMA_DDL)
            self._row_id = self._get_max_id(conn) + 1
            self._total_rows = self._get_row_count(conn)
        
        self._days_done = 0
        self._current_day = None
        self._mape_sum = 0.0
        self._lock = threading.Lock()
        log.info(f"High-Speed LiveWriter ready | rows={self._total_rows:,}")

    def _get_max_id(self, conn) -> int:
        r = conn.execute("SELECT COALESCE(MAX(id), 0) FROM live_sales").fetchone()
        return r[0] if r else 0

    def _get_row_count(self, conn) -> int:
        r = conn.execute("SELECT COUNT(*) FROM live_sales").fetchone()
        return r[0] if r else 0

    def _conn_with_retry(self, read_only=False):
        return _connect_duckdb_with_retry(self.db_path, read_only=read_only)

    def write_batch(self, batch: list):
        if not batch: return
        with self._lock:
            df = pd.DataFrame(batch)
            df["simulated_day"] = pd.to_datetime(df["simulated_day"]).dt.date
            df["invoice_date"] = pd.to_datetime(df["invoice_date"])
            df["id"] = range(self._row_id, self._row_id + len(df))
            self._row_id += len(df)
            self._total_rows += len(df)
            
            # Map columns explicitly to match live_sales table schema
            # Ensure we only insert columns that exist in the target table
            with self._conn_with_retry() as conn:
                conn.execute("""
                    INSERT INTO live_sales
                        (id, invoice, stock_code, description, quantity, price, revenue,
                         customer_id, country, invoice_date, simulated_day)
                    SELECT id, invoice, stock_code, description, quantity, price, revenue,
                           customer_id, country, invoice_date, simulated_day
                    FROM df
                """)

    def update_status(self, day: str):
        with self._lock:
            self._current_day = day
            ctrl = _read_stream_control()
            is_running = ctrl.get("is_running", False)
            speed_mode = ctrl.get("speed_mode", "normal")
            try:
                with self._conn_with_retry() as conn:
                    conn.execute("""
                        INSERT OR REPLACE INTO live_stream_status
                            (id, current_day, total_rows, is_running, speed_mode, days_streamed, mape, started_at)
                        VALUES (1, ?, ?, ?, ?, ?, ?, COALESCE((SELECT started_at FROM live_stream_status WHERE id=1), CURRENT_TIMESTAMP))
                    """, [day, self._total_rows, is_running, speed_mode, self._days_done, round(self._mape_sum/max(1,self._days_done), 2)])
            except Exception as e:
                log.warning(f"Status update failed: {e}")

    def save_forecast(self, day: str, forecast: dict):
        with self._lock:
            try:
                with self._conn_with_retry() as conn:
                    # History
                    conn.execute("INSERT OR REPLACE INTO live_forecasts VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)", [
                        day, forecast["forecast"][0], forecast["lower_ci"][0], forecast["upper_ci"][0]
                    ])
                    # Outlook
                    for i in range(len(forecast["forecast"])):
                        f_date = forecast["dates"][i] if i < len(forecast["dates"]) else None
                        if not f_date: continue
                        conn.execute("INSERT OR REPLACE INTO live_forecast_outlook VALUES (?, ?, ?, ?, ?)", 
                                     [day, f_date, forecast["forecast"][i], forecast["lower_ci"][i], forecast["upper_ci"][i]])
                log.info(f"Cloud update: Forecast for {day} persisted.")
            except Exception as e:
                log.warning(f"Forecast save failed: {e}")

    def mark_stopped(self):
        with self._lock:
            try:
                with self._conn_with_retry() as conn:
                    conn.execute("UPDATE live_stream_status SET is_running = false WHERE id = 1")
            except Exception:
                pass

class RetailConsumer:
    def __init__(self):
        from kafka import KafkaConsumer as _KC
        self._consumer = _KC(
            TOPIC, bootstrap_servers=[KAFKA_BOOTSTRAP], group_id=GROUP_ID,
            auto_offset_reset="earliest", enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_records=500
        )
        self._writer = LiveWriter()
        self._running = True
        self._batch = []
        self._last_flush = time.time()
        self._forecast_queue = queue.Queue(maxsize=1) # Only keep one pending forecast task
        
        # Start background forecast worker
        self._worker = threading.Thread(target=self._forecast_worker, daemon=True)
        self._worker.start()

        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_):
        self._running = False

    def _forecast_worker(self):
        last_run = 0
        while self._running:
            try:
                day = self._forecast_queue.get(timeout=1.0)
                # Hard throttle to avoid DB thrashing
                if time.time() - last_run < FORECAST_THROTTLE_SEC:
                    continue
                
                log.info(f"Worker Trigger: Forecasting for {day}...")
                res = _run_forecast_logic(day)
                if res:
                    self._writer.save_forecast(day, res)
                    last_run = time.time()
                
                self._forecast_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                log.error(f"Worker Error: {e}")

    def run(self):
        log.info(f"HP-Consumer active on {TOPIC}...")
        seen_days = set()
        try:
            while self._running:
                # Check if simulation is paused (Global)
                ctrl = _read_stream_control()
                if not ctrl.get("is_running", False):
                    time.sleep(1.0)
                    continue

                records = self._consumer.poll(timeout_ms=500)
                for _tp, messages in records.items():
                    for msg in messages:
                        event = msg.value
                        self._batch.append(event)
                        day = event.get("simulated_day")
                        
                        if event.get("is_day_start") and day not in seen_days:
                            seen_days.add(day)
                            # Update status immediately (lightweight)
                            self._writer.update_status(day)
                            # Queue forecast (async)
                            try:
                                self._forecast_queue.put_nowait(day)
                            except queue.Full:
                                pass # Skip if already busy

                        if len(self._batch) >= BATCH_SIZE:
                            self._writer.write_batch(self._batch)
                            self._batch = []
                            self._last_flush = time.time()

                if time.time() - self._last_flush > BATCH_TIMEOUT:
                    if self._batch:
                        self._writer.write_batch(self._batch)
                        self._batch = []
                    self._last_flush = time.time()
        finally:
            self._writer.mark_stopped()
            log.info("Consumer shutdown.")

if __name__ == "__main__":
    RetailConsumer().run()
