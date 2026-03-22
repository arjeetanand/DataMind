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
import redis
import clickhouse_connect
import asyncio
import duckdb
import pandas as pd
from aiokafka import AIOKafkaConsumer
from pathlib import Path

# DataMind internal imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from config.settings import DB_PATH
from src.streaming.live_schema import LIVE_SCHEMA_DDL
from src.ml.forecaster import predict, MODEL_PATH, SCALER_PATH
from src.warehouse.queries import daily_sales_series

STREAM_CONTROL_FILE = Path(__file__).resolve().parents[2] / "data" / "stream_control.json"

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "retail-events-v1"
GROUP_ID = "datamind-consumer-hp"
BATCH_SIZE = 1000 
BATCH_TIMEOUT = 0.5
FORECAST_THROTTLE_SEC = 1.0 

# Infrastructure Connectors
REDIS_HOST = "localhost"
REDIS_PORT = 6379
CH_HOST = "localhost"
CH_PORT = 8123

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("datamind.consumer")

# Redis Client for Hot KPI
try:
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    r_client.ping()
    log.info("Redis connected successfully.")
except Exception as e:
    log.warning(f"Redis connection failed: {e}")
    r_client = None

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
        import pandas as pd
        from src.ml.forecaster import predict, MODEL_PATH, SCALER_PATH
        from src.warehouse.queries import daily_sales_series

        if not MODEL_PATH.exists() or not SCALER_PATH.exists():
            return None

        with _connect_duckdb_with_retry(DB_PATH, read_only=False) as h_conn:
            df_hist_base = daily_sales_series(conn=h_conn)
            
            # Fetch 'Live' data from ClickHouse instead of DuckDB
            client = clickhouse_connect.get_client(host="localhost", port=8123)
            try:
                df_live = client.query_df("""
                    SELECT  simulated_day                AS ds,
                            SUM(revenue)                AS y,
                            SUM(quantity)               AS units,
                            COUNT(DISTINCT invoice)    AS orders,
                            toDayOfWeek(simulated_day)  AS day_of_week,
                            toMonth(simulated_day)      AS month
                    FROM retail_events_hot
                    GROUP BY simulated_day
                    ORDER BY simulated_day
                """)
            finally:
                client.close()

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
        
        # Real-time TPS tracking
        self._last_total_rows = self._total_rows
        self._tps = 0
        self._tps_thread = threading.Thread(target=self._tps_monitor, daemon=True)
        self._tps_thread.start()
        
        log.info(f"High-Speed LiveWriter ready | rows={self._total_rows:,}")

    def _tps_monitor(self):
        """Calculate throughput delta every second."""
        while True:
            time.sleep(1.0)
            with self._lock:
                delta = self._total_rows - self._last_total_rows
                self._last_total_rows = self._total_rows
                self._tps = delta
            if r_client:
                r_client.set("live:tps", self._tps)

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
        
        # 1. Update Redis (Hot KPI Layer)
        if r_client:
            p = r_client.pipeline()
            for event in batch:
                rev = float(event.get("revenue", 0))
                p.incrbyfloat("live:kpi:revenue", rev)
                p.incr("live:kpi:orders")
                p.incrby("live:kpi:units", int(event.get("quantity", 0)))
                # Cache last 50 transactions
                txn_data = json.dumps({
                    "invoice": event.get("invoice"),
                    "description": event.get("description"),
                    "revenue": rev,
                    "country": event.get("country"),
                    "time": event.get("invoice_date")
                })
                p.lpush("live:transactions", txn_data)
            
            # Trim only once per batch (instead of inside the loop!)
            p.ltrim("live:transactions", 0, 49)
            p.execute()

        # 2. Update counter
        with self._lock:
            self._total_rows += len(batch)
            
        # [NOTE] DuckDB write removed here. 
        # ClickHouse handles 'retail_events_hot' via native Kafka Engine.
        # This reduces local disk IO and lock contention.

    def update_status(self, day: str):
        if r_client:
            r_client.set("live:status:day", day)
            r_client.set("live:status:rows", self._total_rows)

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

    def finalize_day(self, day: str):
        """Calculate MAPE for a completed day by comparing ClickHouse actuals with DuckDB prediction."""
        if not day: return
        with self._lock:
            try:
                # 1. Get Actual Revenue from ClickHouse
                ch_client = clickhouse_connect.get_client(host="localhost", port=8123)
                try:
                    res = ch_client.query(f"SELECT sum(revenue) FROM retail_events_hot WHERE simulated_day = '{day}'")
                    actual = float(res.result_rows[0][0]) if res.result_rows and res.result_rows[0][0] is not None else 0
                finally:
                    ch_client.close()

                if actual <= 0: return

                # 2. Get Predicted Revenue from DuckDB
                with self._conn_with_retry() as conn:
                    row = conn.execute("SELECT predicted_revenue FROM live_forecasts WHERE simulated_day = ?", [day]).fetchone()
                    if not row: return
                    predicted = row[0]

                # 3. Calculate APE and update counters
                ape = abs(predicted - actual) / actual * 100
                self._mape_sum += ape
                self._days_done += 1
                avg_mape = round(self._mape_sum / self._days_done, 2)
                
                # 4. Save back to DuckDB
                with self._conn_with_retry() as conn:
                    conn.execute("UPDATE live_stream_status SET mape = ?, days_streamed = ? WHERE id = 1", [avg_mape, self._days_done])
                
                log.info(f"Day Finalized: {day} | Actual={actual:,.0f} | Pred={predicted:,.0f} | APE={ape:.1f}% | MAPE={avg_mape:.1f}%")
            except Exception as e:
                log.warning(f"Finalize day {day} failed: {e}")

    def mark_stopped(self):
        with self._lock:
            try:
                with self._conn_with_retry() as conn:
                    conn.execute("UPDATE live_stream_status SET is_running = false WHERE id = 1")
            except Exception:
                pass

import asyncio
from aiokafka import AIOKafkaConsumer

class RetailConsumer:
    def __init__(self):
        self._consumer = None
        self._writer = LiveWriter()
        self._running = True
        self._batch = []
        self._last_flush = time.time()
        self._forecast_queue = queue.Queue(maxsize=1) # Only keep one pending forecast task
        
        # Start background forecast worker (remain as thread for heavy logic)
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

    async def run(self):
        log.info(f"HP-Consumer (Async) active on {TOPIC}...")
        self._consumer = AIOKafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        await self._consumer.start()
        seen_days = set()
        last_day = None
        try:
            while self._running:
                # Check if simulation is paused (Global)
                ctrl = _read_stream_control()
                if not ctrl.get("is_running", False):
                    await asyncio.sleep(1.0)
                    continue

                # Poll for messages
                data = await self._consumer.getmany(timeout_ms=500, max_records=BATCH_SIZE)
                
                for tp, messages in data.items():
                    for msg in messages:
                        event = msg.value
                        self._batch.append(event)
                        day = event.get("simulated_day")
                        
                        if event.get("is_day_start") and day not in seen_days:
                            # If a new day started, finalize the PREVIOUS day if it exists
                            if last_day and last_day != day:
                                self._writer.finalize_day(last_day)
                            
                            seen_days.add(day)
                            last_day = day
                            self._writer.update_status(day)
                            try:
                                self._forecast_queue.put_nowait(day)
                            except queue.Full:
                                pass

                        if len(self._batch) >= BATCH_SIZE:
                            self._writer.write_batch(self._batch)
                            self._batch = []
                            self._last_flush = time.time()

                if time.time() - self._last_flush > BATCH_TIMEOUT:
                    if self._batch:
                        self._writer.write_batch(self._batch)
                        self._batch = []
                    self._last_flush = time.time()
                
                await asyncio.sleep(0.1) # Yield to event loop
        finally:
            await self._consumer.stop()
            self._writer.mark_stopped()
            log.info("Consumer shutdown.")

if __name__ == "__main__":
    consumer = RetailConsumer()
    try:
        asyncio.run(consumer.run())
    except KeyboardInterrupt:
        pass
