"""
DataMind — FastAPI REST API
Serves both warehouse analytics and live Kafka streaming data.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Literal
import logging
import time
import json
import sys
import redis
from pathlib import Path
import threading
import pandas as pd
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.agents.orchestrator import run_pipeline
from src.warehouse.queries import (
    monthly_revenue_trend, top_products, geo_revenue,
    reorder_signals, customer_rfm_summary,
)
from src.streaming.live_queries import (
    get_rolling_revenue, get_recent_transactions,
    get_forecast_vs_actual, get_stream_status,
    get_live_kpis, get_live_top_products, get_live_geo_revenue,
)
from src.streaming.live_schema import LIVE_SCHEMA_DDL, DELETE_LIVE_DATA_SQL
import duckdb
from config.settings import DB_PATH

STREAM_CONTROL_FILE = Path(__file__).resolve().parents[2] / "data" / "stream_control.json"

log = logging.getLogger(__name__)

def _live_conn(read_only: bool = True):
    """Establish a DuckDB connection with aggressive retry logic for high concurrency.
    Ensures API requests can read from the database even during heavy ingestion."""
    import time
    max_retries = 60
    for i in range(max_retries):
        try:
            return duckdb.connect(str(DB_PATH), read_only=read_only)
        except Exception as e:
            err_msg = str(e).lower()
            if i < max_retries - 1 and ("locked" in err_msg or "already open" in err_msg or "used by another process" in err_msg):
                time.sleep(0.5)
                continue
            raise HTTPException(503, f"Database busy/locked: {e}")


async def _reset_kafka_topic() -> dict:
    """Purge all messages from the retail-events topic by recreating it.
    Ensures that a simulation reset starts from an empty message queue."""
    import asyncio
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    topic_name = "retail-events-v1"
    bootstrap_servers = "localhost:9092"
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        await admin.start()
        # Delete if exists
        try:
            await admin.delete_topics([topic_name])
            await asyncio.sleep(2.0)  # Non-blocking wait for Kafka to propagate deletion
        except Exception as e:
            log.warning(f"Kafka topic delete failed (may not exist yet): {e}")
        
        # Recreate
        new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        await admin.create_topics([new_topic])
        log.info(f"Kafka topic '{topic_name}' recreated successfully.")
        return {"ok": True}
    except Exception as e:
        log.warning(f"Kafka reset failed: {e}")
        return {"ok": False, "error": str(e)}
    finally:
        await admin.close()


def _clear_clickhouse_live_data():
    """Truncate the ClickHouse hot table to remove all live streaming data.
    Called during reset to purge ingested events that bypass DuckDB writes."""
    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(host="localhost", port=8123)
        try:
            client.command("TRUNCATE TABLE IF EXISTS retail_events_hot")
            log.info("ClickHouse retail_events_hot truncated successfully.")
        finally:
            client.close()
    except Exception as e:
        log.warning(f"ClickHouse clear failed: {e}")

# --- Generic Memory Cache ---
_DATA_CACHE = {}
_MAINTENANCE_MODE = False
_DATA_LOCK = threading.Lock()

def _get_cached_data(key: str, func, ttl: float = 0.5, **kwargs):
    """Fetch data from a local thread-safe memory cache with TTL expiration.
    Reduces database load by serving frequently requested metrics from RAM."""
    global _MAINTENANCE_MODE
    if _MAINTENANCE_MODE:
        return {"status": "maintenance", "message": "System is resetting..."}
    now = time.time()
    with _DATA_LOCK:
        if key in _DATA_CACHE:
            val, updated = _DATA_CACHE[key]
            if now - updated < ttl:
                return val
    try:
        with _live_conn(read_only=True) as conn:
            data = func(conn=conn, **kwargs)
            with _DATA_LOCK:
                _DATA_CACHE[key] = (data, now)
            return data
    except Exception as e:
        with _DATA_LOCK:
            if key in _DATA_CACHE: return _DATA_CACHE[key][0]
        raise HTTPException(503, f"Data unavailable ({key}): {e}")

# --- API App ---
app = FastAPI(
    title="DataMind API",
    description="Autonomous Retail Analytics + Live Kafka Streaming",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class PipelineRequest(BaseModel):
    intent: str
    params: dict = {}
    mode: str = "quick"


class NLQueryRequest(BaseModel):
    question: str


class SpeedControlRequest(BaseModel):
    speed_mode: Literal["normal", "fast", "burst"]


class ResetRequest(BaseModel):
    confirm: bool = False


def _set_stream_control(is_running: Optional[bool] = None, speed_mode: Optional[str] = None) -> dict:
    """Atomic update of the simulation control file to signal the producer/consumer.
    Bypasses database locks to provide responsive control signals for the stream."""
    try:
        STREAM_CONTROL_FILE.parent.mkdir(parents=True, exist_ok=True)
        current = {"is_running": False, "speed_mode": "normal"}
        if STREAM_CONTROL_FILE.exists():
            try:
                current = json.loads(STREAM_CONTROL_FILE.read_text())
            except: pass
        
        if is_running is not None: current["is_running"] = is_running
        if speed_mode is not None: current["speed_mode"] = speed_mode
        current["updated_at"] = time.time()

        tmp = STREAM_CONTROL_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(current))
        tmp.replace(STREAM_CONTROL_FILE)
        return {"status": "ok", **current}
    except Exception as e:
        log.error(f"Failed to update stream control: {e}")
        raise HTTPException(500, str(e))


@app.get("/health")
def health():
    """Simple health check endpoint to verify API availability.
    Returns service version and current server timestamp."""
    return {"status": "ok", "service": "DataMind v2", "timestamp": time.time()}


@app.get("/warehouse/revenue-trend")
def revenue_trend():
    """Retrieve historical revenue trends and MoM growth metrics from the warehouse.
    Serves the main analytical dashboard with trend visualization data."""
    try:
        with _live_conn() as conn:
            df = monthly_revenue_trend(conn)
            return {"data": df.fillna(0).to_dict(orient="records"), "rows": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/warehouse/top-products")
def top_products_endpoint(n: int = 20):
    """Fetch the top 'n' overall performing products by revenue from history.
    Provides detailed unit counts and rankings for the product catalog."""
    try:
        with _live_conn() as conn:
            df = top_products(n=n, conn=conn)
            return {"data": df.to_dict(orient="records"), "rows": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/warehouse/geo-revenue")
def geo_revenue_endpoint():
    """Get regional and country-level revenue distribution from the DuckDB warehouse.
    Used to visualize global market penetration and regional success."""
    try:
        with _live_conn() as conn:
            df = geo_revenue(conn=conn)
            return {"data": df.to_dict(orient="records"), "rows": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/warehouse/reorder-signals")
def reorder_signals_endpoint(lookback_days: int = 30):
    """Retrieve inventory reorder signals for products trending downwards.
    Automates stock management by identifying potential supply chain gaps."""
    try:
        with _live_conn() as conn:
            df = reorder_signals(lookback_days=lookback_days, conn=conn)
            return {"data": df.to_dict(orient="records"), "signals": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/warehouse/rfm-summary")
def rfm_summary_endpoint():
    """Fetch the summarized customer segmentation results (RFM Analysis).
    Returns the distribution of customers across various engagement tiers."""
    try:
        with _live_conn() as conn:
            df = customer_rfm_summary(conn=conn)
            return {"data": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/pipeline/run")
def run_agent_pipeline(request: PipelineRequest):
    """Execute a multi-stage autonomous agent pipeline for complex analytics.
    Orchestrates data retrieval, insight generation, and recommended actions."""
    valid_intents = [
        "revenue_trend", "top_products", "geo_revenue",
        "daily_series", "reorder_signals", "rfm_summary",
    ]
    if request.intent not in valid_intents:
        raise HTTPException(400, f"Invalid intent. Valid: {valid_intents}")
    try:
        start = time.time()
        result = run_pipeline(request.intent, request.params, request.mode)
        return {
            "status": "success",
            "intent": request.intent,
            "elapsed_sec": round(time.time() - start, 2),
            "trace": result.get("trace", []),
            "errors": result.get("errors", []),
            "data_summary": result.get("data_result", {}).get("summary"),
            "insight": result.get("insight_result", {}).get("narrative"),
            "action": result.get("action_result"),
        }
    except Exception as e:
        log.exception("Pipeline error")
        raise HTTPException(500, str(e))


@app.post("/query/nl")
def natural_language_query(request: NLQueryRequest):
    """Accept a natural language question and route it to the NL2SQL engine.
    Uses RAG indexing to generate and execute precise SQL against the warehouse."""
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        from src.rag.indexer import build_index, NL2SQLRouter
        index = build_index(conn)
        router = NL2SQLRouter(conn, index)
        result = router.query(request.question)
        return {"question": request.question, "result": result}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/live/status")
def live_status():
    """Retrieve the current state of the live Kafka stream and simulation.
    Includes running status, current day, total rows, and MAPE accuracy."""
    status = _get_cached_data("status", get_stream_status, ttl=0.3)
    # Overlay is_running and speed_mode from control file (master source)
    if STREAM_CONTROL_FILE.exists():
        try:
            ctrl = json.loads(STREAM_CONTROL_FILE.read_text())
            status["is_running"] = ctrl.get("is_running", False)
            status["speed_mode"] = ctrl.get("speed_mode", "normal")
        except: pass
    return status

@app.get("/live/kpis")
def live_kpis():
    """Fetch high-frequency KPIs (Revenue, Orders, TPS) from the Redis hot layer.
    Optimized for sub-millisecond status dashboard updates."""
    return _get_cached_data("kpis", get_live_kpis, ttl=1.0)

@app.get("/live/revenue")
def live_revenue(window: int = 500):
    """Get the latest rolling revenue timeseries for real-time charting.
    Aggregates data across the specified transaction window from ClickHouse."""
    df = _get_cached_data(f"revenue_{window}", get_rolling_revenue, ttl=1.5, window_txns=window)
    return {"data": df.fillna(0).to_dict(orient="records"), "rows": len(df)}


@app.get("/live/transactions")
def live_transactions(n: int = 25):
    """Retrieve the most recent 'n' transactions from the streaming pipeline.
    Populates the live feed ticker with fresh retail event data."""
    try:
        with _live_conn() as conn:
            df = get_recent_transactions(n=n, conn=conn)
            return {"data": df.to_dict(orient="records")}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/live/forecast-vs-actual")
def live_forecast_vs_actual():
    """Compare recent ML forecasts with real-time actual sales performance.
    Used for evaluating model precision and calculating drift in real-time."""
    try:
        with _live_conn() as conn:
            df = get_forecast_vs_actual(conn=conn)
            return {"data": df.fillna(0).to_dict(orient="records"), "rows": len(df)}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/live/forecast-outlook")
def live_forecast_outlook():
    """Fetch the 7-day future revenue projection generated by the LSTM engine.
    Provides the forward-looking visibility displayed on the live dashboard."""
    try:
        with _live_conn() as conn:
            from src.streaming.live_queries import get_live_forecast_outlook
            df = get_live_forecast_outlook(conn=conn)
            return {"data": df.to_dict(orient="records")}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/live/top-products")
def live_top_products_endpoint(n: int = 5):
    """Identity the top 'n' products appearing in the current live stream.
    Aggregates recently consumed events from ClickHouse for hot-item detection."""
    try:
        with _live_conn() as conn:
            df = get_live_top_products(n=n, conn=conn)
            return {"data": df.to_dict(orient="records")}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/live/geo-revenue")
def live_geo_revenue_endpoint(n: int = 10):
    """Track the global distribution of sales in the live streaming data.
    Provides real-time geographic heatmapping of incoming retail orders."""
    try:
        with _live_conn() as conn:
            df = get_live_geo_revenue(n=n, conn=conn)
            return {"data": df.to_dict(orient="records")}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))


@app.post("/live/start")
def live_start():
    """Signal the simulation to start streaming data from the source file.
    Triggers both the Kafka producer loop and the consumer processing."""
    return _set_stream_control(is_running=True)


@app.post("/live/stop")
def live_stop():
    """Pause the live data simulation by signaling all streaming workers.
    Ensures safe, state-preserving halt of the Kafka pipeline."""
    return _set_stream_control(is_running=False)


@app.post("/live/control/speed")
def set_speed(request: SpeedControlRequest):
    """Adjust the simulation emission speed (normal, fast, or burst).
    Allows testing system performance and UI responsiveness under varying loads."""
    return _set_stream_control(speed_mode=request.speed_mode)


@app.post("/live/reset")
async def reset_live_data(request: ResetRequest):
    """Wipe all live data, Redis KPIs, and DuckDB stream state.
    Requires explicit confirmation; used to restart the simulation from scratch."""
    if not request.confirm:
        raise HTTPException(400, "Send confirm=true to wipe live data")
    global _MAINTENANCE_MODE, _DATA_CACHE
    clickhouse_cleanup = {"ok": False, "error": None}
    try:
        # 1. Enable Maintenance Mode & force stop simulation
        _MAINTENANCE_MODE = True
        _set_stream_control(is_running=False)
        
        # 2. Wait for background connections to drain
        time.sleep(3.0)
        
        # 3. Wipe and re-init status
        # Exclusive access should be much easier now
        with _live_conn(read_only=False) as conn:
            conn.execute(DELETE_LIVE_DATA_SQL)
            conn.execute("""
                INSERT OR REPLACE INTO live_stream_status (id, is_running, speed_mode, current_day, total_rows)
                VALUES (1, false, 'normal', NULL, 0)
            """)

        # 4. Wipe ClickHouse hot table (source for live charts/transactions)
        _clear_clickhouse_live_data()
        
        # 5. Purge Kafka Topic
        await _reset_kafka_topic()

        # 6. Cleanup Redis
        try:
            import redis
            r = redis.Redis(host="localhost", port=6379, db=0)
            r.delete(
                "live:kpi:revenue", "live:kpi:orders", "live:kpi:units",
                "live:transactions", "live:status:day", "live:status:rows",
                "live:unique_customers", "live:tps",
            )
            log.info("Redis KPI keys cleared.")
        except Exception as e:
            log.warning(f"Redis cleanup failed: {e}")

        # 6. Cleanup Cache
        with _DATA_LOCK:
            _DATA_CACHE.clear()

        # 7. Second-pass cleanup for any late background forecast writes
        time.sleep(0.5)
        with _live_conn(read_only=False) as conn:
            conn.execute("DELETE FROM live_forecasts")
            conn.execute("DELETE FROM live_forecast_outlook")
            conn.execute("""
                INSERT OR REPLACE INTO live_stream_status (id, is_running, speed_mode, current_day, total_rows)
                VALUES (1, false, 'normal', NULL, 0)
            """)

        if STREAM_CONTROL_FILE.exists():
            _set_stream_control(is_running=False, speed_mode="normal")

        message = "Live data reset successfully."
        if not clickhouse_cleanup.get("ok"):
            message += " (Warning: ClickHouse hot table cleanup failed; some chart data may persist until ClickHouse is reachable.)"

        return {
            "status": "ok",
            "message": message,
            "clickhouse_cleared": bool(clickhouse_cleanup.get("ok")),
        }
    except Exception as e:
        log.error(f"Reset failed: {e}")
        raise HTTPException(500, f"Reset failed: {e}")
    finally:
        _MAINTENANCE_MODE = False


if __name__ == "__main__":
    import uvicorn
    from config.settings import API_HOST, API_PORT
    uvicorn.run("src.api.main:app", host=API_HOST, port=API_PORT, reload=True)
