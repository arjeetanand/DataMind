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

# --- Generic Memory Cache ---
_DATA_CACHE = {}
_MAINTENANCE_MODE = False
_DATA_LOCK = threading.Lock()

def _get_cached_data(key: str, func, ttl: float = 0.5, **kwargs):
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
    """Update stream control file (avoids DuckDB write locks for control signals)."""
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
    return {"status": "ok", "service": "DataMind v2", "timestamp": time.time()}


@app.get("/warehouse/revenue-trend")
def revenue_trend():
    try:
        with _live_conn() as conn:
            df = monthly_revenue_trend(conn)
            return {"data": df.fillna(0).to_dict(orient="records"), "rows": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/warehouse/top-products")
def top_products_endpoint(n: int = 20):
    try:
        with _live_conn() as conn:
            df = top_products(n=n, conn=conn)
            return {"data": df.to_dict(orient="records"), "rows": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/warehouse/geo-revenue")
def geo_revenue_endpoint():
    try:
        with _live_conn() as conn:
            df = geo_revenue(conn=conn)
            return {"data": df.to_dict(orient="records"), "rows": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/warehouse/reorder-signals")
def reorder_signals_endpoint(lookback_days: int = 30):
    try:
        with _live_conn() as conn:
            df = reorder_signals(lookback_days=lookback_days, conn=conn)
            return {"data": df.to_dict(orient="records"), "signals": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/warehouse/rfm-summary")
def rfm_summary_endpoint():
    try:
        with _live_conn() as conn:
            df = customer_rfm_summary(conn=conn)
            return {"data": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/pipeline/run")
def run_agent_pipeline(request: PipelineRequest):
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
    return _get_cached_data("kpis", get_live_kpis, ttl=1.0)

@app.get("/live/revenue")
def live_revenue(window: int = 500):
    df = _get_cached_data(f"revenue_{window}", get_rolling_revenue, ttl=1.5, window_txns=window)
    return {"data": df.fillna(0).to_dict(orient="records"), "rows": len(df)}


@app.get("/live/transactions")
def live_transactions(n: int = 25):
    try:
        with _live_conn() as conn:
            df = get_recent_transactions(n=n, conn=conn)
            return {"data": df.to_dict(orient="records")}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/live/forecast-vs-actual")
def live_forecast_vs_actual():
    try:
        with _live_conn() as conn:
            df = get_forecast_vs_actual(conn=conn)
            return {"data": df.fillna(0).to_dict(orient="records"), "rows": len(df)}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/live/forecast-outlook")
def live_forecast_outlook():
    try:
        with _live_conn() as conn:
            from src.streaming.live_queries import get_live_forecast_outlook
            df = get_live_forecast_outlook(conn=conn)
            return {"data": df.to_dict(orient="records")}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/live/top-products")
def live_top_products_endpoint(n: int = 5):
    try:
        with _live_conn() as conn:
            df = get_live_top_products(n=n, conn=conn)
            return {"data": df.to_dict(orient="records")}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/live/geo-revenue")
def live_geo_revenue_endpoint(n: int = 10):
    try:
        with _live_conn() as conn:
            df = get_live_geo_revenue(n=n, conn=conn)
            return {"data": df.to_dict(orient="records")}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))


@app.post("/live/start")
def live_start():
    return _set_stream_control(is_running=True)


@app.post("/live/stop")
def live_stop():
    return _set_stream_control(is_running=False)


@app.post("/live/control/speed")
def set_speed(request: SpeedControlRequest):
    return _set_stream_control(speed_mode=request.speed_mode)


@app.post("/live/reset")
def reset_live_data(request: ResetRequest):
    if not request.confirm:
        raise HTTPException(400, "Send confirm=true to wipe live data")
    global _MAINTENANCE_MODE, _DATA_CACHE
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
        
        # 4. Cleanup Redis
        try:
            r = redis.Redis(host="localhost", port=6379, db=0)
            r.delete("live:kpi:revenue", "live:kpi:orders", "live:kpi:units", "live:transactions", "live:status:day", "live:status:rows")
        except: pass

        # 5. Cleanup Cache
        with _DATA_LOCK:
            _DATA_CACHE.clear()
        if STREAM_CONTROL_FILE.exists():
            _set_stream_control(is_running=False, speed_mode="normal")
            
        return {"status": "ok", "message": "Live tables reset successfully."}
    except Exception as e:
        log.error(f"Reset failed: {e}")
        raise HTTPException(500, f"Reset failed: {e}")
    finally:
        _MAINTENANCE_MODE = False


if __name__ == "__main__":
    import uvicorn
    from config.settings import API_HOST, API_PORT
    uvicorn.run("src.api.main:app", host=API_HOST, port=API_PORT, reload=True)
