"""
DataMind — FastAPI REST API
Production serving layer over the LangGraph pipeline.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import logging
import time
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.agents.orchestrator import run_pipeline
from src.warehouse.queries import (monthly_revenue_trend, top_products,
                                    geo_revenue, reorder_signals, customer_rfm_summary)
import duckdb
from config.settings import DB_PATH

log = logging.getLogger(__name__)

app = FastAPI(
    title       = "DataMind API",
    description = "Autonomous Retail Analytics Intelligence — powered by LangGraph A2A agents",
    version     = "1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)


# ── Request / Response Models ─────────────────────────────────────────────────
class PipelineRequest(BaseModel):
    intent : str
    params : dict  = {}
    mode   : str   = "quick"          # "quick" | "full"


class NLQueryRequest(BaseModel):
    question: str


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "DataMind", "timestamp": time.time()}


# ── Direct Warehouse Endpoints ────────────────────────────────────────────────
@app.get("/warehouse/revenue-trend")
def revenue_trend():
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        df   = monthly_revenue_trend(conn)
        df   = df.fillna(0)
        return {"data": df.to_dict(orient="records"), "rows": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/warehouse/top-products")
def top_products_endpoint(n: int = 20):
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        df   = top_products(n=n, conn=conn)
        return {"data": df.to_dict(orient="records"), "rows": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/warehouse/geo-revenue")
def geo_revenue_endpoint():
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        df   = geo_revenue(conn=conn)
        return {"data": df.to_dict(orient="records"), "rows": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/warehouse/reorder-signals")
def reorder_signals_endpoint(lookback_days: int = 30):
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        df   = reorder_signals(lookback_days=lookback_days, conn=conn)
        return {"data": df.to_dict(orient="records"), "signals": len(df)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/warehouse/rfm-summary")
def rfm_summary_endpoint():
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        df   = customer_rfm_summary(conn=conn)
        return {"data": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(500, str(e))


# ── Agentic Pipeline Endpoint ─────────────────────────────────────────────────
@app.post("/pipeline/run")
def run_agent_pipeline(request: PipelineRequest):
    """
    Triggers the full DataAgent → InsightAgent → ActionAgent pipeline.
    Returns trace, insights, and action results.
    """
    VALID_INTENTS = ["revenue_trend", "top_products", "geo_revenue",
                     "daily_series", "reorder_signals", "rfm_summary"]
    if request.intent not in VALID_INTENTS:
        raise HTTPException(400, f"Invalid intent. Valid: {VALID_INTENTS}")
    try:
        start  = time.time()
        result = run_pipeline(request.intent, request.params, request.mode)
        elapsed = round(time.time() - start, 2)
        return {
            "status"        : "success",
            "intent"        : request.intent,
            "elapsed_sec"   : elapsed,
            "trace"         : result.get("trace", []),
            "errors"        : result.get("errors", []),
            "data_summary"  : result.get("data_result", {}).get("summary"),
            "insight"       : result.get("insight_result", {}).get("narrative"),
            "action"        : result.get("action_result"),
        }
    except Exception as e:
        log.exception("Pipeline error")
        raise HTTPException(500, str(e))


# ── NL Query Endpoint ──────────────────────────────────────────────────────────
@app.post("/query/nl")
def natural_language_query(request: NLQueryRequest):
    """Natural language → SQL or RAG answer over the warehouse."""
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        from src.rag.indexer import build_index, NL2SQLRouter
        index  = build_index(conn)
        router = NL2SQLRouter(conn, index)
        result = router.query(request.question)
        return {"question": request.question, "result": result}
    except Exception as e:
        raise HTTPException(500, str(e))


if __name__ == "__main__":
    import uvicorn
    from config.settings import API_HOST, API_PORT
    uvicorn.run("src.api.main:app", host=API_HOST, port=API_PORT, reload=True)
