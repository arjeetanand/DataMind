# DataMind — Autonomous Retail Analytics Intelligence Pipeline

[![GitHub](https://img.shields.io/badge/GitHub-Repository-blue?logo=github)](https://github.com/arjeetanand/DataMind.git)

> **Raw Data → Dimensional Warehouse → LlamaIndex RAG → PyTorch Forecasting → A2A Agents → Autonomous Action**

A production-grade AI/ML system demonstrating end-to-end data engineering, LLM-grounded analytics, and agentic orchestration on retail data.

---

## Architecture — Hot/Cold Split

DataMind uses a high-performance **Hot/Cold Split** architecture to handle massive ingestion volumes without locking the analytical warehouse.

```
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                                   DataMind Platform                                       │
│                                                                                           │
│  ┌───────────────────┐           ┌──────────────────────┐         ┌────────────────────┐  │
│  │  React Frontend   │ ◀───────▶ │     Redis (HOT)      │ ◀──────▶ │   FastAPI Backend  │  │
│  │ (TPS Speedometer) │           │   [Sub-ms KPIs]      │         │     (Uvicorn)      │  │
│  └───────────────────┘           └──────────────────────┘         └──────────┬─────────┘  │
│                                              ▲                               │            │
│          ┌───────────────────────────────────┼───────────────────────────────┘            │
│          │                                   │                                            │
│  ┌───────▼─────────┐           ┌─────────────┴────────┐         ┌──────────────────────┐  │
│  │ Kafka Ingestion │ ───────▶  │ ClickHouse (HOT Path)│ ──────▶ │  S3 / Parquet (COLD) │  │
│  │ [4 Partitions]  │           │ [Kafka Native Engine]│         │  [Archival Lake]     │  │
│  └─────────────────┘           └──────────────────────┘         └──────────┬───────────┘  │
│                                              │                             │              │
│          ┌───────────────────────────────────┘                 ┌───────────▼───────────┐  │
│          ▼                                                     │   DuckDB Warehouse    │  │
│  ┌───────────────┐           ┌──────────────┐                  │     (Star Schema)     │  │
│  │ PyTorch LSTM  │ ◀───────▶ │  LlamaIndex  │ ◀────────────────┤ [Analytics + Agents]  │  │
│  │ (Forecasting) │           │  FAISS RAG   │                  └───────────────────────┘  │
│  └───────────────┘           └──────────────┘                                             │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|---|---|
| **Frontend** | **React 18 + Vite + Aether Design System** |
| **Hot Path** | **ClickHouse** (Zero-lock high-volume ingestion) |
| **KPI Cache** | **Redis** (Real-time speed & aggregation) |
| **Warehouse** | **DuckDB** (Star Schema, analytical warehouse) |
| **Streaming** | **Kafka + Native Engine** (Sub-second sync) |
| **AI Layer** | **PyTorch LSTM + Attention** (Forecast) |
| **Orchestration** | **LangGraph** (A2A Multi-agent Protocol) |
| **Data Lake** | **Parquet** (Archival Lake) |

---

## Quick Start — Batch Analytics

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Download dataset
Download [Online Retail II dataset](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci) and place `online_retail_II.csv` in `data/raw/`.

### 3. Run ingestion + ETL
```bash
python -m src.ingestion.data_loader     # CSV → Parquet lake
python -m src.warehouse.etl             # Parquet → DuckDB star schema
```

### 4. Train forecasting model
```bash
python -m src.ml.forecaster
```

### 5. Start API & Dashboards
```bash
# Start API
uvicorn src.api.main:app --reload

# Start React Frontend
cd frontend && npm install && npm run dev
cd frontend && npm run dev
```

---

## Live Streaming Feed

DataMind simulates a high-velocity production environment using Kafka and ClickHouse.

### 1. Start Infrastructure
```bash
docker compose up -d   # Starts Kafka, Redis, ClickHouse, Grafana
```

### 2. Start Streaming Pipeline
```bash
# Terminal A: Start Consumer
python -m src.streaming.consumer

# Terminal B: Start Producer (Simulate high-volume traffic)
python -m src.streaming.producer --speed normal  # normal | fast | burst
```

### 3. Runtime Control & Testing
- **Sync Status**: `GET /live/status`
- **KPI Metrics**: `GET /live/kpis`
- **Reset Dashboard**: `POST /live/reset` (Reliably purges Kafka, ClickHouse, Redis, and DuckDB)
- **Terminal Test Suite**: `python test_cli.py` (Interactive verification of all 9 pipeline features)

---

## Agent Pipeline

The **A2A (Agent-to-Agent)** protocol automates complex analysis via a LangGraph state machine.

```json
POST /pipeline/run
{
  "intent": "reorder_signals",
  "params": {"lookback_days": 30},
  "mode": "full"
}
```

1. **DataAgent** — Fetches multi-dimensional data from DuckDB via SQL.
2. **InsightAgent** — LlamaIndex RAG + PyTorch Forecast + LLM Synthesis.
3. **ActionAgent** — Idempotent execution of alerts and executive reports.

---

## Latest Improvements
- **Live Forecast Correction**: Implemented dynamic scale calibration in `consumer.py` to align ML predictions with high-volume live revenue (£100k+/day) in real-time.
- **Robust Reset Pipeline**: Fixed critical `NameError` and async-blocking bugs in the `/live/reset` endpoint, ensuring 100% data purge across Kafka, ClickHouse, and Redis.
- **Interactive Test CLI**: Created `test_cli.py`, a comprehensive terminal test suite for validating API health, streaming performance, and model accuracy.
- **100% Documentation**: Standardized 2-line "Action + Impact" docstrings across all backend modules.
- **Clean Repository**: Removed extraneous testing and temporary data folders for production readiness.

---

## Resume Bullets (Production-Grade Architecture)

• Orchestrated a Hot/Cold Data Split using **ClickHouse** and **DuckDB**, implementing a zero-lock ingestion pipeline via **Native Kafka Engine** that decoupled real-time simulation traffic from analytical warehouse queries.

• Engineered a sub-millisecond KPI Dashboard using **Redis** as a write-through cache, optimizing dashboard responsiveness and enabling real-time Ingestion Speed (TPS) monitoring for high-velocity streams (5,000+ txns/s).

• Developed a **PyTorch LSTM + Attention** forecasting model with Monte Carlo dropout for demand prediction, integrated with an automated MAPE-aware retraining loop.

• Built a multi-agent A2A (Agent-to-Agent) protocol using **LangGraph** and **LlamaIndex RAG**, enabling fully autonomous insight-to-report generation with zero human in the loop.

---
*Developed by Arjeet Anand — 2024*
