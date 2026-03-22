# DataMind — Autonomous Retail Analytics Intelligence Pipeline

https://github.com/arjeetanand/DataMind.git

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

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Download dataset
Download [Online Retail II dataset](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci)
and place `online_retail_II.csv` in `data/raw/`.

### 3. Run ingestion + ETL
```bash
python -m src.ingestion.data_loader     # CSV → Parquet lake
python -m src.warehouse.etl             # Parquet → DuckDB star schema
```

### 4. Train forecasting model
```bash
python -m src.ml.forecaster
```

### 5. Start API
```bash
uvicorn src.api.main:app --reload
```

### 6. Launch Streamlit Dashboard
```bash
streamlit run app/dashboard.py
```

### 7. Launch React Dashboard (Modern UI)
```bash
cd frontend
npm install
npm run dev
```
### 8. Run Streaming Simulation (Optional)
```bash
# Start Kafka (KRaft mode)
docker-compose up -d

# Start Consumer
python -m src.streaming.consumer

# Start Producer (loads CSV and streams)
python -m src.streaming.producer --speed fast
```

---

## LLM Configuration

Set `LLM_PROVIDER` environment variable:
```bash
# Option A: OCI GenAI (Default)
export LLM_PROVIDER=oci
export OCI_GENAI_MODEL_ID=openai.gpt-5.2

# Option B: Ollama (local, free)
export LLM_PROVIDER=ollama
export OLLAMA_MODEL=llama3.2

# Option C: Cohere
export LLM_PROVIDER=cohere
export COHERE_API_KEY=your_key
```

---

## Dimensional Model

```
dim_date ──────┐
dim_product ───┤
               ├── fact_sales ──── agg_daily_sales
dim_customer ──┤
dim_geography ─┘
```

**Surrogate keys, SCD Type 1, RFM-based customer segmentation.**

---

## Agent Pipeline

```
POST /pipeline/run
{
  "intent": "reorder_signals",
  "params": {"lookback_days": 30},
  "mode": "full"
}
```

Runs:
1. **DataAgent** — fetches reorder data from DuckDB via Advanced SQL
2. **InsightAgent** — LlamaIndex RAG + LLM analysis + LSTM forecast
3. **ActionAgent** — generates reorder alert JSON + saves report

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Health check |
| GET | `/warehouse/revenue-trend` | Monthly revenue with MoM growth |
| GET | `/warehouse/top-products?n=20` | Top products by revenue |
| GET | `/warehouse/geo-revenue` | Revenue by country/region |
| GET | `/warehouse/reorder-signals` | Products flagged for reorder |
| GET | `/warehouse/rfm-summary` | Customer RFM segments |
| POST | `/pipeline/run` | Full agent pipeline |
| POST | `/query/nl` | Natural language query |
| GET | `/live/status` | Live stream health/progress |
| GET | `/live/kpis` | Real-time aggregate metrics |
| GET | `/live/revenue` | Rolling live revenue window |
| GET | `/live/transactions` | Latest transaction ticker |
| GET | `/live/forecast-vs-actual` | Live error tracking |

---

## Resume Bullets (Production-Grade Architecture)

```
• Orchestrated a Hot/Cold Data Split using ClickHouse (Hot Path) and DuckDB (Cold Path), 
  implementing a zero-lock ingestion pipeline via Native Kafka Engine that decoupled 
  real-time simulation traffic from analytical warehouse queries.

• Engineered a sub-millisecond KPI Dashboard using Redis as a write-through cache, 
  optimizing dashboard responsiveness and enabling real-time Ingestion Speed (TPS) monitoring 
  for high-velocity retail event streams (5,000+ txns/s).

• Developed a PyTorch LSTM + Attention forecasting model with Monte Carlo dropout 
  for demand prediction, integrated with an automated MAPE-aware retraining loop 
  that ensures model accuracy stays below 15% during simulated market shifts.

• Built a multi-agent A2A (Agent-to-Agent) protocol using LangGraph and LlamaIndex RAG, 
  enabling fully autonomous insight-to-report generation — from SQL retrieval to 
  executive summary synthesis with zero human in the loop.
```

---

## Project Structure

```
datamind/
├── config/settings.py           # Centralised config
├── data/
│   ├── raw/                     # Kaggle CSV
│   ├── parquet/                 # Hive-partitioned lake
│   ├── datamind.duckdb          # Warehouse
│   ├── models/                  # PyTorch checkpoints
│   └── reports/                 # Agent-generated outputs
├── src/
│   ├── ingestion/data_loader.py # CSV → Parquet
│   ├── warehouse/
│   │   ├── schema.py            # DDL
│   │   ├── etl.py               # ETL pipeline
│   │   └── queries.py           # Advanced SQL library
│   ├── ml/forecaster.py         # PyTorch LSTM
│   ├── rag/indexer.py           # LlamaIndex + NL2SQL
│   ├── agents/
│   │   ├── base_agent.py        # A2A protocol
│   │   ├── data_agent.py        # SQL retrieval
│   │   ├── insight_agent.py     # RAG + LLM
│   │   ├── action_agent.py      # Alerts + reports
│   │   └── orchestrator.py      # LangGraph graph
│   ├── api/main.py              # FastAPI
│   └── streaming/
│       ├── consumer.py          # Async Kafka consumer
│       ├── producer.py          # Async Kafka producer
│       ├── live_queries.py      # DuckDB live window queries
│       └── live_schema.py       # Live table DDL
├── utils/
│   ├── oci_llm_service.py       # Oracle Cloud LLM integration
│   └── schema.py                # LLM request schemas
└── app/dashboard.py             # Streamlit
```
