# DataMind — Autonomous Retail Analytics Intelligence Pipeline

https://github.com/arjeetanand/DataMind.git

> **Raw Data → Dimensional Warehouse → LlamaIndex RAG → PyTorch Forecasting → A2A Agents → Autonomous Action**

A production-grade AI/ML system demonstrating end-to-end data engineering, LLM-grounded analytics, and agentic orchestration on retail data.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DataMind Pipeline                           │
│                                                                 │
│  S3-Style Data Lake     DuckDB Star Schema     LlamaIndex RAG  │
│  (Parquet partitioned)  (fact + 4 dims)        (FAISS + NL2SQL)│
│         │                      │                      │         │
│         └──────────── ETL ─────┘          ┌───────────┘         │
│                                           │                     │
│              ┌────────────────────────────┘                     │
│              ▼                                                  │
│   ┌─────── LangGraph Orchestrator ────────┐                    │
│   │                                       │                    │
│   │  [DataAgent]──▶[InsightAgent]──▶[ActionAgent]             │
│   │      │ A2A          │ A2A          │ A2A                   │
│   │   DuckDB SQL    LLM + RAG      Alerts/Reports              │
│   │                 PyTorch LSTM                               │
│   └───────────────────────────────────────┘                    │
│                                                                 │
│              FastAPI REST  ──  Streamlit Dashboard             │
└─────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|---|---|
| Data Lake | AWS S3 / Local Parquet (Hive-partitioned, Snappy compressed) |
| Warehouse | DuckDB — star schema (fact_sales + 4 dimensions) |
| ETL | Python + PyArrow + DuckDB |
| Embeddings | sentence-transformers (all-MiniLM-L6-v2) |
| RAG | LlamaIndex + FAISS vector index + NL2SQL router |
| Forecasting | PyTorch LSTM + Attention + Monte Carlo dropout |
| Agents | LangGraph + custom A2A protocol |
| LLM Backend | Ollama (local) / Cohere Command R+ / OCI GenAI |
| API | FastAPI + Pydantic v2 |
| Dashboard | Streamlit + Plotly |

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

### 6. Launch dashboard
```bash
streamlit run app/dashboard.py
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

---

## Resume Bullets (copy-paste ready)

```
• Built a Parquet-partitioned S3-style data lake with DuckDB-powered star schema
  (fact_sales + 4 dimensions, surrogate keys, RFM segmentation) enabling 
  sub-second analytical queries across 500K+ retail transactions

• Designed a LlamaIndex RAG pipeline over structured warehouse data with NL2SQL
  routing — grounding LLM outputs in live sales, inventory, and customer context

• Trained a PyTorch LSTM + Attention forecasting model with Monte Carlo dropout
  for 7-day demand prediction; integrated as autonomous tool in the agentic layer

• Orchestrated a 3-agent A2A system (DataAgent → InsightAgent → ActionAgent) 
  using LangGraph, enabling fully autonomous insight-to-action loops — from
  raw warehouse data to reorder alerts and executive reports with zero human input
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
│   └── api/main.py              # FastAPI
└── app/dashboard.py             # Streamlit
```
