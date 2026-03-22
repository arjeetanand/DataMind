"""
DataMind — Centralized Configuration
"""
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# ── Data Paths ────────────────────────────────────────────────────────────────
RAW_DATA_DIR    = BASE_DIR / "data" / "raw"
PARQUET_DIR     = BASE_DIR / "data" / "parquet"
DB_PATH         = BASE_DIR / "data" / "datamind.duckdb"
MODEL_DIR       = BASE_DIR / "data" / "models"
INDEX_DIR       = BASE_DIR / "data" / "indexes"

for p in [RAW_DATA_DIR, PARQUET_DIR, MODEL_DIR, INDEX_DIR]:
    p.mkdir(parents=True, exist_ok=True)

# ── LLM Config ────────────────────────────────────────────────────────────────
LLM_PROVIDER    = os.getenv("LLM_PROVIDER", "ollama")          # ollama | cohere | openai | oci
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "llama3.2")
COHERE_API_KEY  = os.getenv("COHERE_API_KEY", "")
OCI_ENDPOINT    = os.getenv("OCI_GENAI_SERVICE_ENDPOINT", "https://inference.generativeai.us-chicago-1.oci.oraclecloud.com")

# ── Embedding Config ──────────────────────────────────────────────────────────
EMBED_MODEL     = "sentence-transformers/all-MiniLM-L6-v2"
FAISS_INDEX     = str(INDEX_DIR / "retail_faiss.index")

# ── PyTorch Forecasting ───────────────────────────────────────────────────────
SEQ_LEN         = 60      # lookback window (days)
PRED_LEN        = 7       # forecast horizon (days)
HIDDEN_SIZE     = 128
NUM_LAYERS      = 2
BATCH_SIZE      = 64
EPOCHS          = 60
LR              = 5e-4
DEVICE          = "cpu"   # switch to "cuda" if available

# ── FastAPI ───────────────────────────────────────────────────────────────────
API_HOST        = "0.0.0.0"
API_PORT        = 8000

# ── A2A Protocol ──────────────────────────────────────────────────────────────
A2A_TIMEOUT_SEC = 30
MAX_AGENT_RETRIES = 3

# ── Kaggle Dataset ────────────────────────────────────────────────────────────
KAGGLE_CSV      = RAW_DATA_DIR / "online_retail_II.csv"
