"""
DataMind — LlamaIndex RAG over DuckDB Warehouse

Strategy:
  1. Convert all dimension & aggregate tables to text documents
  2. Index with FAISS + sentence-transformers embeddings
  3. QueryEngine routes: semantic search OR NL2SQL
  4. LLM grounds response in retrieved warehouse context
"""

import sys
# Legacy shim for LlamaIndex/LangChain version mismatch
try:
    import langchain_core.callbacks
    sys.modules['langchain.callbacks'] = langchain_core.callbacks
except ImportError:
    pass

import duckdb
import pandas as pd
import json
import logging
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))
from config.settings import DB_PATH, FAISS_INDEX, EMBED_MODEL, LLM_PROVIDER, OLLAMA_BASE_URL, OLLAMA_MODEL

log = logging.getLogger(__name__)


# ── Document Builder: Table Rows → Text ───────────────────────────────────────
def _build_documents(conn: duckdb.DuckDBPyConnection) -> list:
    """Transform structured warehouse tables into LlamaIndex Document objects for indexing.
    Iterates through revenue summaries, product catalogs, and geographic aggregates to build text context."""
    try:
        from llama_index.core import Document
    except ImportError:
        raise ImportError("pip install llama-index-core llama-index-embeddings-huggingface")

    docs = []

    # ── Monthly revenue summaries ──────────────────────────────────────────────
    monthly = conn.execute("""
        SELECT d.year, d.month_name, 
               ROUND(SUM(f.revenue),2) AS revenue,
               COUNT(DISTINCT f.invoice) AS orders
        FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month_name, d.month
        ORDER BY d.year, d.month
    """).df()
    for _, row in monthly.iterrows():
        text = (f"In {row['month_name']} {int(row['year'])}, total revenue was "
                f"£{row['revenue']:,.2f} across {int(row['orders'])} orders.")
        docs.append(Document(text=text, metadata={"type": "monthly_revenue",
                                                   "year": int(row["year"]),
                                                   "month": row["month_name"]}))

    # ── Top product descriptions ───────────────────────────────────────────────
    products = conn.execute("""
        SELECT p.description, p.price_band,
               ROUND(SUM(f.revenue),2) AS revenue,
               SUM(f.quantity) AS units
        FROM fact_sales f JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.description, p.price_band
        ORDER BY revenue DESC LIMIT 100
    """).df()
    for _, row in products.iterrows():
        text = (f"Product '{row['description']}' (price band: {row['price_band']}) "
                f"generated £{row['revenue']:,.2f} revenue from {int(row['units'])} units sold.")
        docs.append(Document(text=text, metadata={"type": "product", "price_band": row["price_band"]}))

    # ── Customer segment summaries ─────────────────────────────────────────────
    segs = conn.execute("""
        SELECT c.customer_segment, COUNT(*) AS customers,
               ROUND(SUM(f.revenue),2) AS revenue
        FROM fact_sales f JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.customer_segment
    """).df()
    for _, row in segs.iterrows():
        text = (f"The {row['customer_segment']}-value customer segment contains "
                f"{int(row['customers'])} customers and contributed £{row['revenue']:,.2f} in revenue.")
        docs.append(Document(text=text, metadata={"type": "segment"}))

    # ── Geographic summaries ───────────────────────────────────────────────────
    geo = conn.execute("""
        SELECT g.country, ROUND(SUM(f.revenue),2) AS revenue
        FROM fact_sales f JOIN dim_geography g ON f.geo_key = g.geo_key
        GROUP BY g.country ORDER BY revenue DESC LIMIT 20
    """).df()
    for _, row in geo.iterrows():
        text = (f"{row['country']} contributed £{row['revenue']:,.2f} in total revenue.")
        docs.append(Document(text=text, metadata={"type": "geography", "country": row["country"]}))

    log.info(f"Built {len(docs)} warehouse documents for indexing")
    return docs


# ── Index Builder ─────────────────────────────────────────────────────────────
_GLOBAL_EMBED_MODEL = None

def build_index(conn: duckdb.DuckDBPyConnection, persist_dir: str = str(Path(FAISS_INDEX).parent)):
    """Initialise or reload the FAISS vector index using embedded warehouse documents.
    Sets up the global embedding model and LLM settings (Ollama/OCI) for RAG operations."""
    from llama_index.core import VectorStoreIndex, StorageContext, load_index_from_storage
    from llama_index.core.settings import Settings
    from llama_index.embeddings.huggingface import HuggingFaceEmbedding

    if LLM_PROVIDER == "ollama":
        from llama_index.llms.ollama import Ollama
        Settings.llm = Ollama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, request_timeout=120.0)
    elif LLM_PROVIDER == "oci":
        from llama_index.llms.langchain import LangChainLLM
        from utils.oci_llm_service import OCILLMService
        Settings.llm = LangChainLLM(llm=OCILLMService().llm)
    else:
        from llama_index.core.llms.mock import MockLLM
        Settings.llm = MockLLM()

    global _GLOBAL_EMBED_MODEL
    if _GLOBAL_EMBED_MODEL is None:
        log.info(f"Initializing embedding model: {EMBED_MODEL}")
        _GLOBAL_EMBED_MODEL = HuggingFaceEmbedding(model_name=EMBED_MODEL)
    
    Settings.embed_model = _GLOBAL_EMBED_MODEL

    persist_path = Path(persist_dir)
    if (persist_path / "docstore.json").exists():
        log.info("Loading existing FAISS index...")
        storage_ctx = StorageContext.from_defaults(persist_dir=persist_dir)
        index = load_index_from_storage(storage_ctx)
    else:
        log.info("Building new FAISS index over warehouse documents...")
        docs  = _build_documents(conn)
        index = VectorStoreIndex.from_documents(docs, embed_model=_GLOBAL_EMBED_MODEL, show_progress=True)
        persist_path.mkdir(parents=True, exist_ok=True)
        index.storage_context.persist(persist_dir=persist_dir)
        log.info(f"Index persisted to {persist_dir}")

    return index


# ── NL2SQL Router ─────────────────────────────────────────────────────────────
class NL2SQLRouter:
    """
    Routes natural language queries to:
      - Direct DuckDB SQL execution (structured data questions)
      - LlamaIndex semantic search (conceptual / trend questions)
    """

    SQL_KEYWORDS = ["total", "sum", "average", "count", "how many", "revenue", "sales",
                    "top", "most", "highest", "best", "least", "between", "compare",
                    "country", "countries", "month", "months", "product", "products",
                    "trend", "growth", "performance", "daily"]

    def __init__(self, conn: duckdb.DuckDBPyConnection, index):
        """Initialise the NL2SQL router with a database connection and a vector index.
        Prepares the query engine for similarity search and SQL generation routing."""
        self.conn  = conn

    def _is_sql_query(self, query: str) -> bool:
        """Determine if a natural language query should be routed to a structured SQL path.
        Checks for specific analytical keywords like 'total', 'sum', or 'revenue'."""
        q = query.lower()
        return any(kw in q for kw in self.SQL_KEYWORDS)

    def _generate_sql(self, query: str) -> str:
        """Translate a natural language request into a valid DuckDB SQL string using LLM or rules.
        Combines pattern matching with deep LLM reasoning for robust schema awareness."""
        # 1. Try rule-based first (Fast & Free)
        rule_sql = self._rule_based_sql(query)
        if rule_sql: return rule_sql

        # 2. LLM-based SQL Generation (True AI)
        from llama_index.core.settings import Settings
        prompt = f"""
        Given the following DuckDB schema:
        - fact_sales (id, invoice, stock_code, description, quantity, price, revenue, customer_key, geo_key, date_key)
        - dim_product (product_key, stock_code, description, price_band)
        - dim_date (date_key, date, year, month, month_name, day_of_week)
        - dim_customer (customer_key, customer_id, customer_segment)
        - dim_geography (geo_key, country)

        Convert this natural language question into a VALID DuckDB SQL query:
        "{query}"

        Return ONLY the SQL code, no explanation.
        """
        try:
            response = Settings.llm.complete(prompt)
            sql = str(response).strip().replace("```sql", "").replace("```", "")
            if "SELECT" in sql.upper():
                return sql
        except Exception as e:
            log.warning(f"LLM NL2SQL failed: {e}")
        
        return None

    def _rule_based_sql(self, query: str) -> str:
        """Execute fast, high-confidence SQL generation for standard dashboard patterns.
        Handles common requests like top products or monthly trends without LLM latency."""
        q = query.lower()
        
        # Pattern 1: Top Products
        if ("top" in q or "most" in q or "highest" in q or "best" in q) and ("product" in q or "item" in q):
            n = 10
            for w in q.split():
                if w.isdigit(): n = int(w); break
            return f"""
                SELECT p.description, ROUND(SUM(f.revenue),2) AS revenue
                FROM fact_sales f JOIN dim_product p ON f.product_key=p.product_key
                GROUP BY p.description ORDER BY revenue DESC LIMIT {n}
            """
        
        # Pattern 2: Monthly Revenue / Sales Trend
        if ("revenue" in q or "sales" in q) and ("month" in q or "trend" in q):
            return """
                SELECT d.year, d.month_name, ROUND(SUM(f.revenue),2) AS revenue
                FROM fact_sales f JOIN dim_date d ON f.date_key=d.date_key
                GROUP BY d.year, d.month_name, d.month ORDER BY d.year, d.month
            """
        
        # Pattern 3: Country Revenue (Handles "country" or "countries")
        if "countr" in q or "geographic" in q or "region" in q:
            return """
                SELECT g.country, ROUND(SUM(f.revenue),2) AS revenue
                FROM fact_sales f JOIN dim_geography g ON f.geo_key=g.geo_key
                GROUP BY g.country ORDER BY revenue DESC LIMIT 15
            """

        # Pattern 4: Customer Segments
        if "segment" in q or "customer type" in q:
            return """
                SELECT c.customer_segment, COUNT(*) AS customers, ROUND(SUM(f.revenue),2) AS revenue
                FROM fact_sales f JOIN dim_customer c ON f.customer_key=c.customer_key
                GROUP BY c.customer_segment ORDER BY revenue DESC
            """
            
        return None

    def query(self, question: str) -> dict:
        """Resolve a natural language question by routing to either SQL execution or semantic RAG.
        Returns a dictionary containing the answer, source data, and execution metadata."""
        sql = self._generate_sql(question)
        if sql:
            try:
                df  = self.conn.execute(sql).df()
                return {"source": "sql", "data": df.to_dict(orient="records"),
                        "query": sql.strip()}
            except Exception as e:
                log.warning(f"SQL failed ({e}), falling back to RAG")

        # Fallback → semantic RAG
        response = self.qe.query(question)
        return {"source": "rag", "answer": str(response),
                "nodes": [n.text for n in response.source_nodes[:3]]}


if __name__ == "__main__":
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    index = build_index(conn)
    router = NL2SQLRouter(conn, index)
    result = router.query("What are the top 5 products by revenue?")
    print(json.dumps(result, indent=2, default=str))
