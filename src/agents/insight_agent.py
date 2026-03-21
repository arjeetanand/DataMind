"""
DataMind — InsightAgent
Combines DataAgent results + LlamaIndex RAG + LLM to generate
grounded narrative insights and demand forecasts.
"""

import json
import requests
import duckdb
import logging
from pathlib import Path
import sys
import sys
# Legacy shim for LlamaIndex/LangChain version mismatch
try:
    import langchain_core.callbacks
    sys.modules['langchain.callbacks'] = langchain_core.callbacks
except ImportError:
    pass

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.agents.base_agent import BaseAgent, AgentRole, A2AMessage
from config.settings import (DB_PATH, LLM_PROVIDER, OLLAMA_BASE_URL,
                              OLLAMA_MODEL, COHERE_API_KEY, INDEX_DIR)

log = logging.getLogger(__name__)


class InsightAgent(BaseAgent):
    """
    Generates LLM-grounded insights from DataAgent output.
    Uses LlamaIndex RAG for context enrichment and Ollama/Cohere for generation.
    """

    def __init__(self):
        super().__init__(role=AgentRole.INSIGHT)
        self._index     = None
        self._router    = None
        self._conn      = duckdb.connect(str(DB_PATH), read_only=True)
        self._forecaster = None

    def _lazy_load_rag(self):
        """Lazy-load LlamaIndex index (heavy import — only when needed)."""
        if self._index is None:
            try:
                from src.rag.indexer import build_index, NL2SQLRouter
                self._index  = build_index(self._conn)
                self._router = NL2SQLRouter(self._conn, self._index)
                log.info("LlamaIndex RAG loaded")
            except Exception as e:
                log.warning(f"LlamaIndex unavailable: {e}. Using LLM-only mode.")

    def _call_llm(self, prompt: str) -> str:
        """Route to configured LLM provider."""
        if LLM_PROVIDER == "ollama":
            return self._call_ollama(prompt)
        if LLM_PROVIDER == "cohere":
            return self._call_cohere(prompt)
        if LLM_PROVIDER == "oci":
            return self._call_oci(prompt)
        return "[LLM not configured — set LLM_PROVIDER env var]"

    def _call_oci(self, prompt: str) -> str:
        try:
            import asyncio
            from utils.oci_llm_service import OCILLMService
            from utils.schema import LLMRequest
            service = OCILLMService()
            request = LLMRequest(query=prompt)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            response = loop.run_until_complete(service.invoke(request))
            loop.close()
            return response.content
        except Exception as e:
            return f"[OCI error: {e}]"

    def _call_ollama(self, prompt: str) -> str:
        try:
            r = requests.post(
                f"{OLLAMA_BASE_URL}/api/generate",
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
                timeout=60,
            )
            r.raise_for_status()
            return r.json().get("response", "")
        except Exception as e:
            return f"[Ollama error: {e}]"

    def _call_cohere(self, prompt: str) -> str:
        try:
            import cohere
            co = cohere.Client(COHERE_API_KEY)
            resp = co.generate(prompt=prompt, model="command-r-plus", max_tokens=512)
            return resp.generations[0].text
        except Exception as e:
            return f"[Cohere error: {e}]"

    def _forecast_insight(self, payload: dict) -> dict:
        """Run PyTorch LSTM forecast and return insight."""
        try:
            import pandas as pd
            from src.ml.forecaster import predict
            from src.warehouse.queries import daily_sales_series

            df = daily_sales_series(conn=self._conn)
            df = df.set_index("ds")
            forecast = predict(df)

            total = sum(forecast["forecast"])
            prompt = (
                f"Based on LSTM demand forecasting, the next {len(forecast['dates'])} days "
                f"are projected to generate £{total:,.2f} total revenue. "
                f"Day-by-day: {list(zip(forecast['dates'], forecast['forecast']))}. "
                f"Write a 3-sentence executive summary for a retail operations team."
            )
            narrative = self._call_llm(prompt)
            return {**forecast, "narrative": narrative, "type": "forecast"}
        except Exception as e:
            return {"error": str(e), "type": "forecast"}

    def _analyse_data(self, data_result: dict) -> dict:
        """Generate LLM narrative from DataAgent result."""
        self._lazy_load_rag()
        intent  = data_result.get("intent", "unknown")
        summary = data_result.get("summary", "")
        records = data_result.get("data", [])[:10]   # top 10 for context window

        rag_context = ""
        if self._router:
            try:
                rag_result  = self._router.query(f"Tell me about {intent.replace('_', ' ')}")
                rag_context = rag_result.get("answer", "") or ""
            except Exception:
                pass

        prompt = f"""You are a retail analytics expert. Analyse this data and provide actionable insights.

Intent: {intent}
Data Summary: {summary}
Top Records: {json.dumps(records[:5], default=str)}
Additional Context: {rag_context[:500]}

Provide:
1. Key finding (1 sentence)
2. Business implication (1 sentence)  
3. Recommended action (1 sentence)

Be specific with numbers. Format as plain text, no markdown."""

        narrative = self._call_llm(prompt)
        return {
            "intent"   : intent,
            "summary"  : summary,
            "narrative": narrative,
            "rag_used" : bool(rag_context),
            "type"     : "analysis",
        }

    def _execute(self, message: A2AMessage) -> dict:
        intent = message.intent
        payload = message.payload

        if intent == "forecast":
            return self._forecast_insight(payload)

        if intent == "analyse":
            data_result = payload.get("data_result", {})
            return self._analyse_data(data_result)

        if intent == "nl_query":
            self._lazy_load_rag()
            if self._router:
                return self._router.query(payload.get("question", ""))
            return {"error": "RAG not available"}

        raise ValueError(f"Unknown intent: {intent}")
