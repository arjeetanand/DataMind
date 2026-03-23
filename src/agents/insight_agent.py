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
        """Initialise the InsightAgent and prepare the RAG and forecasting connectors.
        Sets up the DuckDB warehouse connection and placeholders for heavy indexing."""
        super().__init__(role=AgentRole.INSIGHT)

    def _lazy_load_rag(self):
        """Perform a lazy-load of the LlamaIndex RAG components to save startup time.
        Instantiates the NL2SQL router only upon the first analytics requested."""
        if self._index is None:
            try:
                from src.rag.indexer import build_index, NL2SQLRouter
                self._index  = build_index(self._conn)
                self._router = NL2SQLRouter(self._conn, self._index)
                log.info("LlamaIndex RAG loaded")
            except Exception as e:
                log.warning(f"LlamaIndex unavailable: {e}. Using LLM-only mode.")

    def _call_llm(self, prompt: str) -> str:
        """Route the generation prompt to the configured LLM provider (Ollama or Cohere).
        Ensures a consistent interface for narrative generation across different providers."""
        if LLM_PROVIDER == "ollama":
            return self._call_ollama(prompt)
        if LLM_PROVIDER == "cohere":
            return self._call_cohere(prompt)
        if LLM_PROVIDER == "oci":
            return self._call_oci(prompt)
        return "[LLM not configured — set LLM_PROVIDER env var]"

    def _call_oci(self, prompt: str) -> str:
        """Invoke the OCI GenAI service for high-performance enterprise generation.
        Handles asynchronous communication with the OCI LLM endpoint via a local loop."""
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
        """Send a generation request to the local Ollama instance.
        Optimized for local-first, privacy-conscious AI reasoning on retail data."""
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
        """Invoke the Cohere API for cloud-based large language model generation.
        Uses the 'command-r-plus' model for sophisticated business logic narratives."""
        try:
            import cohere
            co = cohere.Client(COHERE_API_KEY)
            resp = co.generate(prompt=prompt, model="command-r-plus", max_tokens=512)
            return resp.generations[0].text
        except Exception as e:
            return f"[Cohere error: {e}]"

    def _forecast_insight(self, payload: dict) -> dict:
        """Coordinate the PyTorch LSTM forecast and generate a business-friendly summary.
        Links raw predictive values with an LLM narrative for executive understanding."""
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
        """Generate a grounded narrative insight from DataAgent results using RAG context.
        Synthesizes raw database records, statistical summaries, and external index metadata."""
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
        """Main dispatcher for InsightAgent, handling forecasting and analytics intents.
        Routes messages to the appropriate internal logic based on the user's quest."""
        intent = message.intent

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
