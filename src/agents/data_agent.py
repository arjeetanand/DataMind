"""
DataMind — DataAgent
Pulls structured data from DuckDB warehouse via the SQL query library.
Intents: revenue_trend | top_products | geo_revenue | daily_series | reorder_signals | rfm_summary
"""

import duckdb
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.agents.base_agent import BaseAgent, AgentRole, A2AMessage
from src.warehouse import queries
from config.settings import DB_PATH


class DataAgent(BaseAgent):
    """
    Retrieves structured data from DuckDB warehouse.
    Acts as the sole gateway to the dimensional model.
    """

    INTENT_MAP = {
        "revenue_trend"   : "monthly_revenue_trend",
        "top_products"    : "top_products",
        "geo_revenue"     : "geo_revenue",
        "daily_series"    : "daily_sales_series",
        "reorder_signals" : "reorder_signals",
        "rfm_summary"     : "customer_rfm_summary",
        "cohort_retention": "cohort_retention",
    }

    def __init__(self, db_path: Path = DB_PATH):
        """Initialise the DataAgent with a read-only DuckDB warehouse connection.
        Sets up the agent's role and prepares it for SQL-based retrieval."""
        super().__init__(role=AgentRole.DATA)
        self.conn = duckdb.connect(str(db_path), read_only=True)

    def _execute(self, message: A2AMessage) -> dict:
        """Process an incoming data retrieval request by mapping intent to SQL queries.
        Returns a serialised result containing raw records and a concise summary."""
        intent = message.intent

        if intent not in self.INTENT_MAP:
            raise ValueError(f"Unknown intent '{intent}'. Valid: {list(self.INTENT_MAP)}")

        fn_name = self.INTENT_MAP[intent]
        fn      = getattr(queries, fn_name)

        # Pass optional params
        if intent == "top_products":
            df = fn(n=params.get("n", 20), conn=self.conn)
        elif intent == "daily_series":
            df = fn(stock_code=params.get("stock_code"), conn=self.conn)
        elif intent == "reorder_signals":
            df = fn(lookback_days=params.get("lookback_days", 30), conn=self.conn)
        else:
            df = fn(conn=self.conn)

        self.log.info(f"Intent '{intent}' returned {len(df)} rows")
        return {
            "intent"  : intent,
            "row_count": len(df),
            "columns" : list(df.columns),
            "data"    : df.to_dict(orient="records"),
            "summary" : self._summarise(intent, df),
        }

    def _summarise(self, intent: str, df) -> str:
        """Generate a human-readable 1-line statistical summary of the retrieved data.
        Provides high-level context for downstream analysis and insight generation."""
        if df.empty:
            return "No data available."
        if intent == "revenue_trend":
            total = df["revenue"].sum()
            return f"Total revenue across {len(df)} months: £{total:,.2f}"
        if intent == "top_products":
            return f"Top product: '{df.iloc[0]['description']}' with £{df.iloc[0]['total_revenue']:,.2f}"
        if intent == "geo_revenue":
            return f"Top country: {df.iloc[0]['country']} with £{df.iloc[0]['total_revenue']:,.2f}"
        if intent == "reorder_signals":
            return f"{len(df)} products flagged for reorder"
        if intent == "rfm_summary":
            high = df[df["customer_segment"] == "HIGH"]
            if not high.empty:
                return f"HIGH-value customers: {int(high['num_customers'].values[0])}"
        return f"{len(df)} rows retrieved."
