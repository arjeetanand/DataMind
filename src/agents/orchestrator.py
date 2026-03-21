"""
DataMind — LangGraph Orchestrator
Coordinates the full closed-loop pipeline:

  [START]
    → DataAgent   (fetch from warehouse)
    → InsightAgent (RAG + LLM analysis + forecast)
    → ActionAgent  (alerts + reports)
  [END]

LangGraph manages state, conditional routing, and retries.
"""

import logging
from typing import TypedDict, Optional, Literal
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))

from langgraph.graph import StateGraph, END

from src.agents.base_agent import A2AMessage, AgentRole
from src.agents.data_agent import DataAgent
from src.agents.insight_agent import InsightAgent
from src.agents.action_agent import ActionAgent

log = logging.getLogger(__name__)


# ── Graph State ───────────────────────────────────────────────────────────────
class PipelineState(TypedDict):
    query_intent   : str                     # initial user intent
    query_params   : dict
    data_result    : Optional[dict]
    insight_result : Optional[dict]
    forecast_result: Optional[dict]
    action_result  : Optional[dict]
    errors         : list
    trace          : list
    pipeline_mode  : str                     # "full" | "quick" | "forecast_only"


# ── Node Functions ─────────────────────────────────────────────────────────────
def data_node(state: PipelineState, agent: DataAgent) -> PipelineState:
    """Node 1: Fetch data from warehouse via DataAgent."""
    log.info(f"[DataNode] intent={state['query_intent']}")
    msg = A2AMessage(
        sender   = AgentRole.ORCHESTRATOR,
        receiver = AgentRole.DATA,
        intent   = state["query_intent"],
        payload  = state.get("query_params", {}),
    )
    response = agent.handle(msg)
    new_state = dict(state)
    if response.status.value == "success":
        new_state["data_result"] = response.result
        new_state["trace"].append(f"DataAgent ✓ | {response.result.get('summary', '')}")
    else:
        new_state["errors"].append(f"DataAgent: {response.error}")
        new_state["trace"].append(f"DataAgent ✗ | {response.error}")
    return new_state


def insight_node(state: PipelineState, agent: InsightAgent) -> PipelineState:
    """Node 2: Generate LLM-grounded insight from data."""
    log.info("[InsightNode] generating analysis")
    new_state = dict(state)

    if not state.get("data_result"):
        new_state["errors"].append("InsightAgent skipped: no data")
        return new_state

    msg = A2AMessage(
        sender   = AgentRole.DATA,
        receiver = AgentRole.INSIGHT,
        intent   = "analyse",
        payload  = {"data_result": state["data_result"]},
    )
    response = agent.handle(msg)
    if response.status.value == "success":
        new_state["insight_result"] = response.result
        new_state["trace"].append("InsightAgent ✓ | analysis complete")
    else:
        new_state["errors"].append(f"InsightAgent: {response.error}")
    return new_state


def forecast_node(state: PipelineState, agent: InsightAgent) -> PipelineState:
    """Node 3 (optional): Run PyTorch demand forecast."""
    log.info("[ForecastNode] running LSTM forecast")
    new_state = dict(state)
    msg = A2AMessage(
        sender   = AgentRole.ORCHESTRATOR,
        receiver = AgentRole.INSIGHT,
        intent   = "forecast",
        payload  = {},
    )
    response = agent.handle(msg)
    if response.status.value == "success":
        new_state["forecast_result"] = response.result
        new_state["trace"].append("ForecastNode ✓ | LSTM forecast complete")
    else:
        new_state["errors"].append(f"ForecastNode: {response.error}")
    return new_state


def action_node(state: PipelineState, agent: ActionAgent) -> PipelineState:
    """Node 4: Convert insights → autonomous actions."""
    log.info("[ActionNode] executing actions")
    new_state = dict(state)
    intent    = state["query_intent"]

    # Map pipeline intent to action intent
    if intent == "reorder_signals":
        action_intent = "reorder_alert"
        payload = {
            "data_result": state.get("data_result", {}),
            "insight"    : state.get("insight_result", {}),
        }
    elif intent == "revenue_trend":
        action_intent = "anomaly_alert"
        payload = {
            "data_result": state.get("data_result", {}),
            "insight"    : state.get("insight_result", {}),
        }
    else:
        action_intent = "executive_report"
        payload = {
            "insights" : state.get("insight_result", {}),
            "forecast" : state.get("forecast_result", {}),
        }

    msg = A2AMessage(
        sender   = AgentRole.INSIGHT,
        receiver = AgentRole.ACTION,
        intent   = action_intent,
        payload  = payload,
    )
    response = agent.handle(msg)
    if response.status.value == "success":
        new_state["action_result"] = response.result
        new_state["trace"].append(f"ActionAgent ✓ | {action_intent} complete")
    else:
        new_state["errors"].append(f"ActionAgent: {response.error}")
    return new_state


# ── Routing Logic ──────────────────────────────────────────────────────────────
def should_forecast(state: PipelineState) -> Literal["forecast", "action"]:
    """Run forecast if mode is 'full' or intent is revenue/executive."""
    if state.get("pipeline_mode") == "full":
        return "forecast"
    return "action"


# ── Graph Builder ─────────────────────────────────────────────────────────────
def build_pipeline() -> tuple:
    """Instantiate agents and build LangGraph pipeline. Returns (graph, agents)."""
    data_agent    = DataAgent()
    insight_agent = InsightAgent()
    action_agent  = ActionAgent()

    graph = StateGraph(PipelineState)

    # Add nodes with bound agents
    graph.add_node("data",     lambda s: data_node(s,    data_agent))
    graph.add_node("insight",  lambda s: insight_node(s, insight_agent))
    graph.add_node("forecast", lambda s: forecast_node(s, insight_agent))
    graph.add_node("action",   lambda s: action_node(s,  action_agent))

    # Edges
    graph.set_entry_point("data")
    graph.add_edge("data", "insight")
    graph.add_conditional_edges("insight", should_forecast,
                                {"forecast": "forecast", "action": "action"})
    graph.add_edge("forecast", "action")
    graph.add_edge("action", END)

    return graph.compile(), (data_agent, insight_agent, action_agent)


# ── Run Helper ────────────────────────────────────────────────────────────────
def run_pipeline(intent: str, params: dict = None, mode: str = "quick") -> dict:
    """
    High-level entry point.
    mode: 'quick' (data+insight+action) | 'full' (adds LSTM forecast)
    """
    compiled, _ = build_pipeline()

    initial_state: PipelineState = {
        "query_intent"   : intent,
        "query_params"   : params or {},
        "data_result"    : None,
        "insight_result" : None,
        "forecast_result": None,
        "action_result"  : None,
        "errors"         : [],
        "trace"          : [],
        "pipeline_mode"  : mode,
    }

    final_state = compiled.invoke(initial_state)
    log.info(f"Pipeline complete | errors={final_state['errors']} | "
             f"trace_steps={len(final_state['trace'])}")
    return final_state


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_pipeline("top_products", {"n": 10}, mode="quick")
    print("\n── Execution Trace ──")
    for step in result["trace"]:
        print(f"  {step}")
    print("\n── Action Result ──")
    import json
    print(json.dumps(result.get("action_result", {}), indent=2, default=str))
