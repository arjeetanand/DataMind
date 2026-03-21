"""
DataMind — Streamlit Dashboard
Real-time analytics UI backed by DuckDB warehouse + LangGraph agent pipeline.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import json
import requests
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

API_URL = "http://localhost:8000"

from config.settings import DB_PATH
from src.warehouse.queries import (monthly_revenue_trend, top_products, geo_revenue,
                                    reorder_signals, customer_rfm_summary, daily_sales_series)

st.set_page_config(
    page_title = "DataMind — Retail Intelligence",
    page_icon  = "🧠",
    layout     = "wide",
)

# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.image("https://img.icons8.com/fluency/96/artificial-intelligence.png", width=60)
st.sidebar.title("DataMind")
st.sidebar.caption("Autonomous Retail Analytics Intelligence")
st.sidebar.divider()

page = st.sidebar.radio("Navigate", [
    "📊 Revenue Overview",
    "🏆 Product Intelligence",
    "🗺️ Geographic Analysis",
    "👥 Customer Segments",
    "⚠️ Reorder Signals",
    "🔮 Demand Forecast",
    "🤖 Agent Pipeline",
    "💬 Natural Language Query",
])

# ── DB Connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return duckdb.connect(str(DB_PATH), read_only=True)

@st.cache_data(ttl=300)
def load_revenue():   return monthly_revenue_trend(get_conn())
@st.cache_data(ttl=300)
def load_products():  return top_products(n=30, conn=get_conn())
@st.cache_data(ttl=300)
def load_geo():       return geo_revenue(conn=get_conn())
@st.cache_data(ttl=300)
def load_rfm():       return customer_rfm_summary(conn=get_conn())
@st.cache_data(ttl=300)
def load_reorder():   return reorder_signals(conn=get_conn())
@st.cache_data(ttl=300)
def load_daily():     return daily_sales_series(get_conn())


# ═══════════════════════════════════════════════════════════════════════════════
if page == "📊 Revenue Overview":
    st.title("📊 Revenue Overview")
    df = load_revenue()
    conn = get_conn()
    fact_count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    agg_count  = conn.execute("SELECT COUNT(*) FROM agg_daily_sales").fetchone()[0]

    col0, col1, col2, col3, col4 = st.columns(5)
    col0.metric("fact_sales rows", f"{fact_count:,}")
    col1.metric("Total Revenue",  f"£{df['revenue'].sum():,.0f}")
    col2.metric("Total Orders",   f"{int(df['orders'].sum()):,}")
    col3.metric("Avg Monthly Rev",f"£{df['revenue'].mean():,.0f}")
    col4.metric("Peak Month",     df.loc[df['revenue'].idxmax(), 'month_name'])

    fig = px.bar(df, x="month_name", y="revenue", color="year",
                 title="Monthly Revenue by Year",
                 labels={"revenue": "Revenue (£)", "month_name": "Month"},
                 barmode="group", template="plotly_dark")
    st.plotly_chart(fig, width='stretch')

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=df.index, y=df["mom_growth_pct"],
                              mode="lines+markers", name="MoM Growth %",
                              line=dict(color="#00d4aa")))
    fig2.add_hline(y=0, line_dash="dash", line_color="red")
    fig2.update_layout(title="Month-over-Month Revenue Growth %", template="plotly_dark")
    st.plotly_chart(fig2, width='stretch')


elif page == "🏆 Product Intelligence":
    st.title("🏆 Product Intelligence")
    df = load_products()

    col1, col2 = st.columns([3, 1])
    with col2:
        n = st.slider("Top N products", 5, 30, 15)
    df = df.head(n)

    fig = px.bar(df, x="total_revenue", y="description", orientation="h",
                 color="price_band", title=f"Top {n} Products by Revenue",
                 template="plotly_dark", height=600,
                 labels={"total_revenue": "Revenue (£)", "description": "Product"})
    fig.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, width='stretch')

    st.dataframe(df[["description", "total_revenue", "total_units", "price_band", "revenue_rank"]],
                 width='stretch')


elif page == "🗺️ Geographic Analysis":
    st.title("🗺️ Geographic Analysis")
    df = load_geo()

    fig = px.choropleth(df, locations="country", locationmode="country names",
                        color="total_revenue", hover_name="country",
                        title="Revenue by Country",
                        color_continuous_scale="Viridis", template="plotly_dark")
    st.plotly_chart(fig, width='stretch')

    top10 = df.head(10)
    fig2 = px.bar(top10, x="country", y="total_revenue",
                  color="region", template="plotly_dark",
                  title="Top 10 Countries by Revenue",
                  labels={"total_revenue": "Revenue (£)"})
    st.plotly_chart(fig2, width='stretch')


elif page == "👥 Customer Segments":
    st.title("👥 Customer Segments (RFM)")
    df = load_rfm()

    fig = px.pie(df, names="customer_segment", values="total_revenue",
                 title="Revenue Share by Customer Segment",
                 color_discrete_sequence=["#00d4aa","#0099ff","#ff6b35"],
                 template="plotly_dark")
    st.plotly_chart(fig, width='stretch')
    st.dataframe(df, width='stretch')


elif page == "⚠️ Reorder Signals":
    st.title("⚠️ Reorder Signals")
    lookback = st.slider("Lookback window (days)", 14, 60, 30)
    df = reorder_signals(lookback_days=lookback, conn=get_conn())

    if df.empty:
        st.success("✅ No reorder signals detected.")
    else:
        st.error(f"🚨 {len(df)} products require attention")
        fig = px.bar(df.head(20), x="description", y="unit_change_pct",
                     color="signal", template="plotly_dark",
                     title="Unit Sales Change % (Recent vs Prior Period)",
                     labels={"unit_change_pct": "Change %", "description": "Product"})
        st.plotly_chart(fig, width='stretch')
        st.dataframe(df, width='stretch')


elif page == "🔮 Demand Forecast":
    st.title("🔮 Demand Forecast (LSTM)")
    st.info("Ensure model is trained first: `python -m src.ml.forecaster`")
    try:
        from src.ml.forecaster import predict
        df = load_daily().set_index("ds")
        result = predict(df)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=result["dates"], y=result["forecast"],
            mode="lines+markers", name="Forecast", line=dict(color="#00d4aa", width=2)))
        fig.add_trace(go.Scatter(
            x=result["dates"] + result["dates"][::-1],
            y=result["upper_ci"] + result["lower_ci"][::-1],
            fill="toself", fillcolor="rgba(0,212,170,0.15)",
            line=dict(color="rgba(0,0,0,0)"), name="90% CI"))
        fig.update_layout(title="7-Day Revenue Forecast with Confidence Interval",
                          template="plotly_dark",
                          xaxis_title="Date", yaxis_title="Projected Revenue (£)")
        st.plotly_chart(fig, width='stretch')

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Projected", f"£{sum(result['forecast']):,.2f}")
        col2.metric("Peak Day", result["dates"][result["forecast"].index(max(result["forecast"]))])
        col3.metric("Forecast Horizon", f"{len(result['dates'])} days")

    except Exception as e:
        st.error(f"Forecast error: {e}")


elif page == "🤖 Agent Pipeline":
    st.title("🤖 LangGraph Agent Pipeline")
    st.caption("DataAgent → InsightAgent → ActionAgent (A2A Protocol)")

    col1, col2 = st.columns(2)
    with col1:
        intent = st.selectbox("Select Intent", [
            "revenue_trend", "top_products", "geo_revenue",
            "reorder_signals", "rfm_summary"])
    with col2:
        mode = st.radio("Pipeline Mode", ["quick", "full"], horizontal=True)

    if st.button("🚀 Run Pipeline", type="primary"):
        with st.spinner("Agents running..."):
            try:
                # Call FastAPI backend
                payload = {"intent": intent, "mode": mode}
                response = requests.post(f"{API_URL}/pipeline/run", json=payload)
                response.raise_for_status()
                result = response.json()

                st.subheader("Execution Trace")
                for step in result.get("trace", []):
                    st.markdown(f"✅ `{step}`")

                if result.get("insight_result", {}).get("narrative"):
                    st.subheader("💡 Insight")
                    st.write(result["insight_result"]["narrative"])

                if result.get("action_result"):
                    st.subheader("⚡ Action Taken")
                    st.json(result["action_result"])

                if result.get("errors"):
                    st.warning(f"Errors: {result['errors']}")
            except Exception as e:
                st.error(f"Pipeline error: {e}")


elif page == "💬 Natural Language Query":
    st.title("💬 Natural Language Query")
    if "question_input" not in st.session_state:
        st.session_state["question_input"] = ""

    question = st.text_input("Ask anything about your retail data:",
                              value=st.session_state["question_input"],
                              placeholder="e.g. What are the top 5 products by revenue?",
                              key="main_query")
    
    # Update session state when typing manually
    st.session_state["question_input"] = question
    
    st.markdown("💡 **Suggestions:**")
    cols = st.columns(4)
    suggestions = ["Top 5 products by revenue", "Monthly sales trend", 
                   "Countries with highest revenue", "Customer segment breakdown"]
    for i, s in enumerate(suggestions):
        if cols[i % 4].button(s, key=f"sugg_{i}"):
            st.session_state["question_input"] = s
            st.rerun()

    if question and st.button("Ask"):
        with st.spinner("Querying warehouse..."):
            try:
                # Call FastAPI backend (POST with JSON body)
                payload = {"question": question}
                response = requests.post(f"{API_URL}/query/nl", json=payload)
                response.raise_for_status()
                data = response.json()
                result = data.get("result", {})

                st.subheader("Answer")
                if result.get("source") == "sql":
                    st.dataframe(pd.DataFrame(result["data"]), width='stretch')
                    with st.expander("SQL Query"):
                        st.code(result.get("query", ""), language="sql")
                else:
                    st.write(result.get("answer", ""))
            except Exception as e:
                st.error(str(e))
