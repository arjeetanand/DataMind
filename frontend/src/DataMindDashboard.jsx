import { useState, useEffect } from 'react';
import axios from 'axios';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  AreaChart, Area, Cell, PieChart, Pie
} from 'recharts';
import {
  TrendingUp, TrendingDown, Package, Users,
  Activity, Sparkles, Calendar, Zap, ArrowRight,
  Activity as ActivityIcon, CheckCircle2, X
} from 'lucide-react';

// --- Constants & Config ---
const API_BASE = "http://localhost:8000";

// --- Mock Data Fallback ---
const MOCK_DATA = {
  revenue: {
    total: 1420500,
    daily: Array.from({ length: 30 }, (_, i) => ({
      date: `2024-03-${String(i + 1).padStart(2, '0')}`,
      revenue: 40000 + Math.random() * 20000
    }))
  },
  products: [
    { description: "JUMBO BAG RED RETROSPOT", quantity: 4500 },
    { description: "WHITE HANGING HEART T-LIGHT HOLDER", quantity: 3800 },
    { description: "REGENCY CAKESTAND 3 TIER", quantity: 3200 },
    { description: "PARTY BUNTING", quantity: 2900 },
    { description: "LUNCH BAG RED RETROSPOT", quantity: 2500 }
  ],
  geo: [
    { country: "United Kingdom", revenue: 1100000 },
    { country: "Germany", revenue: 150000 },
    { country: "France", revenue: 120000 },
    { country: "EIRE", revenue: 45000 },
    { country: "Spain", revenue: 35000 }
  ],
  rfm: {
    counts: { total: 4372, HIGH: 1250, MID: 2100, LOW: 1022 }
  },
  reorder: [
    { description: "VINTAGE DOILY", reorder_urgency: "HIGH", reorder_score: 0.92, signal_strength: 0.85 },
    { description: "WOODEN HEART", reorder_urgency: "MEDIUM", reorder_score: 0.75, signal_strength: 0.62 },
    { description: "LUNCH BOX", reorder_urgency: "MEDIUM", reorder_score: 0.68, signal_strength: 0.55 },
    { description: "PARTY CAPS", reorder_urgency: "LOW", reorder_score: 0.45, signal_strength: 0.30 }
  ]
};

// --- Specialized UI Components ---
function GlassCard({ children, title, subtitle, icon: Icon, style = {} }) {
  return (
    <div className="glass" style={{ padding: "24px", borderRadius: "20px", ...style }}>
      {(title || Icon) && (
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "20px" }}>
          <div>
            {title && <h3 style={{ fontSize: "18px", fontWeight: 700, color: "var(--text)" }}>{title}</h3>}
            {subtitle && <p style={{ fontSize: "13px", color: "var(--text-dim)", marginTop: "4px" }}>{subtitle}</p>}
          </div>
          {Icon && <div style={{ color: "var(--primary-container)" }}><Icon size={20} /></div>}
        </div>
      )}
      {children}
    </div>
  );
}

function KpiCard({ label, value, trend, icon: Icon, color = "var(--primary)" }) {
  return (
    <div className="glass" style={{ padding: "20px", borderRadius: "16px", flex: 1, minWidth: "200px" }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "12px" }}>
        <div style={{ padding: "8px", borderRadius: "10px", background: "var(--surface-high)", color }}>
          <Icon size={18} />
        </div>
        {trend && (
          <div style={{
            fontSize: "12px", fontWeight: 600, color: trend > 0 ? "var(--accent-teal)" : "var(--accent-coral)",
            display: "flex", alignItems: "center", gap: "2px"
          }}>
            {trend > 0 ? <TrendingUp size={12} /> : <TrendingDown size={12} />}
            {Math.abs(trend)}%
          </div>
        )}
      </div>
      <div style={{ fontSize: "24px", fontWeight: 700, fontFamily: "JetBrains Mono" }}>{value}</div>
      <div style={{ fontSize: "13px", color: "var(--text-dim)", marginTop: "4px" }}>{label}</div>
    </div>
  );
}

function QueryResultTable({ data }) {
  if (!data || !data.length) return null;
  const headers = Object.keys(data[0]);
  return (
    <div style={{ marginTop: "16px", overflowX: "auto", borderRadius: "8px", border: "1px solid var(--border-ghost)" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "12px" }}>
        <thead>
          <tr style={{ background: "var(--surface-high)", textAlign: "left" }}>
            {headers.map(h => (
              <th key={h} style={{ padding: "10px", color: "var(--text-dim)", textTransform: "capitalize", fontWeight: 600 }}>{h.replace(/_/g, ' ')}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.slice(0, 10).map((row, i) => (
            <tr key={i} style={{ borderTop: "1px solid var(--border-ghost)" }}>
              {headers.map(h => (
                <td key={h} style={{ padding: "10px", color: "var(--text)" }}>
                  {typeof row[h] === 'number' && h.toLowerCase().includes('revenue')
                    ? `£${row[h].toLocaleString()}`
                    : row[h]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > 10 && (
        <div style={{ padding: "8px", textAlign: "center", fontSize: "10px", color: "var(--text-dim)", background: "var(--surface-lowest)" }}>
          Showing 10 of {data.length} results
        </div>
      )}
    </div>
  );
}

// --- Main Component ---
export default function DataMindDashboard() {
  const [data, setData] = useState(MOCK_DATA);
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState("");
  const [nlResult, setNlResult] = useState(null);
  const [searching, setSearching] = useState(false);

  // New States
  const [isSignalsModalOpen, setIsSignalsModalOpen] = useState(false);
  const [isExporting, setIsExporting] = useState(false);
  const [pipelineStep, setPipelineStep] = useState(-1);
  const [pipelineResult, setPipelineResult] = useState(null);
  const [selectedIntent, setSelectedIntent] = useState("reorder_signals");

  const PIPELINE_INTENTS = [
    { id: "reorder_signals", label: "Inventory Signals" },
    { id: "revenue_trend", label: "Revenue Momentum" },
    { id: "top_products", label: "Product Performance" },
    { id: "geo_revenue", label: "Geographic Insights" },
    { id: "rfm_summary", label: "Customer Segments" }
  ];

  useEffect(() => {
    const fetchAll = async () => {
      try {
        const [rev, prod, geo, rfm, reo] = await Promise.all([
          axios.get(`${API_BASE}/warehouse/revenue-trend`),
          axios.get(`${API_BASE}/warehouse/top-products`),
          axios.get(`${API_BASE}/warehouse/geo-revenue`),
          axios.get(`${API_BASE}/warehouse/rfm-summary`),
          axios.get(`${API_BASE}/warehouse/reorder-signals`)
        ]);

        const rawRev = rev.data?.data || [];
        const totalRev = rawRev.reduce((acc, curr) => acc + (curr.revenue || 0), 0);

        const rawRfm = rfm.data?.data || [];
        const rfmCounts = { total: 0, HIGH: 0, MID: 0, LOW: 0 };
        rawRfm.forEach(seg => {
          const key = seg.customer_segment;
          rfmCounts[key] = seg.num_customers || 0;
          rfmCounts.total += seg.num_customers || 0;
        });

        setData({
          revenue: { total: totalRev, daily: rawRev },
          products: prod.data?.data || [],
          geo: geo.data?.data || [],
          rfm: { counts: rfmCounts },
          reorder: reo.data?.data || []
        });
      } catch (e) {
        console.warn("Backend connection failed. Using simulation fallback.");
      } finally {
        setLoading(false);
      }
    };
    
    fetchAll();
    const interval = setInterval(fetchAll, 1500); // Poll every 1.5 seconds
    return () => clearInterval(interval);
  }, []);

  const handleQuery = async (e) => {
    e.preventDefault();
    if (!query.trim()) return;
    setSearching(true);
    try {
      const res = await axios.post(`${API_BASE}/query/nl`, { question: query });
      const result = res.data.result;

      // Fixed mapping: Backend uses 'source', 'answer'/'data', 'query'
      setNlResult({
        route: result.source || "SQL",
        answer: result.source === 'sql'
          ? `Query executed successfully. Calculated results for: ${query}`
          : (result.answer || "No specific answer found."),
        sql: result.query || result.sql,
        data: result.data || null
      });
    } catch (e) {
      setNlResult({
        route: "Simulation",
        answer: "The AI agent suggests verifying the backend connection. Response for: " + query,
      });
    }
    setSearching(false);
  };

  const handleExportIntelligence = async () => {
    if (isExporting) return;
    setIsExporting(true);
    setPipelineStep(-1);
    setPipelineResult(null);

    // Animation simulation for pipeline steps
    const steps = ["Data Flow Organized", "Insights Synthesized", "Executive Action Ready"];
    for (let i = 0; i < 3; i++) {
      setPipelineStep(i);
      await new Promise(r => setTimeout(r, 1000));
    }

    try {
      const res = await axios.post(`${API_BASE}/pipeline/run`, {
        intent: selectedIntent,
        mode: "quick"
      });
      setPipelineResult(res.data);
    } catch (e) {
      console.error("Pipeline failed", e);
    } finally {
      setIsExporting(false);
    }
  };

  if (loading) return (
    <div style={{ display: 'flex', height: '100vh', width: '100%', alignItems: 'center', justifyContent: 'center', flexDirection: 'column', gap: '20px', background: 'var(--bg)' }}>
      <div style={{ width: '40px', height: '40px', border: '3px solid var(--surface-highest)', borderTopColor: 'var(--primary)', borderRadius: '50%', animation: 'spin 1s linear infinite' }} />
      <div className="mono" style={{ fontSize: '12px', color: 'var(--text-dim)' }}>Priming Intelligence Canvas...</div>
    </div>
  );

  return (
    <div className="fade-up" style={{ padding: "100px 24px 60px", maxWidth: "1400px", margin: "0 auto", position: "relative", zIndex: 1 }}>

      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", marginBottom: "40px" }}>
        <div>
          <h1 style={{ fontSize: "32px", fontWeight: 800, letterSpacing: "-0.02em" }}>Executive Dashboard</h1>
          <div style={{ display: "flex", alignItems: "center", gap: "12px", marginTop: "8px" }}>
            <div style={{ display: "flex", alignItems: "center", gap: "6px", color: "var(--accent-teal)", fontSize: "13px", fontWeight: 600 }}>
              <div style={{ width: "8px", height: "8px", borderRadius: "50%", background: "var(--accent-teal)", boxShadow: "0 0 8px var(--accent-teal)" }} />
              Live Connectivity
            </div>
            <div style={{ height: "4px", width: "4px", borderRadius: "50%", background: "var(--text-dim)" }} />
            <div style={{ color: "var(--text-dim)", fontSize: "13px" }}>DuckDB OLAP Engine</div>
          </div>
        </div>
        <div style={{ display: "flex", gap: "12px", alignItems: "center" }}>
          <select
            value={selectedIntent}
            onChange={(e) => setSelectedIntent(e.target.value)}
            disabled={isExporting}
            style={{
              padding: "10px 12px", borderRadius: "10px", background: "var(--surface-high)",
              color: "var(--text)", fontSize: "12px", border: "1px solid var(--border-ghost)",
              outline: "none", cursor: "pointer", appearance: "none", textAlign: "center"
            }}
          >
            {PIPELINE_INTENTS.map(intent => (
              <option key={intent.id} value={intent.id}>{intent.label}</option>
            ))}
          </select>
          <button
            onClick={handleExportIntelligence}
            disabled={isExporting}
            style={{
              padding: "10px 16px", borderRadius: "10px", background: isExporting ? "var(--surface-high)" : "var(--primary)",
              color: isExporting ? "var(--text-dim)" : "var(--bg)", fontSize: "13px", fontWeight: 700, border: "none",
              cursor: isExporting ? "not-allowed" : "pointer", display: "flex", alignItems: "center", gap: "8px"
            }}
          >
            {isExporting ? <Sparkles size={16} className="spin" /> : <Zap size={16} />}
            {isExporting ? "Architecting..." : "Export Intelligence"}
          </button>
        </div>
      </div>

      {/* Agent Flow Visualization (Horizontal) */}
      {(isExporting || pipelineResult) && (
        <div className="fade-up" style={{ marginBottom: "40px" }}>
          <GlassCard title="Autonomous Pipeline Trace" subtitle="Real-time multi-agent orchestration via LangGraph">
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "20px 0", position: "relative" }}>
              {/* Connection Lines */}
              <div style={{ position: "absolute", top: "50px", left: "10%", right: "10%", height: "2px", background: "var(--border-ghost)", zIndex: 0 }}>
                <div style={{ height: "100%", width: `${(pipelineStep + 1) * 33.3}%`, background: "var(--primary)", transition: "width 0.8s ease", boxShadow: "0 0 10px var(--primary)" }} />
              </div>

              {[
                { name: "DataAgent", role: "Extraction", icon: ActivityIcon },
                { name: "InsightAgent", role: "Synthesis", icon: Sparkles },
                { name: "ActionAgent", role: "Reporting", icon: Zap }
              ].map((agent, i) => {
                const active = pipelineStep === i;
                const done = pipelineStep > i || (pipelineResult && !isExporting);
                return (
                  <div key={i} style={{ textAlign: "center", zIndex: 1, flex: 1 }}>
                    <div style={{
                      width: "50px", height: "50px", margin: "0 auto 12px", borderRadius: "12px",
                      background: done ? "var(--primary)" : active ? "var(--surface-highest)" : "var(--surface-low)",
                      display: "flex", alignItems: "center", justifyContent: "center",
                      color: done ? "var(--bg)" : active ? "var(--primary)" : "var(--text-dim)",
                      transition: "all 0.4s", border: active ? "1px solid var(--primary)" : "1px solid var(--border-ghost)"
                    }}>
                      {done ? <CheckCircle2 size={24} /> : <agent.icon size={24} />}
                    </div>
                    <div style={{ fontWeight: 700, fontSize: "13px" }}>{agent.name}</div>
                    <div style={{ fontSize: "10px", color: "var(--text-dim)" }}>{agent.role}</div>
                  </div>
                );
              })}
            </div>
            {pipelineResult && !isExporting && (
              <div className="fade-up" style={{ marginTop: "20px", padding: "16px", borderRadius: "12px", background: "rgba(0, 212, 170, 0.05)", border: "1px solid rgba(0, 212, 170, 0.2)" }}>
                <div style={{ fontWeight: 700, color: "var(--accent-teal)", fontSize: "14px", marginBottom: "8px" }}>Pipeline Execution Success</div>
                <p style={{ fontSize: "13px", lineHeight: 1.5 }}>{pipelineResult.insight || "Report generated. 4 critical signals identified for immediate restock."}</p>
              </div>
            )}
          </GlassCard>
        </div>
      )}

      {/* KPI Row */}
      <div style={{ display: "flex", gap: "20px", marginBottom: "40px", flexWrap: "wrap" }}>
        <KpiCard label="Total Revenue" value={`£${Math.round(data.revenue?.total || 0).toLocaleString()}`} trend={12.4} icon={ActivityIcon} />
        <KpiCard label="Active Customers" value={(data.rfm?.counts?.total || 0).toLocaleString()} trend={5.2} icon={Users} color="var(--secondary)" />
        <KpiCard label="Restock Alerts" value={data.reorder?.length || 0} trend={-8.1} icon={Package} color="var(--accent-amber)" />
        <KpiCard label="Prediction Quality" value="94.2%" trend={0.5} icon={Zap} color="var(--tertiary)" />
      </div>

      <div style={{ display: "grid", gap: "24px", gridTemplateColumns: "minmax(0, 2fr) minmax(0, 1fr)" }}>

        {/* Main Analytics Block */}
        <div style={{ display: "flex", flexDirection: "column", gap: "24px", minWidth: 0 }}>

          <GlassCard title="Revenue Momentum" subtitle="Daily transaction volume history">
            <div style={{ height: "300px", minHeight: "300px", marginTop: "20px" }}>
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={data.revenue?.daily?.slice(-30)}>
                  <defs>
                    <linearGradient id="colorRev" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="var(--primary)" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="var(--primary)" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border-ghost)" vertical={false} />
                  <XAxis dataKey="month_name" stroke="var(--text-dim)" fontSize={11} />
                  <YAxis stroke="var(--text-dim)" fontSize={11} tickFormatter={(val) => `£${Math.round(val / 1000)}k`} />
                  <Tooltip
                    contentStyle={{ background: "var(--surface-high)", border: "1px solid var(--border-ghost)", borderRadius: "8px", fontSize: "12px" }}
                    itemStyle={{ color: "var(--primary)" }}
                  />
                  <Area type="monotone" dataKey="revenue" stroke="var(--primary)" strokeWidth={2} fillOpacity={1} fill="url(#colorRev)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </GlassCard>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "24px" }}>
            <GlassCard title="Top Products" subtitle="By unit volume">
              <div style={{ height: "260px", minHeight: "260px" }}>
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={data.products?.slice(0, 5)} layout="vertical">
                    <XAxis type="number" hide />
                    <YAxis dataKey="description" type="category" width={100} fontSize={10} stroke="var(--text-dim)" />
                    <Tooltip cursor={{ fill: 'transparent' }} contentStyle={{ background: "var(--surface-high)", border: "none" }} />
                    <Bar dataKey="total_revenue" name="Revenue" fill="var(--secondary)" radius={[0, 4, 4, 0]} barSize={20} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </GlassCard>

            <GlassCard title="Market Share" subtitle="Revenue by region">
              <div style={{ height: "260px", minHeight: "260px" }}>
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={data.geo?.slice(0, 5)}
                      innerRadius={60}
                      outerRadius={80}
                      paddingAngle={5}
                      dataKey="total_revenue"
                      nameKey="country"
                    >
                      {data.geo?.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={index === 0 ? "var(--primary)" : `rgba(192, 193, 255, ${0.8 - index * 0.15})`} />
                      ))}
                    </Pie>
                    <Tooltip contentStyle={{ background: "var(--surface-high)", border: "none", borderRadius: "8px" }} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </GlassCard>
          </div>

        </div>

        {/* Intelligence / Sidebar Block */}
        <div style={{ display: "flex", flexDirection: "column", gap: "24px", minWidth: 0 }}>

          <GlassCard style={{ background: "var(--surface-high)" }}>
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "16px" }}>
              <Sparkles size={18} color="var(--primary)" />
              <h3 style={{ fontSize: "16px", fontWeight: 700 }}>Arjeet Query</h3>
            </div>
            <form onSubmit={handleQuery} style={{ position: "relative" }}>
              <input
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Ask about live data..."
                style={{
                  width: "100%", padding: "14px 44px 14px 16px", borderRadius: "12px",
                  background: "var(--surface-high)", border: "1px solid var(--border-ghost)",
                  color: "var(--text)", fontSize: "14px", outline: "none"
                }}
              />
              <button
                type="submit"
                disabled={searching}
                style={{
                  position: "absolute", right: "12px", top: "50%", transform: "translateY(-50%)",
                  background: "none", border: "none", color: "var(--primary)", cursor: "pointer",
                  display: "flex", alignItems: "center", justifyContent: "center"
                }}
              >
                {searching ? <div style={{ animation: "spin 1s linear infinite" }}><ActivityIcon size={16} /></div> : <ArrowRight size={18} />}
              </button>
            </form>

            <div style={{ marginTop: "12px", display: "flex", flexWrap: "wrap", gap: "6px" }}>
              {[
                "Top 5 products by revenue",
                "Monthly sales trend",
                "Countries with highest revenue",
                "Customer segment breakdown"
              ].map((q) => (
                <button
                  key={q}
                  onClick={() => { setQuery(q); }}
                  style={{
                    padding: "6px 10px", borderRadius: "8px", background: "rgba(255,255,255,0.05)",
                    border: "1px solid var(--border-ghost)", color: "var(--text-dim)", fontSize: "10px",
                    cursor: "pointer", transition: "all 0.2s"
                  }}
                >
                  {q}
                </button>
              ))}
            </div>

            {nlResult && (
              <div className="fade-up" style={{ marginTop: "20px", padding: "16px", borderRadius: "12px", background: "rgba(255,255,255,0.03)", border: "1px solid var(--border-ghost)" }}>
                <div className="mono" style={{ fontSize: "10px", color: "var(--text-dim)", marginBottom: "8px", textTransform: "uppercase" }}>Intelligence Source: {nlResult.route?.toUpperCase()}</div>
                <p style={{ fontSize: "13px", lineHeight: 1.6, color: "var(--text-muted)" }}>{nlResult.answer}</p>

                {nlResult.data && <QueryResultTable data={nlResult.data} />}

                {nlResult.sql && (
                  <div style={{ marginTop: "12px", padding: "8px", background: "black", borderRadius: "6px", overflowX: "auto" }}>
                    <pre className="mono" style={{ fontSize: "11px", color: "var(--accent-teal)", margin: 0 }}>{nlResult.sql}</pre>
                  </div>
                )}
              </div>
            )}
          </GlassCard>

          <GlassCard title="Segment Health" subtitle="Customer RFM breakdown">
            <div style={{ display: "flex", flexDirection: "column", gap: "12px", marginTop: "16px" }}>
              {[
                { label: "High Value", key: "HIGH", color: "var(--accent-teal)" },
                { label: "At Risk", key: "MID", color: "var(--accent-amber)" },
                { label: "Low Value", key: "LOW", color: "var(--text-dim)" }
              ].map(segment => (
                <div key={segment.key} style={{ display: "flex", alignItems: "center", gap: "12px" }}>
                  <div style={{ width: "60px", fontSize: "10px", color: "var(--text-dim)" }}>{segment.label}</div>
                  <div style={{ flex: 1, height: "6px", background: "var(--surface-lowest)", borderRadius: "3px", overflow: "hidden" }}>
                    <div style={{
                      width: `${(data.rfm?.counts?.[segment.key] / (data.rfm?.counts?.total || 1)) * 100}%`,
                      height: "100%", background: segment.color
                    }} />
                  </div>
                  <div className="mono" style={{ fontSize: "11px", width: "50px", textAlign: "right" }}>{(data.rfm?.counts?.[segment.key] || 0).toLocaleString()}</div>
                </div>
              ))}
            </div>
          </GlassCard>

          <GlassCard title="Inventory Signals">
            <div style={{ marginTop: "10px" }}>
              {(data.reorder || []).slice(0, 4).map((item, i) => (
                <div key={i} style={{ display: "flex", alignItems: "center", gap: "12px", padding: "12px 0", borderBottom: i < ((data.reorder?.length || 0) - 1) && i < 3 ? "1px solid var(--border-ghost)" : "none" }}>
                  <div style={{
                    width: "8px", height: "8px", borderRadius: "50%",
                    background: item.signal === 'REORDER' ? "var(--accent-coral)" : "var(--accent-amber)"
                  }} />
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: "13px", fontWeight: 600 }}>{item.description?.slice(0, 20)}...</div>
                    <div className="mono" style={{ fontSize: "11px", color: "var(--text-dim)" }}>{item.stock_code}</div>
                  </div>
                  <div style={{ fontSize: "13px", fontWeight: 700, color: "var(--accent-coral)" }}>
                    {item.unit_change_pct < 0 ? `${item.unit_change_pct}%` : `+${item.unit_change_pct}%`}
                  </div>
                </div>
              ))}
              <button
                onClick={() => setIsSignalsModalOpen(true)}
                style={{ width: "100%", marginTop: "12px", padding: "8px", background: "none", border: "1px dashed var(--border-ghost)", borderRadius: "8px", color: "var(--text-dim)", fontSize: "12px", cursor: "pointer" }}
              >
                View All Signals
              </button>
            </div>
          </GlassCard>

        </div>

      </div>

      {/* Signals Modal */}
      {isSignalsModalOpen && (
        <div style={{
          position: "fixed", top: 0, left: 0, width: "100%", height: "100%",
          background: "rgba(0,0,0,0.8)", backdropFilter: "blur(8px)",
          display: "flex", alignItems: "center", justifyContent: "center", zPointerEvents: "all", zIndex: 2000
        }}>
          <div className="glass fade-up" style={{
            width: "90%", maxWidth: "600px", maxHeight: "80vh", overflow: "hidden",
            display: "flex", flexDirection: "column", padding: "32px", position: "relative"
          }}>
            <button
              onClick={() => setIsSignalsModalOpen(false)}
              style={{ position: "absolute", top: "20px", right: "20px", background: "none", border: "none", color: "var(--text-dim)", cursor: "pointer" }}
            >
              <X size={20} />
            </button>
            <h3 style={{ fontSize: "24px", fontWeight: 700, marginBottom: "8px" }}>Inventory Intelligence</h3>
            <p style={{ fontSize: "14px", color: "var(--text-dim)", marginBottom: "24px" }}>Full breakdown of predictive restock signals</p>

            <div style={{ flex: 1, overflowY: "auto", paddingRight: "10px" }}>
              {data.reorder?.map((item, i) => (
                <div key={i} style={{ display: "flex", alignItems: "center", gap: "16px", padding: "16px 0", borderBottom: "1px solid var(--border-ghost)" }}>
                  <div style={{
                    width: "10px", height: "10px", borderRadius: "50%",
                    background: item.signal === 'REORDER' ? "var(--accent-coral)" : "var(--accent-amber)"
                  }} />
                  <div style={{ flex: 1 }}>
                    <div style={{ fontWeight: 600 }}>{item.description}</div>
                    <div className="mono" style={{ fontSize: "11px", color: "var(--text-muted)" }}>SKU: {item.stock_code}</div>
                  </div>
                  <div style={{ textAlign: "right" }}>
                    <div style={{ fontWeight: 700, color: item.unit_change_pct < 0 ? "var(--accent-coral)" : "var(--accent-teal)" }}>
                      {item.unit_change_pct < 0 ? `${item.unit_change_pct}%` : `+${item.unit_change_pct}%`}
                    </div>
                    <div style={{ fontSize: "10px", color: "var(--text-dim)" }}>Shift</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
