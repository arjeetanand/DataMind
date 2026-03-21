import { useState, useEffect, useRef } from "react";
import { 
  Database, Brain, Zap, ArrowRight, Play, CheckCircle2, 
  TrendingUp, Activity, Cpu, Sparkles
} from "lucide-react";

// --- Custom Hooks ---
function useCountUp(target, duration = 2000, start = false) {
    const [val, setVal] = useState(0);
    useEffect(() => {
        if (!start) return;
        let t0 = null;
        const raf = (ts) => {
            if (!t0) t0 = ts;
            const p = Math.min((ts - t0) / duration, 1);
            const e = 1 - Math.pow(1 - p, 4);
            setVal(Math.floor(e * target));
            if (p < 1) requestAnimationFrame(raf);
            else setVal(target);
        };
        requestAnimationFrame(raf);
    }, [target, duration, start]);
    return val;
}

function useVisible(threshold = 0.1) {
    const ref = useRef(null);
    const [visible, setVisible] = useState(false);
    useEffect(() => {
        const obs = new IntersectionObserver(([e]) => { if (e.isIntersecting) setVisible(true); }, { threshold });
        if (ref.current) obs.observe(ref.current);
        return () => obs.disconnect();
    }, [threshold]);
    return [ref, visible];
}

// --- Specialized UI Components ---
function Badge({ children, type = "primary" }) {
  const styles = {
    primary: { background: "var(--primary-glow)", color: "var(--primary)", border: "1px solid rgba(192, 193, 255, 0.2)" },
    accent: { background: "rgba(0, 212, 170, 0.1)", color: "var(--accent-teal)", border: "1px solid rgba(0, 212, 170, 0.2)" },
    ghost: { background: "var(--surface-high)", color: "var(--text-dim)", border: "1px solid var(--border-ghost)" }
  };
  return (
    <span className="mono" style={{
      padding: "4px 12px", borderRadius: "99px", fontSize: "11px", fontWeight: 600,
      letterSpacing: "0.04em", textTransform: "uppercase", display: "inline-flex", alignItems: "center", gap: "6px",
      ...styles[type]
    }}>
      {children}
    </span>
  );
}

function GlassCard({ children, style = {}, onClick, active, hoverScale = true }) {
  return (
    <div
      onClick={onClick}
      className="glass"
      style={{
        padding: "24px", borderRadius: "16px", cursor: onClick ? "pointer" : "default",
        transition: "all var(--duration-md) var(--ease-out)",
        boxShadow: active ? "0 0 32px var(--primary-glow)" : "none",
        border: active ? "1px solid var(--primary)" : "1px solid var(--border-ghost)",
        transform: hoverScale && onClick ? "scale(1.02)" : "scale(1)",
        ...style
      }}
    >
      {children}
    </div>
  );
}

function SectionHead({ label, title, sub }) {
  return (
    <div style={{ marginBottom: "var(--spacing-12)", animation: "fadeUp 0.8s ease-out" }}>
      <Badge type="accent">{label}</Badge>
      <h2 style={{ fontSize: "36px", fontWeight: 700, marginTop: "16px", color: "var(--text)" }}>{title}</h2>
      {sub && <p style={{ fontSize: "16px", color: "var(--text-muted)", marginTop: "12px", maxWidth: "600px", lineHeight: 1.6 }}>{sub}</p>}
    </div>
  );
}

// --- Layout & Content ---
const AGENTS = [
  { name: "DataAgent", role: "AI Data Engineer", icon: Database, color: "var(--accent-teal)", file: "data_agent.py" },
  { name: "InsightAgent", role: "AI Strategist", icon: Brain, color: "var(--accent-amber)", file: "insight_agent.py" },
  { name: "ActionAgent", role: "AI Automation", icon: Zap, color: "var(--tertiary)", file: "action_agent.py" }
];

export default function DataMindShowcase() {
  const [heroRef, heroVisible] = useVisible(0.1);
  const [activeTab, setActiveTab] = useState(0);
  const [pipelineStep, setPipelineStep] = useState(-1);
  const [running, setRunning] = useState(false);

  const runPipeline = async () => {
    if (running) return;
    setRunning(true);
    setPipelineStep(-1);
    for (let i = 0; i < 3; i++) {
      await new Promise(r => setTimeout(r, 400));
      setPipelineStep(i);
      await new Promise(r => setTimeout(r, 1200));
    }
    setRunning(false);
  };

  return (
    <div className="DataMindShowcase" style={{ paddingBottom: "100px" }}>
      
      {/* --- HERO SECTION --- */}
      <section ref={heroRef} style={{ padding: "160px 24px 80px", textAlign: "center", position: "relative" }}>
        <div className="fade-up" style={{ animationDelay: "0.1s" }}>
          <Badge>Version 2.0 · Intelligent Retail</Badge>
          <h1 style={{ fontSize: "clamp(48px, 8vw, 84px)", fontWeight: 800, margin: "24px 0", lineHeight: 0.95, letterSpacing: "-0.04em" }}>
            The Intelligence <br />
            <span className="shimmer-text">Canvas</span>
          </h1>
          <p style={{ fontSize: "20px", color: "var(--text-muted)", maxWidth: "600px", margin: "0 auto 40px", lineHeight: 1.6 }}>
            Autonomous Retail Analytics & Predictive Demand Intelligence.
            Bridging the gap between raw data and decisive action.
          </p>
          <div style={{ display: "flex", gap: "16px", justifyContent: "center" }}>
            <button style={{
              padding: "16px 36px", borderRadius: "99px", background: "var(--primary)",
              color: "var(--bg)", fontWeight: 700, fontSize: "16px", border: "none",
              cursor: "pointer", display: "flex", alignItems: "center", gap: "10px",
              boxShadow: "0 8px 24px var(--primary-glow)"
            }}>
              Explore Pipeline <ArrowRight size={18} />
            </button>
            <button className="glass" style={{
              padding: "16px 36px", borderRadius: "99px", border: "1px solid var(--border-ghost)",
              color: "var(--text)", fontWeight: 600, fontSize: "16px", cursor: "pointer"
            }}>
              View Tech Stack
            </button>
          </div>
        </div>

        {/* Hero Stats */}
        <div style={{ 
          display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "24px", 
          maxWidth: "900px", margin: "80px auto 0" 
        }}>
          {[
            { label: "Transactions Processed", value: 541910, icon: Activity },
            { label: "Predictive Accuracy", value: 94, suffix: "%", icon: TrendingUp },
            { label: "Autonomous Actions", value: 1205, icon: Zap },
          ].map((stat, i) => (
            <GlassCard key={i} style={{ animationDelay: `${0.4 + i * 0.1}s` }} className="fade-up">
              <div style={{ color: "var(--primary)", marginBottom: "12px" }}>
                <stat.icon size={24} />
              </div>
              <div style={{ fontSize: "32px", fontWeight: 700, fontFamily: "JetBrains Mono" }}>
                {heroVisible && <CountUp value={stat.value} duration={2500} />}{stat.suffix}
              </div>
              <div style={{ fontSize: "13px", color: "var(--text-dim)", marginTop: "4px", textTransform: "uppercase", letterSpacing: "0.08em" }}>{stat.label}</div>
            </GlassCard>
          ))}
        </div>
      </section>

      {/* --- PIPELINE LAYERS --- */}
      <section style={{ maxWidth: "1000px", margin: "100px auto", padding: "0 24px" }}>
        <SectionHead 
          label="01 — The Architecture" 
          title="Distributed Layer Intelligence"
          sub="Our pipeline transforms unstructured Kaggle transactions into actionable signals via a multi-stage refinement process."
        />

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "40px", alignItems: "center" }}>
          <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
            {[
              { id: 0, title: "Data Lake (Parquet)", desc: "Hive-partitioned storage for 500k+ records.", icon: Database },
              { id: 1, title: "OLAP Warehouse (DuckDB)", desc: "Sub-second analytical queries on star schemas.", icon: Cpu },
              { id: 2, title: "Intelligence Layer (RAG)", desc: "Semantic grounding using LlamaIndex.", icon: Brain }
            ].map((layer, i) => (
              <GlassCard 
                key={i} 
                active={activeTab === i} 
                onClick={() => setActiveTab(i)}
                style={{ padding: "16px 20px" }}
              >
                <div style={{ display: "flex", gap: "16px", alignItems: "center" }}>
                  <div style={{ 
                    width: "40px", height: "40px", borderRadius: "10px", 
                    background: activeTab === i ? "var(--primary)" : "var(--surface-high)",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    color: activeTab === i ? "var(--bg)" : "var(--primary)",
                    transition: "all var(--duration-sm)"
                  }}>
                    <layer.icon size={20} />
                  </div>
                  <div>
                    <div style={{ fontWeight: 600, fontSize: "16px" }}>{layer.title}</div>
                    <div style={{ fontSize: "13px", color: "var(--text-dim)" }}>{layer.desc}</div>
                  </div>
                </div>
              </GlassCard>
            ))}
          </div>

          <div className="glass" style={{ height: "340px", borderRadius: "24px", padding: "32px", position: "relative", overflow: "hidden" }}>
             <div style={{ 
               position: 'absolute', top: '10px', left: '10px', padding: '6px 12px', 
               borderRadius: '99px', background: 'var(--surface-highest)', fontSize: '10px', 
               fontWeight: 600, color: 'var(--text-dim)', border: '1px solid var(--border-ghost)' 
             }}>
               SYSTEM_TRACE_LOG
             </div>
             
             <div style={{ marginTop: "30px", fontFamily: "JetBrains Mono", fontSize: "12px", lineHeight: 1.8 }}>
                {activeTab === 0 && (
                  <div key="t0" className="fade-up">
                    <div style={{ color: "var(--accent-teal)" }}>{">"} initializing datalake_session...</div>
                    <div style={{ color: "white" }}>[INFO] Loading snappy.parquet partitions</div>
                    <div style={{ color: "white" }}>[INFO] Scanning Year=2010... Success</div>
                    <div style={{ color: "white" }}>[INFO] Record count: 397,885 rows</div>
                    <div style={{ color: "var(--text-dim)" }}>{">"} Memory optimization enabled (Zero-copy)</div>
                  </div>
                )}
                {activeTab === 1 && (
                  <div key="t1" className="fade-up">
                    <div style={{ color: "var(--accent-amber)" }}>{">"} connection_pool check: DuckDB in-process</div>
                    <div style={{ color: "white" }}>[SQL] SELECT count(*) FROM fact_sales;</div>
                    <div style={{ color: "white" }}>[INFO] Latency: 0.04ms</div>
                    <div style={{ color: "white" }}>[INFO] JOIN dim_customer ON rfm_score;</div>
                    <div style={{ color: "var(--text-dim)" }}>{">"} Analytic cache primed</div>
                  </div>
                )}
                {activeTab === 2 && (
                  <div key="t2" className="fade-up">
                     <div style={{ color: "var(--tertiary)" }}>{">"} FAISS index loading...</div>
                     <div style={{ color: "white" }}>[RAG] Embedding query: "High-value customers"</div>
                     <div style={{ color: "white" }}>[RAG] top_k similarity nodes retrieved</div>
                     <div style={{ color: "white" }}>[RAG] Grounding report using OCI GenAI</div>
                     <div style={{ color: "var(--text-dim)" }}>{">"} context window: 4096 tokens</div>
                  </div>
                )}
             </div>

             {/* Visual decoration */}
             <div style={{ 
               position: "absolute", bottom: "-20px", right: "-20px", width: "120px", height: "120px",
               background: "var(--primary)", opacity: 0.1, filter: "blur(40px)", borderRadius: "50%"
             }} />
          </div>
        </div>
      </section>

      {/* --- AGENT PIPELINE --- */}
      <section style={{ maxWidth: "1000px", margin: "140px auto", padding: "0 24px" }}>
        <div style={{ textAlign: "center", marginBottom: "60px" }}>
          <Badge type="accent">02 — Autonomous Agents</Badge>
          <h2 style={{ fontSize: "42px", fontWeight: 700, marginTop: "16px" }}>The Multi-Agent Protocol</h2>
          <p style={{ color: "var(--text-muted)", marginTop: "12px" }}>Conditional orchestration using LangGraph. One intent, multiple specialists.</p>
        </div>

        <div style={{ display: "flex", justifyContent: "center", marginBottom: "60px" }}>
           <button 
             onClick={runPipeline}
             disabled={running}
             style={{
               padding: "12px 32px", borderRadius: "99px", background: running ? "var(--surface-high)" : "var(--primary-glow)",
               border: `1px solid ${running ? "var(--border-ghost)" : "var(--primary)"}`,
               color: "var(--primary)", fontWeight: 700, fontSize: "14px", cursor: running ? "not-allowed" : "pointer",
               display: "flex", alignItems: "center", gap: "10px", transition: "all 0.2s"
             }}
           >
             {running ? <LoaderIcon /> : <Play size={16} />}
             {running ? "Pipeline Running..." : "Execute Intent: reorder_signals"}
           </button>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "24px", position: "relative" }}>
           {/* Connecting Line */}
           <div style={{ 
             position: "absolute", top: "40px", left: "15%", right: "15%", height: "2px", 
             background: "var(--surface-high)", zIndex: 0 
           }}>
             <div style={{ 
               height: "100%", width: `${(pipelineStep + 1) * 33.3}%`, 
               background: "var(--primary)", transition: "width 0.8s ease",
               boxShadow: "0 0 10px var(--primary)" 
             }} />
           </div>

           {AGENTS.map((agent, i) => {
             const active = pipelineStep === i;
             const done = pipelineStep > i;
             return (
               <GlassCard key={i} active={active || done} hoverScale={false} style={{ textAlign: "center", zIndex: 1 }}>
                  <div style={{ 
                    width: "56px", height: "56px", margin: "0 auto 20px", borderRadius: "16px",
                    background: done ? "var(--primary)" : active ? "var(--surface-highest)" : "var(--surface-low)",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    color: done ? "var(--bg)" : active ? "var(--primary)" : "var(--text-dim)",
                    transition: "all 0.4s",
                    border: active ? "1px solid var(--primary)" : "1px solid var(--border-ghost)"
                  }}>
                    {done ? <CheckCircle2 size={28} /> : <agent.icon size={28} />}
                  </div>
                  <div style={{ fontWeight: 700, fontSize: "18px" }}>{agent.name}</div>
                  <div style={{ fontSize: "13px", color: "var(--text-dim)", marginTop: "4px" }}>{agent.role}</div>
                  <div className="mono" style={{ fontSize: "10px", marginTop: "12px", color: done ? "var(--accent-teal)" : "var(--text-muted)" }}>
                    {agent.file}
                  </div>
               </GlassCard>
             );
           })}
        </div>
      </section>

      {/* --- TECH GRID --- */}
      <section style={{ maxWidth: "1000px", margin: "140px auto", padding: "0 24px" }}>
        <SectionHead label="03 — Technology" title="Engineered for Performance" />
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "16px" }}>
          {[
            { tag: "Core", label: "DuckDB", icon: "⚡" },
            { tag: "AI", label: "LlamaIndex", icon: "🔍" },
            { tag: "Graph", label: "LangGraph", icon: "🕸️" },
            { tag: "Embed", label: "sentence-transformers", icon: "🔢" },
            { tag: "API", label: "FastAPI", icon: "🚀" },
            { tag: "State", label: "Pydantic v2", icon: "🛡️" },
            { tag: "ML", label: "PyTorch", icon: "📈" },
            { tag: "Store", label: "FAISS", icon: "🏛️" },
          ].map((item, i) => (
            <GlassCard key={i} style={{ padding: "12px 16px" }}>
              <div style={{ fontSize: "20px", marginBottom: "8px" }}>{item.icon}</div>
              <div style={{ fontSize: "10px", color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.08em" }}>{item.tag}</div>
              <div className="mono" style={{ fontSize: "12px", fontWeight: 700, color: "var(--primary)" }}>{item.label}</div>
            </GlassCard>
          ))}
        </div>
      </section>
    </div>
  );
}

// --- Internal Utilities ---
function CountUp({ value, duration }) {
  const n = useCountUp(value, duration, true);
  return <>{n.toLocaleString()}</>;
}

function LoaderIcon() {
  return (
    <div style={{ animation: "spin 1s linear infinite" }}>
      <Sparkles size={16} />
    </div>
  );
}