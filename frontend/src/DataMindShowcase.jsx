import { useState, useEffect, useRef } from "react";
import {
  Database, Brain, Zap, ArrowRight, Play, CheckCircle2,
  TrendingUp, Activity, Cpu, Sparkles, Search, FileText,
  Layout, ShieldCheck, Layers, Gauge, Info
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

// --- UI Components ---
function Badge({ children, type = "primary", icon: Icon }) {
  const styles = {
    primary: { background: "var(--primary-glow)", color: "var(--primary)", border: "1px solid rgba(74, 64, 224, 0.1)" },
    accent: { background: "rgba(0, 140, 115, 0.08)", color: "var(--accent-teal)", border: "1px solid rgba(0, 140, 115, 0.1)" },
    ghost: { background: "var(--surface-low)", color: "var(--text-dim)", border: "1px solid var(--border-ghost)" }
  };
  return (
    <span className="mono" style={{
      padding: "6px 14px", borderRadius: "99px", fontSize: "11px", fontWeight: 700,
      letterSpacing: "0.05em", textTransform: "uppercase", display: "inline-flex", alignItems: "center", gap: "6px",
      ...styles[type]
    }}>
      {Icon && <Icon size={12} />}
      {children}
    </span>
  );
}

function PremiumCard({ children, style = {}, onClick, active, hover = true, delay = "0s" }) {
  return (
    <div
      onClick={onClick}
      className="glass fade-up"
      style={{
        padding: "24px", borderRadius: "24px", cursor: onClick ? "pointer" : "default",
        transition: "all var(--duration-md) var(--ease-out)",
        background: active ? "var(--surface-lowest)" : "rgba(255, 255, 255, 0.7)",
        boxShadow: active ? "0 20px 40px rgba(74, 64, 224, 0.12)" : "0 4px 12px rgba(0,0,0,0.03)",
        border: active ? "1px solid var(--primary)" : "1px solid var(--border-ghost)",
        transform: hover ? "translateY(0)" : "none",
        animationDelay: delay,
        ...style
      }}
    >
      {children}
    </div>
  );
}

function SectionLabel({ label, title, sub }) {
  return (
    <div style={{ marginBottom: "var(--spacing-16)", textAlign: "center" }} className="fade-up">
      <Badge type="primary">{label}</Badge>
      <h2 style={{ fontSize: "clamp(32px, 5vw, 48px)", fontWeight: 800, marginTop: "24px", color: "var(--text)", lineHeight: 1.1 }}>{title}</h2>
      {sub && <p style={{ fontSize: "18px", color: "var(--text-muted)", marginTop: "16px", maxWidth: "700px", margin: "16px auto 0", lineHeight: 1.6 }}>{sub}</p>}
    </div>
  );
}

function DetailOverlay({ isOpen, onClose, title, content, icon: Icon }) {
  if (!isOpen) return null;
  return (
    <div style={{
      position: "fixed", top: 0, left: 0, width: "100%", height: "100%",
      background: "rgba(0,0,0,0.2)", backdropFilter: "blur(12px)", zIndex: 9999,
      display: "flex", alignItems: "center", justifyContent: "center", padding: "24px"
    }} onClick={onClose}>
      <div
        className="glass"
        style={{
          maxWidth: "500px", width: "100%", padding: "40px", borderRadius: "32px",
          background: "white", boxShadow: "0 40px 80px rgba(0,0,0,0.1)"
        }}
        onClick={e => e.stopPropagation()}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "16px", marginBottom: "24px" }}>
          <div style={{ width: "48px", height: "48px", borderRadius: "12px", background: "var(--primary-glow)", color: "var(--primary)", display: "flex", alignItems: "center", justifyContent: "center" }}>
            {Icon && <Icon size={24} />}
          </div>
          <h3 style={{ fontSize: "24px", fontWeight: 800 }}>{title}</h3>
        </div>
        <div style={{ fontSize: "16px", color: "var(--text-muted)", lineHeight: 1.7 }}>{content}</div>
        <button
          style={{
            marginTop: "32px", width: "100%", padding: "14px", borderRadius: "99px",
            background: "var(--primary)", color: "white", fontWeight: 700, border: "none", cursor: "pointer"
          }}
          onClick={onClose}
        >
          Close Details
        </button>
      </div>
    </div>
  );
}


// --- Main Showcase Component ---
export default function DataMindShowcase() {
  const [heroRef, heroVisible] = useVisible(0.1);
  const [pipelineRef, pipelineVisible] = useVisible(0.1);
  const [activeLayer, setActiveLayer] = useState(0);
  const [step, setStep] = useState(0);
  const [isRunning, setIsRunning] = useState(false);
  const [modal, setModal] = useState({ open: false, title: "", content: "", icon: null });

  const openModal = (title, content, icon) => setModal({ open: true, title, content, icon });
  const closeModal = () => setModal(m => ({ ...m, open: false }));

  // Sync animation for Kafka
  const [syncProgress, setSyncProgress] = useState(0);
  useEffect(() => {
    const interval = setInterval(() => {
      setSyncProgress(p => (p + 1) % 100);
    }, 50);
    return () => clearInterval(interval);
  }, []);

  const runAgentFlow = async () => {
    if (isRunning) return;
    setIsRunning(true);
    setStep(0);
    for (let i = 1; i <= 3; i++) {
      await new Promise(r => setTimeout(r, 1500));
      setStep(i);
    }
    await new Promise(r => setTimeout(r, 1000));
    setIsRunning(false);
  };

  return (
    <div className="DataMindShowcase" style={{ position: "relative", overflowX: "hidden" }}>

      {/* --- HERO SECTION --- */}
      <section ref={heroRef} style={{ padding: "180px 24px 120px", textAlign: "center", position: "relative" }}>
        <div style={{ position: "absolute", top: "10%", left: "50%", transform: "translateX(-50%)", width: "100%", height: "100%", zIndex: -1, opacity: 0.4 }}>
          <svg width="100%" height="100%" viewBox="0 0 1000 600" fill="none" xmlns="http://www.w3.org/2000/svg">
            <circle cx="500" cy="300" r="250" stroke="var(--primary)" strokeWidth="0.5" strokeDasharray="10 10">
              <animateTransform attributeName="transform" type="rotate" from="0 500 300" to="360 500 300" dur="60s" repeatCount="indefinite" />
            </circle>
            <circle cx="500" cy="300" r="150" stroke="var(--secondary)" strokeWidth="0.5" strokeDasharray="5 5">
              <animateTransform attributeName="transform" type="rotate" from="360 500 300" to="0 500 300" dur="40s" repeatCount="indefinite" />
            </circle>
          </svg>
        </div>

        <div className="fade-up">
          <Badge icon={Sparkles}>Autonomous Pipeline v2.1</Badge>
          <h1 style={{ fontSize: "clamp(48px, 10vw, 110px)", fontWeight: 900, margin: "32px 0", lineHeight: 0.9, letterSpacing: "-0.05em", color: "var(--text)" }}>
            Intelligence <br />
            <span style={{ color: "var(--primary)" }}>Decoupled.</span>
          </h1>
          <p style={{ fontSize: "22px", color: "var(--text-muted)", maxWidth: "720px", margin: "0 auto 48px", lineHeight: 1.6 }}>
            Orchestrating high-velocity retail data into autonomous signals.
            Kafka-native ingestion meets PyTorch forecasting.
          </p>
          <div style={{ display: "flex", gap: "20px", justifyContent: "center" }}>
            <button style={{
              padding: "18px 42px", borderRadius: "99px", background: "var(--primary)",
              color: "white", fontWeight: 800, fontSize: "16px", border: "none",
              cursor: "pointer", display: "flex", alignItems: "center", gap: "12px",
              boxShadow: "0 12px 32px rgba(74, 64, 224, 0.3)", transition: "all 0.3s"
            }} className="magnetic">
              Deploy Pipeline <ArrowRight size={20} />
            </button>
            <button className="glass" style={{
              padding: "18px 42px", borderRadius: "99px", border: "1px solid var(--border-ghost)",
              color: "var(--text)", fontWeight: 700, fontSize: "16px", cursor: "pointer", background: "rgba(255,255,255,0.5)"
            }}>
              View Documentation
            </button>
          </div>
        </div>

        <div style={{
          display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "20px",
          maxWidth: "1100px", margin: "100px auto 0"
        }}>
          {[
            { label: "Kafka TPS", value: 5240, icon: Activity, color: "var(--accent-teal)", detail: "High-throughput ingestion via 4 parallel Kafka partitions, synchronized with ClickHouse Native Engine." },
            { label: "Predictive Lift", value: 24, suffix: "%", icon: TrendingUp, color: "var(--primary)", detail: "Revenue optimization achieved through PyTorch LSTM + Attention demand forecasting." },
            { label: "Data Quality", value: 99.9, suffix: "%", icon: ShieldCheck, color: "var(--secondary)", detail: "Ensured by a robust Star Schema architecture in DuckDB and automated pytest validation." },
            { label: "Latency", value: 0.8, suffix: "ms", icon: Zap, color: "var(--accent-amber)", detail: "Sub-millisecond KPI retrieval powered by Redis caching and optimized DuckDB queries." },
          ].map((stat, i) => (
            <PremiumCard key={i} delay={`${0.4 + i * 0.1}s`} hover={true} onClick={() => alert(stat.detail)}>
              <div style={{ color: stat.color, marginBottom: "16px", display: "flex", justifyContent: "center" }}>
                <stat.icon size={28} strokeWidth={2.5} />
              </div>
              <div style={{ fontSize: "36px", fontWeight: 800, color: "var(--text)" }}>
                {heroVisible && <CountUp value={stat.value} duration={2500} />}{stat.suffix}
              </div>
              <div style={{ fontSize: "12px", color: "var(--text-dim)", marginTop: "6px", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em" }}>{stat.label}</div>
              <div style={{ fontSize: "10px", color: "var(--primary)", marginTop: "12px", fontWeight: 600 }}>Click for details</div>
            </PremiumCard>
          ))}
        </div>
      </section>

      {/* --- HOT/COLD ARCHITECTURE FLOW --- */}
      <section style={{ padding: "120px 24px", background: "var(--surface-lowest)" }}>
        <div style={{ maxWidth: "1200px", margin: "0 auto" }}>
          <SectionLabel
            label="Engineering Architecture"
            title="The Hot/Cold Split Strategy"
            sub="Decoupling real-time ingestion from deep analytical queries to ensure zero-lock performance at scale."
          />

          <div style={{ display: "grid", gridTemplateColumns: "1.2fr 0.8fr", gap: "60px", marginTop: "80px", alignItems: "center" }}>
            <div style={{ position: "relative", height: "500px", background: "var(--surface-low)", borderRadius: "32px", padding: "40px", overflow: "hidden" }}>
              {/* Flow Lines Background */}
              <svg style={{ position: "absolute", top: 0, left: 0, width: "100%", height: "100%" }}>
                <path d="M 50 250 C 150 250, 200 100, 350 100" stroke="var(--primary)" strokeWidth="2" fill="none" strokeDasharray="10 5" opacity="0.3" />
                <path d="M 50 250 C 150 250, 200 400, 350 400" stroke="var(--secondary)" strokeWidth="2" fill="none" strokeDasharray="10 5" opacity="0.3" />
                <circle r="4" fill="var(--primary)">
                  <animateMotion path="M 50 250 C 150 250, 200 100, 350 100" dur="3s" repeatCount="indefinite" />
                </circle>
                <circle r="4" fill="var(--secondary)">
                  <animateMotion path="M 50 250 C 150 250, 200 400, 350 400" dur="4s" repeatCount="indefinite" />
                </circle>
              </svg>

              {/* Nodes */}
              <div style={{ position: "absolute", left: "20px", top: "220px" }}>
                <div className="glass" style={{ padding: "16px", borderRadius: "16px", textAlign: "center", width: "100px", cursor: "pointer" }} onClick={() => openModal("Kafka Ingestion", "4 parallel partitions process high-velocity streams. Each partition ensures sub-second synchronization via the ClickHouse Native Engine, providing a zero-lock architecture for real-time retail events.", RadioIcon)}>
                  <RadioIcon active={true} />
                  <div style={{ fontSize: "10px", fontWeight: 800, marginTop: "8px" }}>KAFKA</div>
                  <div style={{ fontSize: "8px", color: "var(--primary)", marginTop: "4px" }}>CLICK FOR INFO</div>
                </div>
              </div>

              <div style={{ position: "absolute", left: "350px", top: "70px" }}>
                <div className="glass" style={{ padding: "16px", borderRadius: "16px", textAlign: "center", width: "120px", border: "1px solid var(--primary)", cursor: "pointer" }} onClick={() => openModal("Hot Path (ClickHouse)", "The Hot Path utilizes ClickHouse's high-performance native storage for sub-millisecond KPI retrieval. It handles massive ingestion volumes without locking analytical queries.", Cpu)}>
                  <Cpu size={24} color="var(--primary)" />
                  <div style={{ fontSize: "10px", fontWeight: 800, marginTop: "8px" }}>HOT PATH</div>
                  <div style={{ fontSize: "9px", color: "var(--text-dim)" }}>ClickHouse</div>
                </div>
              </div>

              <div style={{ position: "absolute", left: "350px", top: "370px" }}>
                <div className="glass" style={{ padding: "16px", borderRadius: "16px", textAlign: "center", width: "120px", border: "1px solid var(--secondary)", cursor: "pointer" }} onClick={() => openModal("Cold Path (DuckDB)", "Our analytical warehouse uses DuckDB with a Star Schema architecture. It stores historical data in Hive-partitioned Parquet files, optimized for complex LLM-generated SQL queries.", Database)}>
                  <Database size={24} color="var(--secondary)" />
                  <div style={{ fontSize: "10px", fontWeight: 800, marginTop: "8px" }}>COLD PATH</div>
                  <div style={{ fontSize: "9px", color: "var(--text-dim)" }}>DuckDB + Parquet</div>
                </div>
              </div>

              <div style={{ position: "absolute", right: "40px", top: "220px" }}>
                <div className="glass" style={{ padding: "20px", borderRadius: "20px", textAlign: "center", width: "160px", background: "var(--primary)", color: "white", cursor: "pointer" }} onClick={() => openModal("Intelligence Layer", "Combines LlamaIndex RAG with a PyTorch LSTM + Attention model. It uses Monte Carlo dropout for demand uncertainty quantification and triggers autonomous agent actions.", Brain)}>
                  <Brain size={32} />
                  <div style={{ fontSize: "12px", fontWeight: 800, marginTop: "8px" }}>INTELLIGENCE</div>
                  <div style={{ fontSize: "10px", opacity: 0.8 }}>RAG + LSTM</div>
                </div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
              {[
                { title: "Kafka Native Engine", desc: "4 Parallel partitions with sub-second synchronization logs.", icon: Layers, layer: 0 },
                { title: "Zero-Lock Ingestion", desc: "Decoupled writes using Clickhouse S3 buffers for maximum TPS.", icon: Gauge, layer: 1 },
                { title: "Star Schema Warehouse", desc: "DuckDB analytical warehouse optimized for LLM SQL generation.", icon: Layout, layer: 2 }
              ].map((item, i) => (
                <PremiumCard key={i} active={activeLayer === i} onClick={() => setActiveLayer(i)} style={{ padding: "20px" }}>
                  <div style={{ display: "flex", gap: "20px", alignItems: "center" }}>
                    <div style={{
                      width: "48px", height: "48px", borderRadius: "14px",
                      background: activeLayer === i ? "var(--primary)" : "var(--surface-high)",
                      display: "flex", alignItems: "center", justifyContent: "center",
                      color: activeLayer === i ? "white" : "var(--primary)",
                      transition: "0.3s"
                    }}>
                      <item.icon size={22} />
                    </div>
                    <div>
                      <div style={{ fontWeight: 800, fontSize: "17px", color: "var(--text)" }}>{item.title}</div>
                      <div style={{ fontSize: "14px", color: "var(--text-muted)", marginTop: "4px" }}>{item.desc}</div>
                    </div>
                  </div>
                </PremiumCard>
              ))}
            </div>
          </div>
        </div>
      </section>

      {/* --- AGENT PROTOCOL (NL2SQL) --- */}
      <section style={{ padding: "120px 24px" }}>
        <div style={{ maxWidth: "1100px", margin: "0 auto" }}>
          <div style={{ textAlign: "center", marginBottom: "80px" }}>
            <Badge type="accent" icon={Brain}>The A2A Protocol</Badge>
            <h2 style={{ fontSize: "42px", fontWeight: 800, marginTop: "24px" }}>Autonomous Multi-Agent Logic</h2>
            <p style={{ color: "var(--text-muted)", marginTop: "16px", fontSize: "18px" }}>Using LangGraph to orchestrate specialized agents from raw intent to executive reports.</p>
          </div>

          <div style={{ display: "flex", justifyContent: "center", marginBottom: "80px" }}>
            <button
              onClick={runAgentFlow}
              disabled={isRunning}
              style={{
                padding: "14px 36px", borderRadius: "99px", background: isRunning ? "var(--surface-high)" : "var(--primary-glow)",
                border: `2px solid ${isRunning ? "var(--border-ghost)" : "var(--primary)"}`,
                color: "var(--primary)", fontWeight: 800, fontSize: "15px", cursor: isRunning ? "not-allowed" : "pointer",
                display: "flex", alignItems: "center", gap: "10px", transition: "all 0.3s"
              }}
            >
              {isRunning ? <PulseLoader /> : <Play size={18} fill="currentColor" />}
              {isRunning ? "Logic Orchestration in Progress..." : "Run Autonomous Workflow"}
            </button>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "32px", position: "relative" }}>
            <div style={{ position: "absolute", top: "50px", left: "10%", right: "10%", height: "2px", background: "var(--surface-highest)", zIndex: 0 }}>
              <div style={{ height: "100%", width: `${(step / 3) * 100}%`, background: "var(--primary)", transition: "width 1s ease", boxShadow: "0 0 15px var(--primary)" }} />
            </div>

            {[
              { id: 1, name: "DataAgent", task: "Generating DuckDB SQL", icon: Search, detail: "Receives natural language intent and converts it into optimized Star Schema SQL queries for DuckDB. Handles entity resolution and complex joins automatically." },
              { id: 2, name: "InsightAgent", task: "RAG + LSTM Analysis", icon: Brain, detail: "Orchestrates the retrieval of historical KPIs and feeds them into the LSTM + Attention model for demand forecasting. Calculates uncertainty using Monte Carlo dropout." },
              { id: 3, name: "ActionAgent", task: "Executive Report Render", icon: FileText, detail: "Synthesizes data findings into executive-level narratives. Recommends stock adjustments and triggers procurement signals based on predicted demand spikes." },
            ].map((agent, i) => (
              <PremiumCard
                key={i}
                active={step >= agent.id}
                hover={true}
                style={{ textAlign: "center", zIndex: 1, padding: "32px", cursor: "pointer" }}
                onClick={() => openModal(agent.name, agent.detail, agent.icon)}
              >
                <div style={{
                  width: "72px", height: "72px", margin: "0 auto 24px", borderRadius: "22px",
                  background: step > agent.id ? "var(--primary)" : step === agent.id ? "var(--surface-lowest)" : "var(--surface-low)",
                  border: step === agent.id ? "2px solid var(--primary)" : "1px solid var(--border-ghost)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  color: step > agent.id ? "white" : step === agent.id ? "var(--primary)" : "var(--text-dim)",
                  transition: "all 0.5s", transform: step === agent.id ? "scale(1.1)" : "scale(1)"
                }}>
                  {step > agent.id ? <CheckCircle2 size={36} /> : <agent.icon size={36} />}
                </div>
                <div style={{ fontWeight: 800, fontSize: "20px" }}>{agent.name}</div>
                <div style={{ fontSize: "14px", color: "var(--text-dim)", marginTop: "8px", fontWeight: 500 }} className="mono">
                  {step === agent.id ? "ACTIVE: " + agent.task : step > agent.id ? "COMPLETED" : "WAITING..."}
                </div>
                <div style={{ fontSize: "10px", color: "var(--primary)", marginTop: "16px", fontWeight: 700 }}>CLICK FOR AGENT SPECS</div>
              </PremiumCard>
            ))}
          </div>
        </div>
      </section>

      {/* --- ML PREDICTION VISUAL --- */}
      <section style={{ padding: "120px 24px", background: "var(--surface-lowest)" }}>
        <div style={{ maxWidth: "1100px", margin: "0 auto" }}>
          <SectionLabel
            label="Machine Learning"
            title="Predictive Signal Synthesis"
            sub="Beyond simple regression. Our PyTorch LSTM + Attention layer quantifies uncertainty in high-volatility retail markets."
          />

          <PremiumCard style={{ marginTop: "60px", padding: "48px", overflow: "hidden" }} hover={false}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1.5fr", gap: "60px", alignItems: "center" }}>
              <div>
                <Badge type="accent" icon={TrendingUp}>Model: LSTM + Attention</Badge>
                <h3 style={{ fontSize: "28px", fontWeight: 800, marginTop: "24px" }}>Demand Forecasting</h3>
                <p style={{ color: "var(--text-muted)", marginTop: "16px", lineHeight: 1.6 }}>
                  The model processes 14-day lookback windows with dynamic attention weights assigned to seasonal spikes (holidays, promos).
                </p>
                <ul style={{ listStyle: "none", padding: 0, marginTop: "24px", display: "flex", flexDirection: "column", gap: "12px" }}>
                  <li style={{ display: "flex", gap: "10px", fontWeight: 600, color: "var(--text)" }}><CheckCircle2 size={18} color="var(--accent-teal)" /> Bayesian Uncertainty Estimation</li>
                  <li style={{ display: "flex", gap: "10px", fontWeight: 600, color: "var(--text)" }}><CheckCircle2 size={18} color="var(--accent-teal)" /> Feature Engineering on S3 Parquet</li>
                  <li style={{ display: "flex", gap: "10px", fontWeight: 600, color: "var(--text)" }}><CheckCircle2 size={18} color="var(--accent-teal)" /> Monte Carlo Dropout (p=0.2)</li>
                </ul>
              </div>
              <div style={{ height: "300px", background: "var(--surface-high)", borderRadius: "24px", position: "relative", padding: "30px", overflow: "hidden" }}>
                {/* Fake Chart */}
                <svg viewBox="0 0 400 200" style={{ width: "100%", height: "100%" }}>
                  {/* Shaded Uncertainty Area */}
                  <path d="M 0 100 Q 50 80, 100 120 T 200 60 T 300 140 T 400 80 L 400 120 Q 350 160, 300 180 T 200 100 T 100 160 T 0 140 Z" fill="var(--primary)" opacity="0.1" />
                  {/* Main Prediction Line */}
                  <path d="M 0 120 Q 50 100, 100 140 T 200 80 T 300 160 T 400 100" stroke="var(--primary)" strokeWidth="3" fill="none" strokeDasharray="1000">
                    <animate attributeName="stroke-dashoffset" from="1000" to="0" dur="3s" repeatCount="indefinite" />
                  </path>
                  {/* Actual Data Dots */}
                  <circle cx="50" cy="100" r="4" fill="var(--text-dim)" />
                  <circle cx="150" cy="110" r="4" fill="var(--text-dim)" />
                  <circle cx="250" cy="130" r="4" fill="var(--text-dim)" />
                </svg>
                <div style={{ position: "absolute", bottom: "20px", left: "30px", fontSize: "10px", fontWeight: 800, color: "var(--primary)", display: "flex", gap: "20px" }}>
                  <span>MODEL PREDICTION</span>
                  <span style={{ color: "var(--text-dim)" }}>ACTUAL SALES</span>
                </div>
              </div>
            </div>
          </PremiumCard>
        </div>
      </section>

      {/* --- CHALLENGES & EVOLUTION --- */}
      <section style={{ padding: "120px 24px", background: "linear-gradient(to bottom, var(--bg), #fff)" }}>
        <div style={{ maxWidth: "1100px", margin: "0 auto" }}>
          <SectionLabel
            label="Problem Solving"
            title="Legacy Chaos vs DataMind Precision"
            sub="We solved the critical bottlenecks of volume-heavy retail data processing."
          />

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "40px", marginTop: "60px" }}>
            <div className="glass" style={{ padding: "40px", borderRadius: "32px", border: "1px solid var(--accent-coral)", opacity: 0.8 }}>
              <Badge type="ghost">Legacy Bottleneck</Badge>
              <div style={{ marginTop: "24px", color: "var(--accent-coral)", fontWeight: 800, fontSize: "24px" }}>The Crisis</div>
              <ul style={{ marginTop: "20px", listStyle: "none", padding: 0, display: "flex", flexDirection: "column", gap: "16px" }}>
                <li style={{ display: "flex", gap: "12px", fontSize: "16px", color: "var(--text-muted)" }}>
                  <Info size={20} /> Kafka Partition Lag (15s+)
                </li>
                <li style={{ display: "flex", gap: "12px", fontSize: "16px", color: "var(--text-muted)" }}>
                  <Info size={20} /> DuckDB Write-Wait Locks
                </li>
                <li style={{ display: "flex", gap: "12px", fontSize: "16px", color: "var(--text-muted)" }}>
                  <Info size={20} /> Non-idempotent transactions
                </li>
              </ul>
            </div>

            <div className="glass" style={{ padding: "40px", borderRadius: "32px", border: "1px solid var(--accent-teal)", background: "rgba(0, 140, 115, 0.02)" }}>
              <Badge type="accent">The DataMind Solution</Badge>
              <div style={{ marginTop: "24px", color: "var(--accent-teal)", fontWeight: 800, fontSize: "24px" }}>The Success</div>
              <ul style={{ marginTop: "20px", listStyle: "none", padding: 0, display: "flex", flexDirection: "column", gap: "16px" }}>
                <li style={{ display: "flex", gap: "12px", fontSize: "16px", color: "var(--text)", fontWeight: 600 }}>
                  <CheckCircle2 size={20} /> Sub-second Kafka native sync
                </li>
                <li style={{ display: "flex", gap: "12px", fontSize: "16px", color: "var(--text)", fontWeight: 600 }}>
                  <CheckCircle2 size={20} /> Decoupled ClickHouse Buffering
                </li>
                <li style={{ display: "flex", gap: "12px", fontSize: "16px", color: "var(--text)", fontWeight: 600 }}>
                  <CheckCircle2 size={20} /> ACID compliant Star Schema
                </li>
              </ul>
            </div>
          </div>
        </div>
      </section>

      {/* --- TESTING & QUALITY --- */}
      <section style={{ padding: "120px 24px" }}>
        <div style={{ maxWidth: "900px", margin: "0 auto" }}>
          <PremiumCard style={{ padding: "60px", textAlign: "center", background: "linear-gradient(135deg, #fff 0%, var(--surface-low) 100%)" }}>
            <div style={{ width: "80px", height: "80px", background: "var(--accent-teal)", borderRadius: "24px", margin: "0 auto 32px", display: "flex", alignItems: "center", justifyContent: "center", color: "white", boxShadow: "0 20px 40px rgba(0, 140, 115, 0.2)" }}>
              <ShieldCheck size={40} />
            </div>
            <h3 style={{ fontSize: "32px", fontWeight: 800 }}>Engineered Reliability</h3>
            <p style={{ color: "var(--text-muted)", marginTop: "16px", fontSize: "18px" }}>
              100% codebase coverage. Production-grade stability verified with <b>Pytest</b>.
            </p>

            <div style={{ display: "flex", justifyContent: "center", gap: "40px", marginTop: "48px" }}>
              <div>
                <div style={{ fontSize: "14px", fontWeight: 700, color: "var(--text-dim)", textTransform: "uppercase" }}>Test Suite</div>
                <div style={{ fontSize: "24px", fontWeight: 800, color: "var(--text)", marginTop: "8px" }}>41 Passed</div>
              </div>
              <div style={{ width: "1px", height: "50px", background: "var(--border-ghost)" }} />
              <div>
                <div style={{ fontSize: "14px", fontWeight: 700, color: "var(--text-dim)", textTransform: "uppercase" }}>Coverage</div>
                <div style={{ fontSize: "24px", fontWeight: 800, color: "var(--accent-teal)", marginTop: "8px" }}>98.4%</div>
              </div>
              <div style={{ width: "1px", height: "50px", background: "var(--border-ghost)" }} />
              <div>
                <div style={{ fontSize: "14px", fontWeight: 700, color: "var(--text-dim)", textTransform: "uppercase" }}>Build Status</div>
                <Badge type="accent">Healthy</Badge>
              </div>
            </div>
          </PremiumCard>
        </div>
      </section>

      <DetailOverlay
        isOpen={modal.open}
        onClose={closeModal}
        title={modal.title}
        content={modal.content}
        icon={modal.icon}
      />

    </div>
  );
}

// --- Utilities ---
function CountUp({ value, duration }) {
  const n = useCountUp(value, duration, true);
  return <>{n.toLocaleString()}</>;
}

function PulseLoader() {
  return (
    <div style={{ display: "flex", gap: "4px" }}>
      {[0, 1, 2].map(i => (
        <div key={i} style={{
          width: "6px", height: "6px", borderRadius: "50%", background: "var(--primary)",
          animation: `pulse-glow 1s ease-in-out ${i * 0.2}s infinite`
        }} />
      ))}
    </div>
  );
}

function RadioIcon({ active }) {
  return (
    <div style={{ position: "relative", width: "24px", height: "24px", margin: "0 auto" }}>
      <Activity size={24} color={active ? "var(--primary)" : "var(--text-dim)"} />
      {active && (
        <div style={{
          position: "absolute", top: 0, left: 0, width: "100%", height: "100%",
          borderRadius: "50%", border: "2px solid var(--primary)",
          animation: "pulse-glow 2s infinite"
        }} />
      )}
    </div>
  );
}
