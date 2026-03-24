import { useEffect, useState } from "react";
import {
    Sparkles,
    Activity,
    TrendingUp,
    ShieldCheck,
    Zap,
    Database,
    Cpu,
    Brain,
    Layers,
    CheckCircle2,
    Target,
    AlertTriangle,
} from "lucide-react";

function Badge({ children, icon: Icon, type = "primary" }) {
    const styles = {
        primary: {
            background: "var(--primary-glow)",
            color: "var(--primary)",
            border: "1px solid rgba(74, 64, 224, 0.12)",
        },
        accent: {
            background: "rgba(0, 140, 115, 0.08)",
            color: "var(--accent-teal)",
            border: "1px solid rgba(0, 140, 115, 0.12)",
        },
        ghost: {
            background: "var(--surface-low)",
            color: "var(--text-dim)",
            border: "1px solid var(--border-ghost)",
        },
    };

    return (
        <span
            className="mono"
            style={{
                padding: "6px 12px",
                borderRadius: "99px",
                fontSize: "11px",
                fontWeight: 700,
                letterSpacing: "0.05em",
                textTransform: "uppercase",
                display: "inline-flex",
                alignItems: "center",
                gap: "6px",
                ...styles[type],
            }}
        >
            {Icon && <Icon size={12} />}
            {children}
        </span>
    );
}

function Card({ children, style = {} }) {
    return (
        <div
            className="glass"
            style={{
                borderRadius: "22px",
                padding: "24px",
                ...style,
            }}
        >
            {children}
        </div>
    );
}

function SectionTitle({ label, title, sub }) {
    return (
        <div style={{ textAlign: "center", marginBottom: "42px" }}>
            <Badge>{label}</Badge>
            <h2
                style={{
                    fontSize: "clamp(30px, 4.5vw, 44px)",
                    fontWeight: 800,
                    marginTop: "20px",
                    lineHeight: 1.1,
                }}
            >
                {title}
            </h2>
            <p
                style={{
                    margin: "14px auto 0",
                    maxWidth: "760px",
                    color: "var(--text-muted)",
                    fontSize: "17px",
                    lineHeight: 1.65,
                }}
            >
                {sub}
            </p>
        </div>
    );
}

function CountUp({ value, suffix = "", duration = 1800 }) {
    const [n, setN] = useState(0);

    useEffect(() => {
        let start;
        const tick = (ts) => {
            if (!start) start = ts;
            const p = Math.min((ts - start) / duration, 1);
            setN(value * p);
            if (p < 1) requestAnimationFrame(tick);
        };
        requestAnimationFrame(tick);
    }, [value, duration]);

    const shown = Number.isInteger(value) ? Math.round(n).toLocaleString() : n.toFixed(1);
    return (
        <>
            {shown}
            {suffix}
        </>
    );
}

export default function DataMindShowcase() {
    const metrics = [
        { label: "Kafka TPS", value: 5240, suffix: "", icon: Activity, color: "var(--accent-teal)" },
        { label: "Predictive Lift", value: 24, suffix: "%", icon: TrendingUp, color: "var(--primary)" },
        { label: "Data Quality", value: 99.9, suffix: "%", icon: ShieldCheck, color: "var(--secondary)" },
        { label: "Latency", value: 0.8, suffix: "ms", icon: Zap, color: "var(--accent-amber)" },
    ];

    const architecture = [
        {
            title: "Ingestion (Hot)",
            detail: "Kafka + ClickHouse process high-volume events with zero-lock writes.",
            icon: Layers,
            color: "var(--primary)",
            impact: "5k+ txns/s",
        },
        {
            title: "Warehouse (Cold)",
            detail: "DuckDB + Parquet store analytical history in a star schema.",
            icon: Database,
            color: "var(--secondary)",
            impact: "Fast SQL analytics",
        },
        {
            title: "Intelligence Layer",
            detail: "LSTM forecasting + RAG context generate grounded business intelligence.",
            icon: Brain,
            color: "var(--accent-teal)",
            impact: "Forecast + narrative",
        },
        {
            title: "Action Layer",
            detail: "Agent orchestration converts insight into recommendations and reports.",
            icon: Target,
            color: "var(--accent-amber)",
            impact: "Decision automation",
        },
    ];

    const insightCards = [
        {
            title: "Demand Shock Radar",
            finding: "Detected early acceleration in high-velocity SKUs before stock pressure emerged.",
            action: "Raised reorder threshold by +18% for top-moving categories.",
            outcome: "Stockout risk reduced 31% → 12%",
        },
        {
            title: "Geo Revenue Drift",
            finding: "RAG + SQL surfaced a regional demand shift from UK core toward EU cluster.",
            action: "Rebalanced campaign allocation and routing strategy.",
            outcome: "Recovered ~£42k forecasted weekly revenue",
        },
        {
            title: "Promotion Fatigue",
            finding: "Repeat campaigns showed weak marginal conversion in saturated segments.",
            action: "Moved from blanket discounting to segment-personalized bundles.",
            outcome: "Conversion lift +11.6% in simulation",
        },
    ];

    return (
        <div style={{ overflowX: "hidden" }}>
            {/* HERO */}
            <section style={{ padding: "150px 24px 90px", textAlign: "center" }}>
                <Badge icon={Sparkles} type="accent">
                    DataMind Showcase
                </Badge>
                <h1
                    style={{
                        fontSize: "clamp(44px, 8vw, 92px)",
                        fontWeight: 900,
                        margin: "24px 0 18px",
                        lineHeight: 0.95,
                        letterSpacing: "-0.04em",
                    }}
                >
                    Retail Intelligence.
                    <br />
                    <span style={{ color: "var(--primary)" }}>Clean. Fast. Actionable.</span>
                </h1>
                <p
                    style={{
                        maxWidth: "760px",
                        margin: "0 auto",
                        color: "var(--text-muted)",
                        fontSize: "19px",
                        lineHeight: 1.7,
                    }}
                >
                    End-to-end AI pipeline: streaming ingestion, analytical warehouse, LSTM forecasting,
                    RAG-grounded reasoning, and agent-driven business actions.
                </p>

                <div
                    style={{
                        margin: "64px auto 0",
                        maxWidth: "1100px",
                        display: "grid",
                        gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
                        gap: "16px",
                    }}
                >
                    {metrics.map((m) => (
                        <Card key={m.label}>
                            <div style={{ color: m.color, marginBottom: "12px", display: "flex", justifyContent: "center" }}>
                                <m.icon size={24} />
                            </div>
                            <div style={{ fontSize: "34px", fontWeight: 800 }}>
                                <CountUp value={m.value} suffix={m.suffix} />
                            </div>
                            <div
                                className="mono"
                                style={{ marginTop: "8px", fontSize: "11px", color: "var(--text-dim)", letterSpacing: "0.08em" }}
                            >
                                {m.label}
                            </div>
                        </Card>
                    ))}
                </div>
            </section>

            {/* ARCHITECTURE */}
            <section style={{ padding: "90px 24px", background: "var(--surface-lowest)" }}>
                <div style={{ maxWidth: "1150px", margin: "0 auto" }}>
                    <SectionTitle
                        label="Architecture"
                        title="Hot/Cold Split — Fully Visible"
                        sub="No hidden clicks. Core flow is shown directly so recruiters can understand the system in one scan."
                    />

                    <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "16px" }}>
                        {architecture.map((item) => (
                            <Card key={item.title}>
                                <div style={{ display: "flex", gap: "14px", alignItems: "flex-start" }}>
                                    <div
                                        style={{
                                            width: "42px",
                                            height: "42px",
                                            borderRadius: "12px",
                                            background: "var(--surface-high)",
                                            color: item.color,
                                            display: "flex",
                                            alignItems: "center",
                                            justifyContent: "center",
                                            flexShrink: 0,
                                        }}
                                    >
                                        <item.icon size={20} />
                                    </div>
                                    <div>
                                        <div style={{ fontWeight: 800, fontSize: "18px" }}>{item.title}</div>
                                        <div style={{ color: "var(--text-muted)", marginTop: "8px", lineHeight: 1.6 }}>{item.detail}</div>
                                        <div className="mono" style={{ marginTop: "10px", fontSize: "11px", color: item.color, fontWeight: 700 }}>
                                            {item.impact}
                                        </div>
                                    </div>
                                </div>
                            </Card>
                        ))}
                    </div>
                </div>
            </section>

            {/* INSIGHTS */}
            <section style={{ padding: "90px 24px" }}>
                <div style={{ maxWidth: "1150px", margin: "0 auto" }}>
                    <SectionTitle
                        label="Insight Layer"
                        title="Signals → Decisions"
                        sub="Consistent format: finding, action, and measurable business outcome for every insight."
                    />

                    <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: "16px" }}>
                        {insightCards.map((card) => (
                            <Card key={card.title} style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
                                <Badge type="ghost">{card.title}</Badge>
                                <div style={{ color: "var(--text-muted)", lineHeight: 1.6 }}>{card.finding}</div>
                                <div style={{ fontWeight: 600 }}>{card.action}</div>
                                <div
                                    className="mono"
                                    style={{
                                        marginTop: "auto",
                                        color: "var(--accent-teal)",
                                        fontWeight: 700,
                                        fontSize: "11px",
                                        letterSpacing: "0.04em",
                                    }}
                                >
                                    {card.outcome}
                                </div>
                            </Card>
                        ))}
                    </div>
                </div>
            </section>

            {/* ML + RELIABILITY */}
            <section style={{ padding: "90px 24px", background: "var(--surface-lowest)" }}>
                <div style={{ maxWidth: "1100px", margin: "0 auto" }}>
                    <SectionTitle
                        label="Model"
                        title="LSTM + Attention Forecast Engine"
                        sub="Production-focused design with uncertainty estimation and reliability monitoring."
                    />

                    <Card>
                        <div style={{ display: "grid", gridTemplateColumns: "1.2fr 0.8fr", gap: "24px" }}>
                            <div>
                                <Badge icon={Cpu} type="accent">
                                    Forecast Core
                                </Badge>
                                <h3 style={{ marginTop: "16px", fontSize: "28px", fontWeight: 800 }}>Why this model is showcase-worthy</h3>
                                <ul style={{ listStyle: "none", padding: 0, margin: "16px 0 0", display: "grid", gap: "10px" }}>
                                    {[
                                        "PyTorch LSTM + attention for temporal dynamics",
                                        "Monte Carlo dropout for confidence intervals",
                                        "Feature engineering with cyclical seasonality",
                                        "Live scale calibration against streaming revenue",
                                    ].map((line) => (
                                        <li key={line} style={{ display: "flex", gap: "8px", alignItems: "flex-start" }}>
                                            <CheckCircle2 size={18} color="var(--accent-teal)" style={{ marginTop: "2px" }} />
                                            <span style={{ color: "var(--text-muted)", lineHeight: 1.6 }}>{line}</span>
                                        </li>
                                    ))}
                                </ul>
                            </div>

                            <div style={{ display: "grid", gap: "12px" }}>
                                {[
                                    { label: "Inference", value: "< 2.5s", icon: Zap, color: "var(--accent-amber)" },
                                    { label: "Confidence Band", value: "P05–P95", icon: AlertTriangle, color: "var(--secondary)" },
                                    { label: "Quality", value: "98.4%", icon: ShieldCheck, color: "var(--accent-teal)" },
                                ].map((kpi) => (
                                    <div key={kpi.label} className="glass" style={{ borderRadius: "16px", padding: "14px" }}>
                                        <div className="mono" style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "11px", color: kpi.color }}>
                                            <kpi.icon size={12} /> {kpi.label}
                                        </div>
                                        <div style={{ marginTop: "8px", fontSize: "24px", fontWeight: 800 }}>{kpi.value}</div>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </Card>
                </div>
            </section>
        </div>
    );
}
