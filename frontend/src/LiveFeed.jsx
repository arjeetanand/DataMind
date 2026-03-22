import { useState, useEffect, useRef, useCallback } from "react";
import {
    ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip,
    ResponsiveContainer, Legend, Area, AreaChart, BarChart
} from "recharts";
import {
    Radio, Cpu, AlertCircle, Play, Pause, RotateCcw,
    TrendingUp, Users, Globe, ShoppingBag
} from "lucide-react";

const API = "http://localhost:8000";
const POLL_MS = 300;

const SPEEDS = [
    { id: "normal", label: "Normal", desc: "10s / day" },
    { id: "fast", label: "Fast", desc: "1s / day" },
    { id: "burst", label: "Burst", desc: "Max speed" },
];

const COUNTRY_FLAGS = {
    "United Kingdom": "🇬🇧", "Germany": "🇩🇪", "France": "🇫🇷",
    "Australia": "🇦🇺", "EIRE": "🇮🇪", "Spain": "🇪🇸",
    "Netherlands": "🇳🇱", "Belgium": "🇧🇪", "Sweden": "🇸🇪",
    "Switzerland": "🇨🇭", "Portugal": "🇵🇹", "Italy": "🇮🇹",
    "Norway": "🇳🇴", "Denmark": "🇩🇰", "Finland": "🇫🇮",
    "Japan": "🇯🇵", "USA": "🇺🇸", "Canada": "🇨🇦",
    "Singapore": "🇸🇬", "Brazil": "🇧🇷",
};

function fmt(n) {
    if (!n && n !== 0) return "—";
    if (n >= 1_000_000) return `£${(n / 1_000_000).toFixed(2)}M`;
    if (n >= 1_000) return `£${(n / 1_000).toFixed(1)}k`;
    return `£${n.toFixed(2)}`;
}

function fmtNum(n) {
    if (!n && n !== 0) return "—";
    return n.toLocaleString();
}

function ForecastTooltip({ active, payload, label }) {
    if (!active || !payload?.length) return null;
    const data = payload[0]?.payload || {};
    return (
        <div style={{
            background: "var(--surface-high)", border: "1px solid var(--border-ghost)",
            borderRadius: "12px", padding: "14px 18px", fontSize: "12px", minWidth: "180px"
        }}>
            <div style={{ color: "var(--text-dim)", marginBottom: "10px", fontSize: "11px" }}>{label}</div>
            {data.actual > 0 && (
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "6px" }}>
                    <span style={{ color: "var(--accent-teal)" }}>Actual</span>
                    <span style={{ fontFamily: "JetBrains Mono", fontWeight: 700 }}>{fmt(data.actual)}</span>
                </div>
            )}
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "6px" }}>
                <span style={{ color: "var(--primary)" }}>Forecast</span>
                <span style={{ fontFamily: "JetBrains Mono" }}>{fmt(data.predicted)}</span>
            </div>
            {data.actual > 0 && data.ape_pct > 0 && (
                <div style={{
                    marginTop: "10px", paddingTop: "10px", borderTop: "1px solid var(--border-ghost)",
                    display: "flex", justifyContent: "space-between", alignItems: "center"
                }}>
                    <span style={{ color: "var(--text-dim)" }}>Error</span>
                    <span style={{
                        color: data.ape_pct < 10 ? "var(--accent-teal)" : data.ape_pct < 20 ? "var(--accent-amber)" : "var(--accent-coral)",
                        fontFamily: "JetBrains Mono", fontWeight: 700
                    }}>
                        {data.ape_pct}%
                    </span>
                </div>
            )}
        </div>
    );
}

function TransactionTicker({ transactions }) {
    const prevRef = useRef([]);
    const newIds = new Set(
        transactions.filter(t => !prevRef.current.find(p => p.id === t.id)).map(t => t.id)
    );
    useEffect(() => { prevRef.current = transactions; }, [transactions]);

    return (
        <div style={{ display: "flex", flexDirection: "column", gap: "1px" }}>
            {transactions.map((txn) => (
                <div
                    key={txn.id}
                    style={{
                        display: "flex", alignItems: "center", gap: "12px",
                        padding: "10px 12px", borderRadius: "8px",
                        background: newIds.has(txn.id) ? "rgba(192,193,255,0.06)" : "transparent",
                        borderLeft: newIds.has(txn.id) ? "2px solid var(--primary)" : "2px solid transparent",
                        transition: "all 0.4s ease",
                    }}
                >
                    <div style={{ fontSize: "16px", width: "24px", textAlign: "center", flexShrink: 0 }}>
                        {COUNTRY_FLAGS[txn.country] || "🌍"}
                    </div>
                    <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{
                            fontSize: "12px", fontWeight: 600, color: "var(--text)",
                            whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis"
                        }}>
                            {txn.description}
                        </div>
                        <div style={{ fontSize: "11px", color: "var(--text-dim)", marginTop: "1px" }}>
                            {txn.country} · qty {txn.quantity}
                        </div>
                    </div>
                    <div style={{ textAlign: "right", flexShrink: 0 }}>
                        <div style={{
                            fontSize: "13px", fontWeight: 700, fontFamily: "JetBrains Mono",
                            color: txn.revenue > 100 ? "var(--accent-teal)" : "var(--text)"
                        }}>
                            {fmt(txn.revenue)}
                        </div>
                        <div className="mono" style={{ fontSize: "10px", color: "var(--text-dim)" }}>
                            {txn.simulated_day}
                        </div>
                    </div>
                </div>
            ))}
        </div>
    );
}

function LiveKPI({ label, value, sub, icon: Icon, color = "var(--primary)" }) {
    return (
        <div className="glass" style={{ padding: "16px 20px", borderRadius: "14px", flex: 1, minWidth: "140px" }}>
            <div style={{ width: "32px", height: "32px", borderRadius: "8px", background: "var(--surface-high)", display: "flex", alignItems: "center", justifyContent: "center", color, marginBottom: "12px" }}>
                <Icon size={16} />
            </div>
            <div style={{ fontSize: "20px", fontWeight: 700, fontFamily: "JetBrains Mono", lineHeight: 1 }}>{value}</div>
            <div style={{ fontSize: "11px", color: "var(--text-dim)", marginTop: "4px" }}>{label}</div>
            {sub && <div style={{ fontSize: "10px", color: "var(--accent-teal)", marginTop: "2px" }}>{sub}</div>}
        </div>
    );
}

function LiveDot({ active }) {
    return (
        <span style={{ position: "relative", display: "inline-flex", width: "10px", height: "10px" }}>
            <span style={{
                position: "absolute", inset: 0, borderRadius: "50%",
                background: active ? "var(--accent-teal)" : "var(--text-dim)",
                animation: active ? "pulse-glow 1.5s ease-in-out infinite" : "none",
                opacity: active ? 0.6 : 0.4
            }} />
            <span style={{
                position: "relative", width: "10px", height: "10px", borderRadius: "50%",
                background: active ? "var(--accent-teal)" : "var(--text-dim)"
            }} />
        </span>
    );
}

export default function LiveFeed() {
    const [status, setStatus] = useState(null);
    const [kpis, setKpis] = useState(null);
    const [transactions, setTransactions] = useState([]);
    const [forecastData, setForecastData] = useState([]);
    const [revenueData, setRevenueData] = useState([]);
    const [topProducts, setTopProducts] = useState([]);
    const [forecastOutlook, setForecastOutlook] = useState([]);
    const [geoRevenue, setGeoRevenue] = useState([]);
    const [speed, setSpeed] = useState("normal");
    const [speedLoading, setSpeedLoading] = useState(false);
    const [error, setError] = useState(null);
    const [lastPoll, setLastPoll] = useState(null);
    const pollRef = useRef(null);

    const fetchAll = useCallback(async () => {
        try {
            const [s, k, t, f, r, p, p2, g] = await Promise.all([
                fetch(`${API}/live/status`).then(r => r.json()).catch(() => null),
                fetch(`${API}/live/kpis`).then(r => r.json()).catch(() => null),
                fetch(`${API}/live/transactions?n=20`).then(r => r.json()).catch(() => null),
                fetch(`${API}/live/forecast-vs-actual`).then(r => r.json()).catch(() => null),
                fetch(`${API}/live/revenue?window=200`).then(r => r.json()).catch(() => null),
                fetch(`${API}/live/top-products?n=5`).then(r => r.json()).catch(() => null),
                fetch(`${API}/live/forecast-outlook`).then(r => r.json()).catch(() => null),
                fetch(`${API}/live/geo-revenue?n=8`).then(r => r.json()).catch(() => null),
            ]);

            if (s) { setStatus(s); setSpeed(s.speed_mode || "normal"); }
            if (k) setKpis(k);
            if (t?.data) setTransactions(t.data);
            if (f?.data) setForecastData(f.data.map(d => ({
                ...d,
                day: d.day?.slice(5) || d.day,
                actual: d.actual || 0,
                predicted: d.predicted || 0,
            })));
            if (r?.data) setRevenueData(r.data.map(d => ({
                ...d,
                day: String(d.simulated_day).slice(5),
                rev: d.daily_revenue || 0,
            })));
            if (p?.data) setTopProducts(p.data);
            if (p2?.data) setForecastOutlook(p2.data.map(d => ({
                ...d,
                day: d.day?.slice(5) || d.day,
            })));
            if (g?.data) setGeoRevenue(g.data);

            setError(null);
            setLastPoll(new Date());
        } catch (e) {
            setError("Cannot reach API. Is FastAPI running on port 8000?");
        }
    }, []);

    useEffect(() => {
        fetchAll();
        pollRef.current = setInterval(fetchAll, 1000);
        return () => clearInterval(pollRef.current);
    }, [fetchAll]);

    const changeSpeed = async (newSpeed) => {
        setSpeedLoading(true);
        try {
            await fetch(`${API}/live/control/speed`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ speed_mode: newSpeed }),
            });
            setSpeed(newSpeed);
        } catch (e) {
            console.error("Speed change failed", e);
        } finally {
            setSpeedLoading(false);
        }
    };

    const toggleSimulation = async () => {
        const action = isLive ? "stop" : "start";
        setSpeedLoading(true);
        try {
            await fetch(`${API}/live/${action}`, { method: "POST" });
            fetchAll();
        } catch (e) {
            console.error(`${action} failed`, e);
        } finally {
            setSpeedLoading(false);
        }
    };

    const resetLiveData = async () => {
        if (!window.confirm("Hard Reset: Wiping all live data and clearing Kafka. Proceed?")) return;
        setSpeedLoading(true);
        try {
            // First stop simulation
            await fetch(`${API}/live/stop`, { method: "POST" });
            // Then reset
            await fetch(`${API}/live/reset`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ confirm: true }),
            });
            alert("System Reset Successfully. You can now press Play to start a new simulation.");
            fetchAll();
        } catch (e) {
            console.error("Reset failed", e);
        } finally {
            setSpeedLoading(false);
        }
    };

    const isLive = status?.is_running;
    const hasData = transactions.length > 0;
    const mape = status?.mape;

    return (
        <div className="fade-up" style={{ padding: "100px 24px 60px", maxWidth: "1400px", margin: "0 auto", position: "relative", zIndex: 1 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "32px", flexWrap: "wrap", gap: "16px" }}>
                <div>
                    <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "8px" }}>
                        <LiveDot active={isLive} />
                        <h1 style={{ fontSize: "28px", fontWeight: 800, letterSpacing: "-0.02em", margin: 0 }}>Live Feed</h1>
                    </div>
                    <div style={{ color: "var(--text-dim)", fontSize: "13px" }}>
                        {isLive
                            ? `Simulated date: ${status?.current_day || "—"} · ${fmtNum(status?.total_rows)} rows ingested · ${status?.days_streamed} days complete`
                            : "Start the producer and consumer to begin live streaming"}
                    </div>
                </div>

                <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
                    <span style={{ fontSize: "12px", color: "var(--text-dim)", marginRight: "4px" }}>Speed:</span>
                    {SPEEDS.map(s => (
                        <button
                            key={s.id}
                            onClick={() => changeSpeed(s.id)}
                            disabled={speedLoading}
                            title={s.desc}
                            style={{
                                padding: "8px 16px", borderRadius: "8px", fontSize: "12px", fontWeight: 700,
                                border: "1px solid",
                                borderColor: speed === s.id ? "var(--primary)" : "var(--border-ghost)",
                                background: speed === s.id ? "rgba(192,193,255,0.15)" : "transparent",
                                color: speed === s.id ? "var(--primary)" : "var(--text-dim)",
                                cursor: speedLoading ? "not-allowed" : "pointer",
                            }}
                        >
                            {s.label}
                        </button>
                    ))}
                    <div style={{ display: "flex", gap: "10px", alignItems: "center", marginLeft: "16px" }}>
                        <button
                            onClick={toggleSimulation}
                            disabled={speedLoading}
                            className="glass"
                            style={{
                                padding: "10px 20px", borderRadius: "12px", border: "1px solid var(--border-ghost)",
                                background: isLive ? "rgba(255, 107, 107, 0.15)" : "rgba(32, 201, 151, 0.15)",
                                color: isLive ? "var(--accent-coral)" : "var(--accent-teal)",
                                display: "flex", alignItems: "center", gap: "8px", cursor: "pointer", fontWeight: 600,
                                transition: "all 0.2s"
                            }}
                        >
                            {isLive ? <Pause size={16} /> : <Play size={16} />}
                            {isLive ? "Stop Simulation" : "Start Simulation"}
                        </button>

                        <button
                            onClick={resetLiveData}
                            disabled={speedLoading}
                            className="glass"
                            style={{
                                padding: "10px 14px", borderRadius: "12px", border: "1px solid var(--border-ghost)",
                                background: "var(--surface-high)", color: "var(--text-dim)",
                                display: "flex", alignItems: "center", gap: "6px", cursor: "pointer", fontSize: "13px"
                            }}
                            title="Reset Database & Kafka"
                        >
                            <RotateCcw size={14} />
                            Reset
                        </button>

                        <div style={{ width: "1px", height: "24px", background: "var(--border-ghost)", margin: "0 10px" }} />
                    </div>
                </div>
            </div>

            {error && (
                <div style={{
                    padding: "14px 18px", borderRadius: "12px", marginBottom: "24px",
                    background: "rgba(248, 113, 113, 0.08)", border: "1px solid rgba(248,113,113,0.25)",
                    display: "flex", alignItems: "center", gap: "10px", color: "var(--accent-coral)"
                }}>
                    <AlertCircle size={16} />
                    <span style={{ fontSize: "13px" }}>{error}</span>
                </div>
            )}

            {!hasData && !error && (
                <div className="glass" style={{ padding: "40px", borderRadius: "20px", textAlign: "center", marginBottom: "32px" }}>
                    <div style={{ width: "56px", height: "56px", borderRadius: "14px", margin: "0 auto 16px", background: "var(--surface-high)", display: "flex", alignItems: "center", justifyContent: "center", color: "var(--primary)" }}>
                        <Radio size={24} />
                    </div>
                    <h3 style={{ fontSize: "18px", fontWeight: 700, marginBottom: "8px" }}>No live data yet</h3>
                    <p style={{ color: "var(--text-dim)", fontSize: "14px", maxWidth: "420px", margin: "0 auto 24px" }}>
                        Spin up Kafka, then run the producer and consumer in separate terminals.
                    </p>
                </div>
            )}

            {kpis && (
                <div style={{ display: "flex", gap: "16px", marginBottom: "28px", flexWrap: "wrap" }}>
                    <LiveKPI label="Live Revenue" value={fmt(kpis.total_live_revenue)} icon={TrendingUp} color="var(--accent-teal)" />
                    <LiveKPI label="Transactions" value={fmtNum(kpis.total_txns)} icon={Radio} color="var(--primary)" />
                    <LiveKPI label="Customers" value={fmtNum(kpis.unique_customers)} icon={Users} color="var(--secondary)" />
                    <LiveKPI label="Countries" value={kpis.countries} icon={Globe} color="var(--tertiary)" />
                    <LiveKPI label="Avg Transaction" value={fmt(kpis.avg_txn_value)} icon={ShoppingBag} color="var(--accent-amber)" />
                    {mape !== null && mape !== undefined && (
                        <LiveKPI
                            label="Forecast MAPE"
                            value={`${mape}%`}
                            sub={mape < 15 ? "✓ Good accuracy" : mape < 25 ? "Fair accuracy" : "High error"}
                            icon={Cpu}
                            color={mape < 15 ? "var(--accent-teal)" : mape < 25 ? "var(--accent-amber)" : "var(--accent-coral)"}
                        />
                    )}
                </div>
            )}

            {hasData && (
                <div style={{ display: "grid", gridTemplateColumns: "1fr 360px", gap: "24px" }}>
                    <div style={{ display: "flex", flexDirection: "column", gap: "24px", minWidth: 0 }}>
                        <div className="glass" style={{ padding: "24px", borderRadius: "20px" }}>
                            <div style={{ marginBottom: "20px" }}>
                                <h3 style={{ fontSize: "18px", fontWeight: 700, margin: 0 }}>Forecast vs Actual</h3>
                                <p style={{ color: "var(--text-dim)", fontSize: "13px", marginTop: "4px" }}>
                                    LSTM predictions (line) against actual streamed revenue (bars).
                                </p>
                            </div>

                            {forecastData.length === 0 ? (
                                <div style={{ height: "320px", display: "flex", alignItems: "center", justifyContent: "center" }}>
                                    <div style={{ textAlign: "center", color: "var(--text-dim)" }}>
                                        <Cpu size={28} style={{ marginBottom: "8px", opacity: 0.4 }} />
                                        <div style={{ fontSize: "13px" }}>Forecast data will appear as days stream in</div>
                                    </div>
                                </div>
                            ) : (
                                <div style={{ height: "320px" }}>
                                    <ResponsiveContainer width="100%" height="100%">
                                        <ComposedChart data={forecastData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                                            <CartesianGrid strokeDasharray="3 3" stroke="var(--border-ghost)" vertical={false} />
                                            <XAxis dataKey="day" stroke="var(--text-dim)" fontSize={10} tick={{ fill: "var(--text-dim)" }} interval="preserveStartEnd" />
                                            <YAxis stroke="var(--text-dim)" fontSize={10} tick={{ fill: "var(--text-dim)" }} tickFormatter={v => `£${Math.round(v / 1000)}k`} />
                                            <Tooltip content={<ForecastTooltip />} />
                                            <Legend wrapperStyle={{ fontSize: "11px", color: "var(--text-dim)", paddingTop: "8px" }} />
                                            <Bar dataKey="actual" name="Actual" fill="var(--accent-teal)" fillOpacity={0.7} radius={[3, 3, 0, 0]} barSize={14} />
                                            <Line type="monotone" dataKey="predicted" name="LSTM Forecast" stroke="var(--primary)" strokeWidth={2} dot={{ r: 3, fill: "var(--primary)", strokeWidth: 0 }} activeDot={{ r: 5 }} />
                                            <Line type="monotone" dataKey="upper_ci" name="Upper CI" stroke="var(--primary)" strokeWidth={1} strokeOpacity={0.3} dot={false} strokeDasharray="4 3" legendType="none" />
                                            <Line type="monotone" dataKey="lower_ci" name="Lower CI" stroke="var(--primary)" strokeWidth={1} strokeOpacity={0.3} dot={false} strokeDasharray="4 3" legendType="none" />
                                        </ComposedChart>
                                    </ResponsiveContainer>
                                </div>
                            )}
                        </div>

                        <div className="glass" style={{ padding: "24px", borderRadius: "20px" }}>
                            <div style={{ marginBottom: "20px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                                <div>
                                    <h3 style={{ fontSize: "18px", fontWeight: 700, margin: 0 }}>7-Day Revenue Outlook</h3>
                                    <p style={{ color: "var(--text-dim)", fontSize: "13px", marginTop: "4px" }}>
                                        Next 7 days model projection (Forward looking).
                                    </p>
                                </div>
                                <div style={{ padding: "4px 10px", borderRadius: "20px", background: "rgba(192,193,255,0.1)", color: "var(--primary)", fontSize: "11px", fontWeight: 700 }}>
                                    Live Inference
                                </div>
                            </div>

                            {forecastOutlook.length === 0 ? (
                                <div style={{ height: "240px", display: "flex", alignItems: "center", justifyContent: "center" }}>
                                    <div style={{ textAlign: "center", color: "var(--text-dim)" }}>
                                        <TrendingUp size={28} className="pulse" style={{ marginBottom: "8px", opacity: 0.4 }} />
                                        <div style={{ fontSize: "13px" }}>Connecting to model engine...</div>
                                    </div>
                                </div>
                            ) : (
                                <div style={{ height: "240px" }}>
                                    <ResponsiveContainer width="100%" height="100%">
                                        <AreaChart data={forecastOutlook} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                                            <defs>
                                                <linearGradient id="outlookGrad" x1="0" y1="0" x2="0" y2="1">
                                                    <stop offset="5%" stopColor="var(--primary)" stopOpacity={0.4} />
                                                    <stop offset="95%" stopColor="var(--primary)" stopOpacity={0} />
                                                </linearGradient>
                                            </defs>
                                            <CartesianGrid strokeDasharray="3 3" stroke="var(--border-ghost)" vertical={false} />
                                            <XAxis dataKey="day" stroke="var(--text-dim)" fontSize={10} tick={{ fill: "var(--text-dim)" }} />
                                            <YAxis stroke="var(--text-dim)" fontSize={10} tick={{ fill: "var(--text-dim)" }} tickFormatter={v => `£${Math.round(v / 1000)}k`} />
                                            <Tooltip content={<ForecastTooltip />} />
                                            <Area type="monotone" dataKey="predicted" name="Forecast" stroke="var(--primary)" strokeWidth={3} fill="url(#outlookGrad)" />
                                            <Area type="monotone" dataKey="upper_ci" stroke="none" fill="var(--primary)" fillOpacity={0.05} />
                                            <Area type="monotone" dataKey="lower_ci" stroke="none" fill="var(--primary)" fillOpacity={0.05} />
                                        </AreaChart>
                                    </ResponsiveContainer>
                                </div>
                            )}
                        </div>

                        {revenueData.length > 0 && (
                            <div className="glass" style={{ padding: "24px", borderRadius: "20px" }}>
                                <h3 style={{ fontSize: "18px", fontWeight: 700, marginBottom: "4px" }}>Daily Revenue (Streaming Window)</h3>
                                <p style={{ color: "var(--text-dim)", fontSize: "13px", marginBottom: "20px" }}>Revenue for most recent streamed days</p>
                                <div style={{ height: "200px" }}>
                                    <ResponsiveContainer width="100%" height="100%">
                                        <AreaChart data={revenueData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                                            <defs>
                                                <linearGradient id="liveGrad" x1="0" y1="0" x2="0" y2="1">
                                                    <stop offset="5%" stopColor="var(--accent-teal)" stopOpacity={0.3} />
                                                    <stop offset="95%" stopColor="var(--accent-teal)" stopOpacity={0} />
                                                </linearGradient>
                                            </defs>
                                            <CartesianGrid strokeDasharray="3 3" stroke="var(--border-ghost)" vertical={false} />
                                            <XAxis dataKey="day" fontSize={10} stroke="var(--text-dim)" tick={{ fill: "var(--text-dim)" }} interval="preserveStartEnd" />
                                            <YAxis fontSize={10} stroke="var(--text-dim)" tick={{ fill: "var(--text-dim)" }} tickFormatter={v => `£${Math.round(v / 1000)}k`} />
                                            <Tooltip contentStyle={{ background: "var(--surface-high)", border: "1px solid var(--border-ghost)", borderRadius: "8px", fontSize: "12px" }} formatter={(v) => [fmt(v), "Revenue"]} />
                                            <Area type="monotone" dataKey="rev" stroke="var(--accent-teal)" strokeWidth={2} fill="url(#liveGrad)" />
                                        </AreaChart>
                                    </ResponsiveContainer>
                                </div>
                            </div>
                        )}

                        {geoRevenue.length > 0 && (
                            <div className="glass" style={{ padding: "24px", borderRadius: "20px" }}>
                                <div style={{ marginBottom: "20px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                                    <div>
                                        <h3 style={{ fontSize: "18px", fontWeight: 700, margin: 0 }}>Live Market Presence</h3>
                                        <p style={{ color: "var(--text-dim)", fontSize: "13px", marginTop: "4px" }}>
                                            Real-time geographic revenue distribution.
                                        </p>
                                    </div>
                                    <Globe size={20} style={{ color: "var(--primary)", opacity: 0.6 }} />
                                </div>
                                <div style={{ height: "240px" }}>
                                    <ResponsiveContainer width="100%" height="100%">
                                        <BarChart
                                            data={geoRevenue}
                                            layout="vertical"
                                            margin={{ top: 5, right: 30, left: 40, bottom: 5 }}
                                        >
                                            <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="var(--border-ghost)" />
                                            <XAxis type="number" hide />
                                            <YAxis
                                                dataKey="country"
                                                type="category"
                                                width={100}
                                                fontSize={11}
                                                tick={{ fill: "var(--text-dim)" }}
                                                tickFormatter={(v) => `${COUNTRY_FLAGS[v] || "🌍"} ${v}`}
                                            />
                                            <Tooltip
                                                cursor={{ fill: "rgba(192,193,255,0.05)" }}
                                                contentStyle={{ background: "var(--surface-high)", border: "1px solid var(--border-ghost)", borderRadius: "8px", fontSize: "12px" }}
                                                formatter={(v) => [fmt(v), "Revenue"]}
                                            />
                                            <Bar
                                                dataKey="total_revenue"
                                                name="Revenue"
                                                fill="var(--primary)"
                                                radius={[0, 4, 4, 0]}
                                                barSize={18}
                                            />
                                        </BarChart>
                                    </ResponsiveContainer>
                                </div>
                            </div>
                        )}

                        {topProducts.length > 0 && (
                            <div className="glass" style={{ padding: "24px", borderRadius: "20px" }}>
                                <h3 style={{ fontSize: "18px", fontWeight: 700, marginBottom: "16px" }}>Top Products (Live Period)</h3>
                                <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
                                    {topProducts.map((p, i) => {
                                        const maxRev = topProducts[0]?.total_revenue || 1;
                                        const pct = (p.total_revenue / maxRev) * 100;
                                        return (
                                            <div key={i} style={{ display: "flex", alignItems: "center", gap: "12px" }}>
                                                <div className="mono" style={{ width: "20px", fontSize: "11px", color: "var(--text-dim)", textAlign: "right" }}>{i + 1}</div>
                                                <div style={{ flex: 1, minWidth: 0 }}>
                                                    <div style={{ fontSize: "12px", fontWeight: 600, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{p.description}</div>
                                                    <div style={{ height: "4px", background: "var(--surface-highest)", borderRadius: "2px", marginTop: "4px", overflow: "hidden" }}>
                                                        <div style={{ height: "100%", width: `${pct}%`, background: "var(--primary)", transition: "width 0.6s ease" }} />
                                                    </div>
                                                </div>
                                                <div className="mono" style={{ fontSize: "12px", fontWeight: 700, flexShrink: 0 }}>{fmt(p.total_revenue)}</div>
                                            </div>
                                        );
                                    })}
                                </div>
                            </div>
                        )}
                    </div>

                    <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
                        <div className="glass" style={{ padding: "20px", borderRadius: "20px", flex: 1, maxHeight: "calc(100vh - 200px)", display: "flex", flexDirection: "column" }}>
                            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px", flexShrink: 0 }}>
                                <div>
                                    <h3 style={{ fontSize: "16px", fontWeight: 700, margin: 0 }}>Live Transactions</h3>
                                    <div style={{ fontSize: "11px", color: "var(--text-dim)", marginTop: "2px" }}>Polls every {POLL_MS / 1000}s</div>
                                </div>
                                <div style={{ width: "8px", height: "8px", borderRadius: "50%", background: isLive ? "var(--accent-teal)" : "var(--text-dim)", animation: isLive ? "pulse-glow 1.5s ease-in-out infinite" : "none" }} />
                            </div>

                            <div style={{ flex: 1, overflowY: "auto", paddingRight: "4px" }}>
                                {transactions.length === 0 ? (
                                    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "200px", color: "var(--text-dim)", fontSize: "13px" }}>Waiting for transactions...</div>
                                ) : (
                                    <TransactionTicker transactions={transactions} />
                                )}
                            </div>

                            {lastPoll && (
                                <div className="mono" style={{ fontSize: "10px", color: "var(--text-dim)", marginTop: "12px", paddingTop: "12px", borderTop: "1px solid var(--border-ghost)", flexShrink: 0 }}>
                                    Last updated: {lastPoll.toLocaleTimeString()}
                                </div>
                            )}
                        </div>

                        {status && (
                            <div className="glass" style={{ padding: "16px", borderRadius: "14px" }}>
                                <div style={{ fontSize: "12px", fontWeight: 700, marginBottom: "12px", color: "var(--text-dim)", letterSpacing: "0.06em", textTransform: "uppercase" }}>Stream State</div>
                                {[
                                    ["Mode", SPEEDS.find(s => s.id === speed)?.desc || speed],
                                    ["Day", status.current_day || "—"],
                                    ["Days in", fmtNum(status.days_streamed)],
                                    ["Rows", fmtNum(status.total_rows)],
                                ].map(([k, v]) => (
                                    <div key={k} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "6px 0", borderBottom: "1px solid var(--border-ghost)" }}>
                                        <span style={{ fontSize: "12px", color: "var(--text-dim)" }}>{k}</span>
                                        <span className="mono" style={{ fontSize: "12px", fontWeight: 600 }}>{v}</span>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}
