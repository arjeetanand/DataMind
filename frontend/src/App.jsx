import { useState } from 'react';
import DataMindShowcase from './DataMindShowcase';
import DataMindDashboard from './DataMindDashboard';
import LiveFeed from './LiveFeed';
import { LayoutDashboard, Sparkles, Radio } from 'lucide-react';

function App() {
  const [view, setView] = useState('showcase');

  const NAV_ITEMS = [
    { id: 'showcase', label: 'Showcase', icon: Sparkles },
    { id: 'dashboard', label: 'Live Dashboard', icon: LayoutDashboard },
    { id: 'livefeed', label: 'Live Feed', icon: Radio },
  ];

  return (
    <div className="App" style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* Dynamic Background Glows */}
      <div style={{
        position: 'fixed', top: '-10%', left: '-10%', width: '40%', height: '40%',
        background: 'radial-gradient(circle, var(--primary-glow) 0%, transparent 70%)',
        zIndex: -1, pointerEvents: 'none', filter: 'blur(80px)'
      }} />
      <div style={{
        position: 'fixed', bottom: '-10%', right: '-10%', width: '40%', height: '40%',
        background: 'radial-gradient(circle, rgba(137, 206, 255, 0.1) 0%, transparent 70%)',
        zIndex: -1, pointerEvents: 'none', filter: 'blur(80px)'
      }} />

      {/* Navigation Pill */}
      <nav style={{
        position: 'fixed', top: '24px', left: '50%', transform: 'translateX(-50%)',
        zIndex: 1000, display: 'flex', alignItems: 'center', padding: '6px',
        borderRadius: '99px', background: 'rgba(19, 27, 46, 0.7)',
        backdropFilter: 'blur(16px)', border: '1px solid var(--border-ghost)',
        boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)'
      }}>
        {NAV_ITEMS.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => setView(id)}
            style={{
              display: 'flex', alignItems: 'center', gap: '8px',
              padding: '10px 20px', borderRadius: '99px', fontSize: '13px', fontWeight: 600,
              transition: 'all var(--duration-sm) var(--ease-out)',
              background: view === id ? 'var(--primary)' : 'transparent',
              color: view === id ? 'var(--bg)' : 'var(--text-muted)',
              border: 'none', cursor: 'pointer', position: 'relative'
            }}
          >
            <Icon size={14} />
            {label}
            {id === 'livefeed' && (
              <span style={{
                position: 'absolute', top: '8px', right: '12px',
                width: '6px', height: '6px', borderRadius: '50%',
                background: 'var(--accent-teal)',
                animation: 'pulse-glow 1.5s ease-in-out infinite'
              }} />
            )}
          </button>
        ))}
      </nav>

      <main style={{ flex: 1, position: 'relative' }}>
        {view === 'showcase' && <DataMindShowcase />}
        {view === 'dashboard' && <DataMindDashboard />}
        {view === 'livefeed' && <LiveFeed />}
      </main>

      <footer style={{
        padding: '32px 24px', textAlign: 'center', borderTop: '1px solid var(--border-ghost)',
        background: 'var(--surface-lowest)', color: 'var(--text-dim)', fontSize: '12px'
      }}>
        <div className="mono">github.com/arjeetanand/DataMind</div>
        <div style={{ marginTop: '8px' }}>Built with Arjeet Intelligence © 2026</div>
      </footer>
    </div>
  );
}

export default App;
