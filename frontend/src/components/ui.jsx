import { Info, ServerOff } from 'lucide-react'
import { useToasts } from '../lib/api'

export function Card({ title, icon: Icon, hint, actions, children, className = '', glow }) {
  return (
    <section className={`card ${glow ? 'glow' : ''} ${className}`}>
      {(title || actions) && (
        <div className="card-head">
          <div className="card-title">
            {Icon && <Icon size={16} />}
            {title}
          </div>
          {actions && <div className="row">{actions}</div>}
        </div>
      )}
      {hint && <div className="card-hint">{hint}</div>}
      <div className="card-body">{children}</div>
    </section>
  )
}

export function Stat({ label, value, sub, color }) {
  return (
    <div className="stat" style={color ? { '--c': color } : undefined}>
      <div className="stat-label">{label}</div>
      <div className="stat-value" style={color ? { color } : undefined}>
        {value ?? '–'}
      </div>
      {sub && <div className="stat-sub">{sub}</div>}
    </div>
  )
}

export function Button({ children, kind = '', size = '', icon: Icon, busy, ...rest }) {
  return (
    <button className={`btn ${kind} ${size}`} disabled={busy || rest.disabled} {...rest}>
      {Icon && <Icon size={size === 'sm' ? 13 : 15} />}
      {children}
    </button>
  )
}

export function Badge({ kind = 'neutral', children, dot }) {
  return (
    <span className={`badge ${kind}`}>
      {dot && <span className={`dot ${kind}`} style={{ width: 6, height: 6, boxShadow: 'none' }} />}
      {children}
    </span>
  )
}

export function Segmented({ value, options, onChange }) {
  return (
    <div className="seg">
      {options.map((o) => {
        const [v, label] = Array.isArray(o) ? o : [o, o]
        return (
          <button key={v} className={value === v ? 'on' : ''} onClick={() => onChange(v)} type="button">
            {label}
          </button>
        )
      })}
    </div>
  )
}

export function Range({ label, value, min, max, step = 1, onChange, format = (v) => v }) {
  return (
    <label className="range">
      <div className="range-top">
        <span>{label}</span>
        <b>{format(value)}</b>
      </div>
      <input type="range" min={min} max={max} step={step} value={value} onChange={(e) => onChange(Number(e.target.value))} />
    </label>
  )
}

export function Switch({ on, onChange, label }) {
  return (
    <label className="row small" style={{ cursor: 'pointer', gap: 8 }}>
      <span className={`switch ${on ? 'on' : ''}`} onClick={() => onChange(!on)} role="switch" aria-checked={on} />
      {label}
    </label>
  )
}

export function Field({ label, children }) {
  return (
    <label className="field">
      <span>{label}</span>
      {children}
    </label>
  )
}

export function Callout({ kind = '', icon: Icon = Info, children }) {
  return (
    <div className={`callout ${kind}`}>
      <Icon size={16} />
      <div>{children}</div>
    </div>
  )
}

export function Offline({ project, port, error }) {
  return (
    <div className="offline">
      <ServerOff size={20} color="var(--warn)" />
      <div className="grow">
        <b>Backend {project} isn't answering on port {port}.</b>{' '}
        <span className="muted small">
          Start everything with <code>python dev.py</code> from the project root (or just this one with{' '}
          <code>python dev.py {project}</code>). {error ? `(${error.message})` : ''}
        </span>
      </div>
    </div>
  )
}

export function Toasts() {
  const items = useToasts()
  return (
    <div className="toasts">
      {items.map((t) => (
        <div key={t.id} className={`toast ${t.kind}`}>
          {t.message}
        </div>
      ))}
    </div>
  )
}

// ---- charts (hand-rolled SVG: no chart library needed) ----------------------------

// Stacked bars per time bucket. series: [{key, color}], rows: [{...values}]
export function StackedBars({ rows, series, height = 120, maxHint }) {
  const w = 600
  const max = Math.max(maxHint || 1, ...rows.map((r) => series.reduce((s, x) => s + (r[x.key] || 0), 0)))
  const bw = w / Math.max(rows.length, 1)
  return (
    <svg className="chart" viewBox={`0 0 ${w} ${height}`} preserveAspectRatio="none" style={{ height }}>
      {[0.25, 0.5, 0.75].map((f) => (
        <line key={f} x1="0" x2={w} y1={height * f} y2={height * f} stroke="var(--border)" strokeDasharray="3 4" />
      ))}
      {rows.map((r, i) => {
        let y = height
        return (
          <g key={i}>
            {series.map((s) => {
              const v = r[s.key] || 0
              const h = (v / max) * (height - 6)
              y -= h
              return v ? <rect key={s.key} x={i * bw + 1} y={y} width={Math.max(bw - 2, 1)} height={h} rx="1.5" fill={s.color} /> : null
            })}
          </g>
        )
      })}
    </svg>
  )
}

// Step line chart for balances over time. points: [{x, y}]
export function StepLine({ points, height = 150, color = 'var(--accent)', marker, zeroLine = true, fmt = (v) => v }) {
  const w = 640
  if (!points.length) return <div className="empty">No history yet</div>
  const xs = points.map((p) => p.x)
  const ys = points.map((p) => p.y)
  const x0 = Math.min(...xs)
  const x1 = Math.max(...xs, marker ?? -Infinity, x0 + 1)
  const yMin = Math.min(0, ...ys)
  const yMax = Math.max(...ys, 1)
  const pad = 10
  const X = (x) => pad + ((x - x0) / (x1 - x0)) * (w - pad * 2)
  const Y = (y) => height - pad - ((y - yMin) / (yMax - yMin || 1)) * (height - pad * 2)
  // balances only change at events, so draw horizontal steps, not diagonals
  let d = `M ${X(points[0].x)} ${Y(0)}`
  let prevY = 0
  for (const p of points) {
    d += ` L ${X(p.x)} ${Y(prevY)} L ${X(p.x)} ${Y(p.y)}`
    prevY = p.y
  }
  d += ` L ${X(x1)} ${Y(prevY)}`
  const area = `${d} L ${X(x1)} ${Y(yMin)} L ${X(points[0].x)} ${Y(yMin)} Z`
  return (
    <svg className="chart" viewBox={`0 0 ${w} ${height}`} style={{ height }}>
      <defs>
        <linearGradient id="fillgrad" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.28" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      {zeroLine && yMin < 0 && <line x1={pad} x2={w - pad} y1={Y(0)} y2={Y(0)} stroke="var(--bad)" strokeDasharray="4 4" />}
      <path d={area} fill="url(#fillgrad)" />
      <path d={d} fill="none" stroke={color} strokeWidth="2" strokeLinejoin="round" />
      {marker != null && (
        <g>
          <line x1={X(marker)} x2={X(marker)} y1={4} y2={height - 4} stroke="var(--text)" strokeOpacity="0.6" strokeDasharray="3 3" />
          <circle cx={X(marker)} cy={Y(valueAt(points, marker))} r="5" fill={color} stroke="var(--surface)" strokeWidth="2" />
        </g>
      )}
      <text x={pad} y={14} fill="var(--text-3)" fontSize="11" fontFamily="var(--mono)">
        {fmt(yMax)}
      </text>
    </svg>
  )
}

function valueAt(points, x) {
  let v = 0
  for (const p of points) {
    if (p.x <= x) v = p.y
    else break
  }
  return v
}
