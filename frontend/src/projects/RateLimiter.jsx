import { Activity, Gauge, Play, Rocket, Send, Settings2, Square, Users, Zap } from 'lucide-react'
import { Fragment, useEffect, useMemo, useRef, useState } from 'react'
import { Badge, Button, Callout, Card, Field, Range, Segmented, StackedBars, Stat } from '../components/ui'
import { api, toast, useAction, usePoll } from '../lib/api'

const A = api(1)

function Bucket({ state }) {
  const cap = state?.capacity || 1
  const avail = Math.max(0, Math.min(cap, state?.available ?? 0))
  const frac = avail / cap
  const h = 120
  const level = 30 + h * (1 - frac)
  const sliding = state?.mode === 'sliding_window'
  return (
    <div className="bucket">
      <svg viewBox="0 0 140 170">
        <defs>
          <linearGradient id="liq" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0" stopColor="var(--accent)" stopOpacity="0.95" />
            <stop offset="1" stopColor="var(--accent)" stopOpacity="0.45" />
          </linearGradient>
          <clipPath id="glass">
            <path d="M18 28 L26 158 Q27 164 34 164 L106 164 Q113 164 114 158 L122 28 Z" />
          </clipPath>
        </defs>
        <g clipPath="url(#glass)">
          <rect x="0" y="0" width="140" height="170" fill="var(--surface-2)" />
          <rect x="0" y={level} width="140" height={170 - level} fill="url(#liq)" style={{ transition: 'y 0.18s linear, height 0.18s linear' }} />
          <path
            d={`M0 ${level} Q 35 ${level - 5} 70 ${level} T 140 ${level}`}
            fill="none"
            stroke="white"
            strokeOpacity="0.35"
            strokeWidth="2"
            style={{ transition: 'd 0.18s linear' }}
          />
        </g>
        <path d="M18 28 L26 158 Q27 164 34 164 L106 164 Q113 164 114 158 L122 28" fill="none" stroke="var(--border-strong)" strokeWidth="2.5" />
        <ellipse cx="70" cy="28" rx="52" ry="7" fill="none" stroke="var(--border-strong)" strokeWidth="2" />
        <text x="70" y="104" textAnchor="middle" fontSize="26" fontWeight="800" fill="var(--text)" fontFamily="var(--font)">
          {sliding ? Math.round(avail) : avail.toFixed(1)}
        </text>
        <text x="70" y="122" textAnchor="middle" fontSize="11" fill="var(--text-2)" fontFamily="var(--font)">
          of {cap} {sliding ? 'left in window' : 'tokens'}
        </text>
      </svg>
    </div>
  )
}

export default function RateLimiter() {
  const [key, setKey] = useState('mobile-app')
  const [cfg, setCfg] = useState({ mode: 'token_bucket', rate: 2, burst: 10, limit: 10, window: 5 })
  const [ticks, setTicks] = useState([])
  const [last, setLast] = useState(null)
  const [streamRps, setStreamRps] = useState(8)
  const [streaming, setStreaming] = useState(false)
  const [run, busy] = useAction()
  const [lt, setLt] = useState({ rps: 600, duration: 4, mode: 'token_bucket' })
  const [ltResult, setLtResult] = useState(null)
  const [ltBusy, setLtBusy] = useState(false)
  const streamRef = useRef(null)

  const [client] = usePoll(() => A.get(`/admin/clients/${encodeURIComponent(key)}`), 150, [key])
  const [stats] = usePoll(() => A.get('/stats?seconds=60'), 1000)
  const [configs, , refreshConfigs] = usePoll(() => A.get('/admin/clients'), 4000)

  // load the stored config when the key changes
  useEffect(() => {
    A.get(`/admin/clients/${encodeURIComponent(key)}`)
      .then((c) => c && setCfg(c.config))
      .catch(() => {})
  }, [key])

  const fire = async (n = 1) => {
    const results = await Promise.all(
      Array.from({ length: n }, () =>
        A.raw('GET', `/check/${encodeURIComponent(key)}`)
          .then(async (r) => ({ ok: r.status === 200, status: r.status, headers: r.headers, body: await r.json() }))
          .catch(() => null),
      ),
    )
    const good = results.filter(Boolean)
    if (!good.length) return toast('Rate limiter is not reachable', 'bad')
    setTicks((t) => [...t, ...good.map((r) => (r.ok ? 'a' : 'd'))].slice(-90))
    const r = good[good.length - 1]
    setLast({
      status: r.status,
      decision: r.body.decision,
      headers: ['X-RateLimit-Limit', 'X-RateLimit-Remaining', 'X-RateLimit-Reset', 'X-RateLimit-Mode', 'Retry-After']
        .map((h) => [h, r.headers.get(h)])
        .filter(([, v]) => v !== null),
    })
  }

  useEffect(() => {
    if (!streaming) return
    streamRef.current = setInterval(() => fire(1), 1000 / streamRps)
    return () => clearInterval(streamRef.current)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [streaming, streamRps, key])

  const save = () =>
    run(async () => {
      await A.put(`/admin/clients/${encodeURIComponent(key)}`, cfg)
      setTicks([])
      refreshConfigs()
    }, `Saved limits for "${key}" (bucket reset)`)

  const runLoad = async () => {
    setLtBusy(true)
    setLtResult(null)
    try {
      const config =
        lt.mode === 'token_bucket'
          ? { mode: 'token_bucket', rate: 50, burst: 100, limit: 80, window: 1 }
          : { mode: 'sliding_window', rate: 5, burst: 10, limit: 80, window: 1 }
      setLtResult(await A.post('/loadtest', { key: `loadtest-${lt.mode}`, rps: lt.rps, duration: lt.duration, config }))
    } catch (e) {
      toast(e.message, 'bad')
    } finally {
      setLtBusy(false)
    }
  }

  const series = useMemo(() => stats?.series?.[key] || [], [stats, key])
  const totals = stats?.totals || {}
  const allowed = ticks.filter((t) => t === 'a').length
  const sliding = cfg.mode === 'sliding_window'

  return (
    <div className="grid" style={{ gridTemplateColumns: 'minmax(0, 1.05fr) minmax(0, 1fr)' }}>
      <div className="col" style={{ gap: 16 }}>
        <Card title="Client & limits" icon={Settings2} hint="Each client key gets its own limits (R2). Saving resets that client's bucket.">
          <div className="grid g2">
            <Field label="Client key">
              <input className="input mono" value={key} onChange={(e) => setKey(e.target.value.trim() || 'mobile-app')} />
            </Field>
            <Field label="Algorithm">
              <Segmented
                value={cfg.mode}
                onChange={(mode) => setCfg({ ...cfg, mode })}
                options={[
                  ['token_bucket', 'Token bucket'],
                  ['sliding_window', 'Sliding window'],
                ]}
              />
            </Field>
          </div>
          {!sliding ? (
            <div className="grid g2">
              <Range label="Refill rate" value={cfg.rate} min={0.5} max={50} step={0.5} onChange={(rate) => setCfg({ ...cfg, rate })} format={(v) => `${v}/s`} />
              <Range label="Burst (bucket size)" value={cfg.burst} min={1} max={100} onChange={(burst) => setCfg({ ...cfg, burst })} />
            </div>
          ) : (
            <div className="grid g2">
              <Range label="Limit per window" value={cfg.limit} min={1} max={100} onChange={(limit) => setCfg({ ...cfg, limit })} />
              <Range label="Window" value={cfg.window} min={0.5} max={30} step={0.5} onChange={(window) => setCfg({ ...cfg, window })} format={(v) => `${v}s`} />
            </div>
          )}
          <div className="row between">
            <span className="small muted">
              {sliding
                ? `At most ${cfg.limit} requests in any ${cfg.window}s window, sliding continuously.`
                : `Bursts of up to ${cfg.burst}, then ${cfg.rate} per second on average.`}
            </span>
            <Button kind="primary" icon={Settings2} onClick={save} busy={busy}>
              Save limits
            </Button>
          </div>
        </Card>

        <Card title="Fire requests" icon={Send} hint="Each request is GET /check/{key}. Green = 200 ALLOW, red = 429 DENY.">
          <div className="bucket-wrap">
            <Bucket state={client?.state} />
            <div className="col">
              <div className="row wrap">
                <Button kind="primary" icon={Send} onClick={() => fire(1)}>
                  Send 1
                </Button>
                <Button icon={Zap} onClick={() => fire(15)}>
                  Burst ×15 (parallel)
                </Button>
                <Button icon={streaming ? Square : Play} kind={streaming ? 'danger' : ''} onClick={() => setStreaming(!streaming)}>
                  {streaming ? 'Stop stream' : 'Stream'}
                </Button>
              </div>
              <Range label="Stream rate" value={streamRps} min={1} max={30} onChange={setStreamRps} format={(v) => `${v} req/s`} />
              <div className="ticks">
                {ticks.map((t, i) => (
                  <span key={i} className={`tick ${t}`} />
                ))}
                {!ticks.length && <span className="faint small">No requests yet. Try a burst bigger than the bucket.</span>}
              </div>
              <div className="row small muted">
                <Badge kind="ok">{allowed} allowed</Badge>
                <Badge kind="bad">{ticks.length - allowed} denied</Badge>
                <button className="btn ghost sm" onClick={() => setTicks([])}>
                  clear
                </button>
              </div>
            </div>
          </div>
        </Card>

        <Card title="Last response" icon={Activity} hint="Rate-limit headers are on every response, allowed or not (R6).">
          {last ? (
            <div className="row" style={{ alignItems: 'flex-start', gap: 22 }}>
              <div>
                <div className={`verdict ${last.status === 200 ? 'ok' : 'bad'}`}>{last.decision}</div>
                <div className="mono small faint" style={{ marginTop: 6 }}>
                  HTTP {last.status}
                </div>
              </div>
              <div className="headers grow">
                {last.headers.map(([h, v]) => (
                  <Fragment key={h}>
                    <span>{h}</span>
                    <span>{v}</span>
                  </Fragment>
                ))}
              </div>
            </div>
          ) : (
            <div className="empty">Send a request to see the decision and its headers.</div>
          )}
        </Card>
      </div>

      <div className="col" style={{ gap: 16 }}>
        <Card
          title={`Live decisions · ${key}`}
          icon={Gauge}
          hint="Per-second ALLOW/DENY counts recorded by the service (stretch goal: live dashboard)."
          actions={
            <div className="legend">
              <span><i style={{ background: 'var(--ok)' }} />allowed</span>
              <span><i style={{ background: 'var(--bad)' }} />denied</span>
            </div>
          }
        >
          <StackedBars rows={series} series={[{ key: 'a', color: 'var(--ok)' }, { key: 'd', color: 'var(--bad)' }]} height={130} maxHint={5} />
          <div className="row between tiny faint">
            <span>60s ago</span>
            <span>now</span>
          </div>
        </Card>

        <Card title="Load test · 500+ req/s" icon={Rocket} glow hint="Separate load-generator processes fire at one key; afterwards every decision is replayed through a reference model of the algorithm (R7).">
          <div className="grid g3">
            <Range label="Target rate" value={lt.rps} min={100} max={900} step={50} onChange={(rps) => setLt({ ...lt, rps })} format={(v) => `${v}/s`} />
            <Range label="Duration" value={lt.duration} min={2} max={8} onChange={(duration) => setLt({ ...lt, duration })} format={(v) => `${v}s`} />
            <Field label="Mode">
              <Segmented value={lt.mode} onChange={(mode) => setLt({ ...lt, mode })} options={[['token_bucket', 'Bucket'], ['sliding_window', 'Window']]} />
            </Field>
          </div>
          <div className="row between">
            <span className="small muted">
              {lt.mode === 'token_bucket' ? 'Client config: rate 50/s, burst 100' : 'Client config: 80 per 1s window'} · {lt.rps * lt.duration} requests
            </span>
            <Button kind="primary" icon={Rocket} onClick={runLoad} busy={ltBusy}>
              {ltBusy ? 'Running…' : 'Run load test'}
            </Button>
          </div>
          {ltResult && !ltResult.error && (
            <>
              <div className="grid g4">
                <Stat label="Achieved" value={`${Math.round(ltResult.achieved_rps)}/s`} sub="server clock" color={ltResult.achieved_rps >= 500 ? 'var(--ok)' : 'var(--warn)'} />
                <Stat label="Allowed" value={ltResult.allowed} sub={`of ${ltResult.requests}`} />
                <Stat label="Model says" value={ltResult.model_allowed} sub="reference replay" />
                <Stat label="Mismatches" value={ltResult.mismatches} sub="decisions differing" color={ltResult.mismatches <= 1 ? 'var(--ok)' : 'var(--bad)'} />
              </div>
              <Callout kind={ltResult.ok && !ltResult.errors ? 'ok' : 'bad'}>
                <b>{ltResult.ok && !ltResult.errors ? 'PASS' : 'FAIL'}:</b> {ltResult.rule}{' '}
                {ltResult.max_in_any_window != null && <>· worst window held {ltResult.max_in_any_window}. </>}
                {ltResult.errors ? `${ltResult.errors} transport errors.` : 'No transport errors.'}
              </Callout>
              {ltResult.achieved_rps < 500 && (
                <Callout>
                  The decisions are still exactly right, but the machine couldn't deliver {lt.rps} req/s this time. The other demos
                  (especially project 05's five-process cache cluster) share the same loopback network. For a clean measurement run
                  only this service: <code>python dev.py 1</code>.
                </Callout>
              )}
            </>
          )}
          {ltResult?.error && <div className="code">{ltResult.error}</div>}
        </Card>

        <Card title="All clients" icon={Users} hint="Every configured client and its lifetime totals.">
          <table className="table">
            <thead>
              <tr>
                <th>Client</th>
                <th>Algorithm</th>
                <th className="right">Allowed</th>
                <th className="right">Denied</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries({ ...Object.fromEntries(Object.keys(totals).map((k) => [k, null])), ...(configs || {}) }).map(([k, c]) => (
                <tr key={k} className="clickable" onClick={() => setKey(k)}>
                  <td className="mono">{k}</td>
                  <td className="small muted">
                    {c ? (c.mode === 'sliding_window' ? `${c.limit} / ${c.window}s window` : `${c.rate}/s, burst ${c.burst}`) : 'default'}
                  </td>
                  <td className="right num" style={{ color: 'var(--ok)' }}>{totals[k]?.a ?? 0}</td>
                  <td className="right num" style={{ color: 'var(--bad)' }}>{totals[k]?.d ?? 0}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      </div>
    </div>
  )
}
