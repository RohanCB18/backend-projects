import {
  Activity, Bomb, CircleDot, FlaskConical, KeyRound, Play, Plus, RotateCw, Scale, ScrollText, Server, Skull, Square,
} from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import { Badge, Button, Callout, Card, Field, Range, Segmented, StackedBars, Stat } from '../components/ui'
import { api, fmtTime, toast, useAction, usePoll } from '../lib/api'

const A = api(5)
const NODE_COLORS = ['#fb7185', '#f59e0b', '#a78bfa', '#38bdf8', '#34d399', '#f472b6', '#facc15', '#22d3ee']
const nodeColor = (id) => NODE_COLORS[(Number(String(id).replace(/\D/g, '')) - 1) % NODE_COLORS.length] || '#94a3b8'

function liveness(n) {
  if (n.process !== 'running') return 'dead'
  return n.status || 'starting'
}

function Ring({ ring, nodes, located }) {
  const size = 420
  const c = size / 2
  const r = 150
  const pts = ring?.points || []
  const arcs = []
  for (let i = 0; i < pts.length; i++) {
    const prev = i === 0 ? pts[pts.length - 1].pos - 1 : pts[i - 1].pos
    arcs.push({ from: prev, to: pts[i].pos, node: pts[i].node })
  }
  const xy = (pos, rad = r) => [c + rad * Math.sin(pos * 2 * Math.PI), c - rad * Math.cos(pos * 2 * Math.PI)]
  const arcPath = (a, b, rad) => {
    const [x1, y1] = xy(a, rad)
    const [x2, y2] = xy(b, rad)
    const large = b - a > 0.5 ? 1 : 0
    return `M ${x1} ${y1} A ${rad} ${rad} 0 ${large} 1 ${x2} ${y2}`
  }
  const dead = nodes.filter((n) => liveness(n) === 'dead')
  return (
    <div className="ring-wrap">
      <svg viewBox={`0 0 ${size} ${size}`}>
        <circle cx={c} cy={c} r={r} fill="none" stroke="var(--surface-3)" strokeWidth="22" />
        {arcs.map((a, i) => (
          <path key={i} d={arcPath(a.from, a.to, r)} fill="none" stroke={nodeColor(a.node)} strokeWidth="22" strokeOpacity="0.85" />
        ))}
        {pts.map((p, i) => {
          const [x1, y1] = xy(p.pos, r - 13)
          const [x2, y2] = xy(p.pos, r + 13)
          return <line key={i} x1={x1} y1={y1} x2={x2} y2={y2} stroke="var(--surface)" strokeWidth="1" />
        })}
        {located && (
          <g>
            {located.preference.map((nid, i) => {
              const own = pts.find((p) => p.node === nid && p.pos >= located.pos) || pts.find((p) => p.node === nid)
              if (!own) return null
              const [x, y] = xy(own.pos, r - 30)
              return (
                <g key={nid}>
                  <line x1={c} y1={c} x2={x} y2={y} stroke={nodeColor(nid)} strokeWidth={i === 0 ? 2.5 : 1.5} strokeDasharray={i ? '4 4' : undefined} />
                  <circle cx={x} cy={y} r="4" fill={nodeColor(nid)} />
                </g>
              )
            })}
            {(() => {
              const [x, y] = xy(located.pos, r + 26)
              const [x2, y2] = xy(located.pos, r - 11)
              return (
                <g>
                  <line x1={x} y1={y} x2={x2} y2={y2} stroke="var(--text)" strokeWidth="2.5" />
                  <circle cx={x} cy={y} r="6" fill="var(--text)" />
                </g>
              )
            })()}
          </g>
        )}
        <text x={c} y={c - 8} textAnchor="middle" fontSize="30" fontWeight="800" fill="var(--text)" fontFamily="var(--font)">
          {ring?.nodes?.length ?? 0}
        </text>
        <text x={c} y={c + 14} textAnchor="middle" fontSize="12" fill="var(--text-2)" fontFamily="var(--font)">
          nodes in ring
        </text>
        <text x={c} y={c + 32} textAnchor="middle" fontSize="11" fill="var(--text-3)" fontFamily="var(--mono)">
          {pts.length} virtual nodes
        </text>
        {!!dead.length && (
          <text x={c} y={c + 52} textAnchor="middle" fontSize="11" fill="var(--bad)" fontFamily="var(--mono)">
            down: {dead.map((n) => n.id).join(', ')}
          </text>
        )}
      </svg>
      <div className="row wrap" style={{ justifyContent: 'center', gap: 10, marginTop: 6 }}>
        {nodes.map((n) => (
          <span key={n.id} className="row tiny" style={{ gap: 5, opacity: liveness(n) === 'dead' ? 0.45 : 1 }}>
            <span className="dot" style={{ background: nodeColor(n.id) }} />
            <span className="mono">{n.id}</span>
          </span>
        ))}
      </div>
    </div>
  )
}

function NodeCard({ n, onKill, onRestart }) {
  const st = liveness(n)
  const kind = { alive: 'ok', joining: 'info', starting: 'info', dead: 'bad' }[st] || 'neutral'
  const peerView = (n.members || []).filter((m) => m.status !== 'alive')
  return (
    <div className={`node ${st === 'dead' ? 'dead' : st === 'joining' ? 'joining' : ''}`} style={{ '--nc': nodeColor(n.id) }}>
      <div className="row between">
        <span className="nid">{n.id}</span>
        <Badge kind={kind} dot>
          {st === 'dead' ? 'killed' : st}
        </Badge>
      </div>
      <div className="kv">
        <span>port</span>
        <b>{n.port}</b>
        <span>keys stored</span>
        <b>{n.keys ?? '–'}</b>
        <span>as primary</span>
        <b>{n.primary_keys ?? '–'}</b>
        <span>ring epoch</span>
        <b>{n.epoch ?? '–'}</b>
        <span>gets / puts</span>
        <b>
          {n.stats?.gets ?? 0}/{n.stats?.puts ?? 0}
        </b>
      </div>
      {!!peerView.length && st !== 'dead' && (
        <div className="tiny" style={{ color: 'var(--warn)' }}>
          sees {peerView.map((m) => `${m.id} ${m.status}`).join(', ')}
        </div>
      )}
      {st === 'dead' ? (
        <Button size="sm" icon={RotateCw} onClick={() => onRestart(n.id)}>
          Restart
        </Button>
      ) : (
        <Button size="sm" kind="danger" icon={Skull} onClick={() => onKill(n.id)}>
          Kill process
        </Button>
      )}
    </div>
  )
}

function Compare() {
  const [b, setB] = useState(4)
  const [a, setA] = useState(5)
  const [res] = usePoll(() => A.get(`/ring/compare?before=${b}&after=${a}`), 60000, [a, b])
  return (
    <Card title="Why consistent hashing" icon={Scale} hint="Share of keys whose primary node changes when the cluster resizes (R2).">
      <div className="grid g2">
        <Range label="Before" value={b} min={2} max={10} onChange={setB} format={(v) => `${v} nodes`} />
        <Range label="After" value={a} min={2} max={10} onChange={setA} format={(v) => `${v} nodes`} />
      </div>
      {res && (
        <div className="col" style={{ gap: 8 }}>
          {[
            ['hash(key) % N', res.modulo, 'var(--bad)'],
            ['Consistent hashing', res.consistent, 'var(--ok)'],
            ['Theoretical minimum', res.ideal, 'var(--text-3)'],
          ].map(([label, v, color]) => (
            <div key={label}>
              <div className="small muted" style={{ marginBottom: 3 }}>
                {label}
              </div>
              <div className="compare-bar">
                <i style={{ width: `${v * 100}%`, background: color }} />
                <span>{(v * 100).toFixed(1)}%</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </Card>
  )
}

function useTryIt() {
  const [key, setKey] = useState('user:42')
  const [value, setValue] = useState('Rohan, 3rd year CSE')
  const [ttl, setTtl] = useState('')
  const [cons, setCons] = useState('eventual')
  const [res, setRes] = useState(null)
  const [loc, setLoc] = useState(null)
  const [run, busy] = useAction()
  useEffect(() => {
    const t = setTimeout(() => A.get(`/ring/locate/${encodeURIComponent(key)}`).then(setLoc).catch(() => {}), 200)
    return () => clearTimeout(t)
  }, [key])
  return {
    loc,
    card: (
      <Card title="Read & write a key" icon={KeyRound} hint="The marker on the ring shows where this key hashes to; the lines go to its 3 owners.">
        <div className="grid g2">
          <Field label="Key">
            <input className="input mono" value={key} onChange={(e) => setKey(e.target.value)} />
          </Field>
          <Field label="Value">
            <input className="input" value={value} onChange={(e) => setValue(e.target.value)} />
          </Field>
          <Field label="TTL seconds (optional)">
            <input className="input" type="number" value={ttl} placeholder="no expiry" onChange={(e) => setTtl(e.target.value)} />
          </Field>
          <Field label="Consistency">
            <Segmented value={cons} onChange={setCons} options={[['eventual', 'Eventual (W1/R1)'], ['quorum', 'Quorum (W2/R2)']]} />
          </Field>
        </div>
        <div className="row">
          <Button
            kind="primary"
            busy={busy}
            onClick={() => run(async () => setRes(await A.put(`/kv/${encodeURIComponent(key)}`, { value, ttl: ttl ? Number(ttl) : null, consistency: cons })))}
          >
            PUT
          </Button>
          <Button busy={busy} onClick={() => run(async () => setRes(await A.get(`/kv/${encodeURIComponent(key)}?consistency=${cons}`)))}>
            GET
          </Button>
          {loc && (
            <span className="small muted">
              owners: {loc.preference.map((n, i) => (
                <span key={n} className="mono" style={{ color: nodeColor(n), fontWeight: i === 0 ? 700 : 500 }}>
                  {n}
                  {i === 0 ? ' (primary)' : ''}
                  {i < loc.preference.length - 1 ? ', ' : ''}
                </span>
              ))}
            </span>
          )}
        </div>
        {res && (
          <div className="code">
            {res.found === false && 'MISS (not found or expired)\n'}
            {res.value != null && `value: ${res.value}\n`}
            {`coordinator: ${res.coordinator}  (client entered via ${res.entry_nodes?.join(' → ')}, ${res.attempts} attempt${res.attempts > 1 ? 's' : ''})\n`}
            {res.acked && `written to: ${res.acked.join(', ')}  (owners ${res.owners.join(', ')})\n`}
            {res.trace && `read path: ${res.trace.map((t) => `${t.node}:${t.result}`).join(' → ')}\n`}
            {res.expires_at && `expires at: ${new Date(res.expires_at).toLocaleTimeString()} (absolute, same on every replica)\n`}
            {res.version && `version: [${res.version.join(', ')}]`}
          </div>
        )}
      </Card>
    ),
  }
}

export default function Cache() {
  const [d, , refresh] = usePoll(() => A.get('/cluster'), 700)
  const [run, busy] = useAction()
  const [tr, setTr] = useState({ rps: 150, consistency: 'eventual' })
  const [chaosCfg, setChaosCfg] = useState({ consistency: 'eventual' })
  const tryIt = useTryIt()

  const nodes = d?.nodes || []
  const t = d?.traffic
  const chaos = d?.chaos || { state: 'idle' }
  const alive = nodes.filter((n) => liveness(n) === 'alive').length

  const phase = useMemo(() => {
    if (chaos.state !== 'running' && chaos.state !== 'done') return -1
    if (chaos.state === 'done') return 4
    if (!chaos.victim) return 0
    const since = Date.now() / 1000 - chaos.killed_at
    return since < (chaos.config.duration - chaos.config.kill_at) ? 1 : 2
  }, [chaos, d])

  const kill = (id) => run(() => A.post(`/nodes/${id}/kill`).then(refresh), `${id} killed (no warning, no handoff)`)
  const restart = (id) => run(() => A.post(`/nodes/${id}/restart`).then(refresh), `${id} restarting: it will stream its key range, then join`)

  return (
    <div className="col" style={{ gap: 16 }}>
      <div className="grid g6">
        <Stat label="Nodes alive" value={`${alive}/${nodes.length}`} color={alive === nodes.length ? 'var(--ok)' : 'var(--warn)'} sub="separate OS processes" />
        <Stat label="Copies / key" value="3" sub="primary + 2 replicas" />
        <Stat label="Requests" value={t?.requests ?? 0} sub={t?.running ? `${t.config.rps}/s target · ${t.config.consistency}` : 'traffic stopped'} />
        <Stat label="Failed" value={t?.failed ?? 0} sub="after 1 retry" color={(t?.failed ?? 0) === 0 ? 'var(--ok)' : 'var(--bad)'} />
        <Stat label="Saved by retry" value={t?.ok_after_retry ?? 0} sub="first node was down" color="var(--warn)" />
        <Stat label="Latency" value={t?.p50_ms != null ? `${t.p50_ms}ms` : '–'} sub={t?.p99_ms != null ? `p99 ${t.p99_ms}ms` : 'p50 / p99'} />
      </div>

      <div className="grid" style={{ gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1.25fr)' }}>
        <Card title="Hash ring" icon={CircleDot} hint="Arcs show which node owns each slice of the hash space (64 virtual nodes per server). Kill a node and its arcs go to its neighbours.">
          <Ring ring={d?.ring} nodes={nodes} located={tryIt.loc} />
        </Card>
        <div className="col" style={{ gap: 16 }}>
          <Card
            title="Nodes"
            icon={Server}
            hint="Kill sends TerminateProcess/SIGKILL: no goodbye, no handoff. The others notice through missing gossip heartbeats."
            actions={
              <>
                <Button size="sm" icon={Plus} onClick={() => run(() => A.post('/nodes').then(refresh), (r) => `${r.started} joining`)} disabled={nodes.length >= 8}>
                  Add node
                </Button>
                <Button size="sm" kind="danger" icon={Bomb} onClick={() => run(() => A.post('/nodes/kill-random').then(refresh), (r) => `Killed ${r.killed}`)}>
                  Kill random
                </Button>
              </>
            }
          >
            <div className="nodes">
              {nodes.map((n) => (
                <NodeCard key={n.id} n={n} onKill={kill} onRestart={restart} />
              ))}
            </div>
          </Card>
          {tryIt.card}
        </div>
      </div>

      <div className="grid" style={{ gridTemplateColumns: 'minmax(0, 1.3fr) minmax(0, 1fr)' }}>
        <Card
          title="Client traffic"
          icon={Activity}
          hint="A cluster-aware client sends each request to a random live node and retries once on another node if that fails. Every read is checked against the latest acknowledged write."
          actions={
            <div className="legend">
              <span><i style={{ background: 'var(--ok)' }} />ok</span>
              <span><i style={{ background: 'var(--warn)' }} />ok after retry</span>
              <span><i style={{ background: 'var(--bad)' }} />failed</span>
            </div>
          }
        >
          <StackedBars
            rows={t?.series || []}
            series={[{ key: 'ok', color: 'var(--ok)' }, { key: 'retried', color: 'var(--warn)' }, { key: 'failed', color: 'var(--bad)' }]}
            height={130}
            maxHint={20}
          />
          <div className="grid g3" style={{ alignItems: 'end' }}>
            <Range label="Rate" value={tr.rps} min={20} max={300} step={10} onChange={(rps) => setTr({ ...tr, rps })} format={(v) => `${v} req/s`} />
            <Field label="Consistency">
              <Segmented value={tr.consistency} onChange={(consistency) => setTr({ ...tr, consistency })} options={[['eventual', 'Eventual'], ['quorum', 'Quorum']]} />
            </Field>
            <div className="row" style={{ justifyContent: 'flex-end' }}>
              {t?.running ? (
                <Button kind="danger" icon={Square} onClick={() => run(() => A.post('/traffic/stop'))}>
                  Stop traffic
                </Button>
              ) : (
                <Button kind="primary" icon={Play} onClick={() => run(() => A.post('/traffic/start', { ...tr, keys: 300, write_ratio: 0.3 }), 'Traffic started')}>
                  Start traffic
                </Button>
              )}
            </div>
          </div>
          <div className="row wrap small muted" style={{ gap: 14 }}>
            <span>reads {t?.reads ?? 0}</span>
            <span>writes {t?.writes ?? 0}</span>
            <span>keys {t?.keys_written ?? 0}</span>
            <span style={{ color: t?.stale_reads ? 'var(--warn)' : undefined }}>stale reads {t?.stale_reads ?? 0}</span>
          </div>
        </Card>

        <Card title="Chaos test" icon={FlaskConical} glow hint="Requirement 7: live traffic, kill a random node mid-flight, then read back every acknowledged write with quorum reads.">
          <div className="row between">
            <Segmented value={chaosCfg.consistency} onChange={(consistency) => setChaosCfg({ consistency })} options={[['eventual', 'Eventual'], ['quorum', 'Quorum']]} />
            <Button
              kind="primary"
              icon={FlaskConical}
              disabled={chaos.state === 'running' || alive < 3}
              onClick={() =>
                run(() => A.post('/chaos', { duration: 14, kill_at: 4, rps: 150, keys: 300, consistency: chaosCfg.consistency }).then(() => toast('Chaos test running: traffic, then a random kill at t=4s', 'info')))
              }
            >
              {chaos.state === 'running' ? 'Running…' : 'Run chaos test'}
            </Button>
          </div>
          <div className="phase">
            {['Traffic', `Kill ${chaos.victim || 'random'}`, 'Heal & settle', 'Verify'].map((p, i) => (
              <div key={p} className={phase > i ? 'done' : phase === i ? 'on' : ''}>
                {p}
              </div>
            ))}
          </div>
          {chaos.state === 'done' && (
            <>
              <div className="grid g3">
                <Stat label="Requests" value={chaos.traffic.requests} sub={`${chaos.traffic.ok_after_retry} saved by retry`} />
                <Stat label="Failed" value={chaos.traffic.failed} color={chaos.traffic.failed ? 'var(--bad)' : 'var(--ok)'} sub="after 1 retry" />
                <Stat label="Lost writes" value={chaos.verify.lost} color={chaos.verify.lost ? 'var(--bad)' : 'var(--ok)'} sub={`of ${chaos.verify.checked} keys`} />
              </div>
              <Callout kind={chaos.passed ? 'ok' : 'bad'}>
                <b>{chaos.passed ? 'PASS' : 'FAIL'}:</b> killed <b>{chaos.victim}</b> mid-traffic. {chaos.traffic.failed} failed requests,{' '}
                {chaos.verify.lost} lost and {chaos.verify.stale} stale keys after healing. p50 {chaos.traffic.p50_ms}ms, p99 {chaos.traffic.p99_ms}ms.
              </Callout>
              <Button size="sm" icon={RotateCw} onClick={() => restart(chaos.victim)} disabled={nodes.find((n) => n.id === chaos.victim)?.process === 'running'}>
                Restart {chaos.victim}
              </Button>
            </>
          )}
          {chaos.state === 'error' && <Callout kind="bad">{chaos.error}</Callout>}
          {chaos.state === 'idle' && <div className="empty">Takes about 17 seconds. Needs at least 3 live nodes.</div>}
        </Card>
      </div>

      <div className="grid" style={{ gridTemplateColumns: 'minmax(0, 1.4fr) minmax(0, 1fr)' }}>
        <Card title="Cluster events" icon={ScrollText} hint="Gossip, failure detection, re-replication and handoffs as each node reports them.">
          <div className="feed" style={{ maxHeight: 320 }}>
            {[...(d?.events || [])].reverse().map((e, i) => (
              <div className="feed-row" key={i}>
                <span className="t">{fmtTime(e.ts)}</span>
                <span>
                  <Badge kind={{ failure: 'bad', ops: 'accent', join: 'info', handoff: 'info', rebalance: 'warn', 'anti-entropy': 'neutral', membership: 'warn', gossip: 'neutral', error: 'bad' }[e.kind] || 'neutral'}>
                    {e.kind}
                  </Badge>
                </span>
                <span className="small">
                  <b className="mono" style={{ color: e.node === 'controller' ? 'var(--text-2)' : nodeColor(e.node) }}>{e.node}</b> <span className="muted">{e.msg}</span>
                </span>
              </div>
            ))}
          </div>
        </Card>
        <Compare />
      </div>
    </div>
  )
}
