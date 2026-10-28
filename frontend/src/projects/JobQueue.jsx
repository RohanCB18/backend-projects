import {
  Bomb, Cpu, CreditCard, History, Inbox, ListPlus, Plus, RotateCcw, Skull, Sparkles, Square, Trash2, X,
} from 'lucide-react'
import { useMemo, useState } from 'react'
import { Badge, Button, Callout, Card, Field, Stat } from '../components/ui'
import { api, fmtTime, useAction, usePoll } from '../lib/api'

const A = api(2)

const TYPE_COLORS = {
  send_email: '#60a5fa',
  resize_image: '#a78bfa',
  flaky: '#fbbf24',
  always_fail: '#fb7185',
  slow: '#94a3b8',
  charge_order: '#34d399',
  charge_order_naive: '#f97316',
}

const SCENARIOS = [
  ['mixed', 'Mixed workload', '12 emails, image resizes and flaky jobs'],
  ['priority', 'Priority', '8 bulk jobs, then 3 VIP jobs that jump the line'],
  ['scheduled', 'Scheduled', 'Reminders due in 3, 6, 9 and 12 seconds'],
  ['dead_letter', 'Retries → DLQ', 'Backoff 1s, 2s, 4s… then the dead-letter queue'],
  ['crash', 'Worker crash', 'Worker dies mid-job; lease expires; someone else finishes'],
  ['idempotency', 'Idempotency', 'Both jobs crash right after charging: once vs twice'],
  ['zombie', 'Zombie worker', 'Stalls past its lease; a second worker takes over'],
  ['dependencies', 'Dependencies', 'resize → email uploader → email admin, in order'],
]

const EVENT_KIND = {
  enqueued: 'neutral', claimed: 'info', done: 'ok', retry_scheduled: 'warn', dead: 'bad', lease_expired: 'warn',
  replayed: 'accent', log: 'neutral', stale_complete_rejected: 'bad', lease_lost: 'bad', stale_fail_rejected: 'bad',
}

function JobCard({ job, lease, onOpen }) {
  const now = Date.now() / 1000
  return (
    <div className="job" style={{ '--jc': TYPE_COLORS[job.type] }} onClick={() => onOpen(job.id)}>
      <div className="row between">
        <span className="type">{job.type}</span>
        <span className="id">#{job.id}</span>
      </div>
      <div className="row between tiny muted">
        <span>
          {job.priority ? <Badge kind="accent">P{job.priority}</Badge> : <span className="faint">P0</span>}
          {job.depends_on && <span className="faint"> · after #{job.depends_on}</span>}
        </span>
        <span>
          try {job.attempts}/{job.max_attempts}
        </span>
      </div>
      {job.status === 'queued' && job.run_at > now + 0.05 && (
        <div className="tiny" style={{ color: 'var(--warn)' }}>
          {job.attempts ? 'retry' : 'runs'} in {(job.run_at - now).toFixed(1)}s
        </div>
      )}
      {job.status === 'running' && lease && (
        <>
          <div className="row between tiny muted">
            <span className="mono">{job.lease_owner}</span>
            <span className="mono" style={{ color: lease.lease_remaining < 0 ? 'var(--bad)' : undefined }}>
              lease {Math.max(0, lease.lease_remaining).toFixed(1)}s
            </span>
          </div>
          <div className="lease">
            <i style={{ width: `${Math.max(0, Math.min(1, lease.lease_remaining / lease.total)) * 100}%` }} />
          </div>
        </>
      )}
      {job.status === 'dead' && job.last_error && <div className="tiny" style={{ color: 'var(--bad)' }}>{job.last_error}</div>}
    </div>
  )
}

function JobDrawer({ id, onClose }) {
  const [job] = usePoll(() => A.get(`/jobs/${id}`), 800, [id])
  return (
    <>
      <div className="drawer-bg" onClick={onClose} />
      <div className="drawer">
        <div className="row between">
          <div>
            <div className="eyebrow">Job #{id}</div>
            <h3 style={{ margin: '4px 0 0', fontSize: 22 }}>{job?.type}</h3>
          </div>
          <button className="btn ghost" onClick={onClose}>
            <X size={16} />
          </button>
        </div>
        {job && (
          <div className="col" style={{ marginTop: 16, gap: 14 }}>
            <div className="row wrap">
              <Badge kind={job.status === 'done' ? 'ok' : job.status === 'dead' ? 'bad' : job.status === 'running' ? 'info' : 'neutral'}>
                {job.status}
              </Badge>
              <Badge>priority {job.priority}</Badge>
              <Badge>
                attempt {job.attempts}/{job.max_attempts}
              </Badge>
            </div>
            <div className="code">{JSON.stringify(job.payload, null, 2)}</div>
            {job.last_error && <Callout kind="bad">{job.last_error}</Callout>}
            <div className="card-title">
              <History size={15} /> Event timeline
            </div>
            <div className="timeline">
              {job.events.map((e) => (
                <div className="tl-item" key={e.id}>
                  <span className="tl-dot" style={{ background: `var(--${EVENT_KIND[e.kind] === 'neutral' ? 'surface-3' : EVENT_KIND[e.kind] || 'surface-3'})` }} />
                  <div>
                    <div className="row" style={{ gap: 8 }}>
                      <b className="small">{e.kind.replaceAll('_', ' ')}</b>
                      {e.worker && <span className="mono tiny faint">{e.worker}</span>}
                      <span className="mono tiny faint">{fmtTime(e.ts)}</span>
                    </div>
                    {e.detail && <div className="small muted">{e.detail}</div>}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </>
  )
}

export default function JobQueue() {
  const [d] = usePoll(() => A.get('/dashboard'), 600)
  const [run, busy] = useAction()
  const [open, setOpen] = useState(null)
  const [form, setForm] = useState({ type: 'send_email', priority: 0, delay: 0, max_attempts: 4, misbehave: 'none' })

  const s = d?.stats
  const leases = useMemo(() => {
    const m = {}
    for (const j of s?.in_flight || []) m[j.id] = { lease_remaining: j.lease_remaining, total: s.lease_seconds }
    return m
  }, [s])

  const lanes = useMemo(() => {
    const now = Date.now() / 1000
    const out = { scheduled: [], ready: [], running: [], done: [], dead: [] }
    for (const j of d?.jobs || []) {
      if (j.status === 'queued') (j.run_at > now + 0.05 ? out.scheduled : out.ready).push(j)
      else out[j.status]?.push(j)
    }
    out.ready.sort((a, b) => b.priority - a.priority || a.id - b.id)
    out.scheduled.sort((a, b) => a.run_at - b.run_at)
    return out
  }, [d])

  const enqueue = () => {
    const payload = {}
    if (form.misbehave === 'crash') payload.crash_on_attempt = 1
    if (form.misbehave === 'flaky') payload.fail_rate = 0.7
    if (form.misbehave === 'stall') payload.stall_seconds = 8
    return run(
      () => A.post('/jobs', { type: form.type, priority: form.priority, delay: form.delay, max_attempts: form.max_attempts, payload }),
      (j) => `Enqueued job #${j.id}`,
    )
  }

  const charges = d?.side_effects?.charges || []

  return (
    <div className="col" style={{ gap: 16 }}>
      <div className="grid g6">
        <Stat label="Ready" value={s?.ready} sub="claimable now" color="var(--info)" />
        <Stat label="Scheduled" value={s?.scheduled} sub="delayed / backing off" color="var(--warn)" />
        <Stat label="Waiting" value={s?.waiting_on_dependency} sub="on a parent job" />
        <Stat label="In flight" value={s?.running} sub={`lease ${s?.lease_seconds ?? 5}s`} color="var(--accent)" />
        <Stat label="Done" value={s?.done} color="var(--ok)" />
        <Stat label="Dead letters" value={s?.dead} sub="exhausted retries" color="var(--bad)" />
      </div>

      <div className="grid" style={{ gridTemplateColumns: 'minmax(0, 1.6fr) minmax(0, 1fr)' }}>
        <Card title="Scenarios" icon={Sparkles} hint="One click enqueues a batch built to show one guarantee. Click any job card for its full timeline.">
          <div className="scen">
            {SCENARIOS.map(([k, t, desc]) => (
              <button key={k} onClick={() => run(() => A.post(`/scenarios/${k}`), (r) => `${t}: enqueued ${r.enqueued.length} job(s)`)}>
                <b>{t}</b>
                <span>{desc}</span>
              </button>
            ))}
          </div>
        </Card>
        <Card title="Enqueue a job" icon={ListPlus}>
          <div className="grid g2">
            <Field label="Type">
              <select className="select" value={form.type} onChange={(e) => setForm({ ...form, type: e.target.value })}>
                {Object.keys(TYPE_COLORS).map((t) => (
                  <option key={t}>{t}</option>
                ))}
              </select>
            </Field>
            <Field label="Misbehave">
              <select className="select" value={form.misbehave} onChange={(e) => setForm({ ...form, misbehave: e.target.value })}>
                <option value="none">behave normally</option>
                <option value="flaky">fail 70% of the time</option>
                <option value="crash">crash worker on attempt 1</option>
                <option value="stall">stall 8s without heartbeats</option>
              </select>
            </Field>
            <Field label="Priority">
              <input className="input" type="number" value={form.priority} onChange={(e) => setForm({ ...form, priority: Number(e.target.value) })} />
            </Field>
            <Field label="Delay (s)">
              <input className="input" type="number" min="0" value={form.delay} onChange={(e) => setForm({ ...form, delay: Number(e.target.value) })} />
            </Field>
          </div>
          <div className="row between">
            <span className="small faint">max attempts {form.max_attempts}</span>
            <Button kind="primary" icon={Plus} onClick={enqueue} busy={busy}>
              Enqueue
            </Button>
          </div>
        </Card>
      </div>

      <Card
        title="Workers"
        icon={Cpu}
        hint="Thread workers live inside the API process; process workers are real OS processes (python -m app.worker). Crash/kill one while it holds a job and watch its lease run out."
        actions={
          <>
            <Button size="sm" icon={Plus} onClick={() => run(() => A.post('/workers?n=1'), 'Thread worker added')}>
              Thread worker
            </Button>
            <Button size="sm" icon={Plus} onClick={() => run(() => A.post('/workers/process'), (r) => `Started ${r.spawned} (pid ${r.pid})`)}>
              Process worker
            </Button>
            <Button size="sm" kind="danger" icon={Bomb} onClick={() => run(() => A.post('/workers/crash'), (r) => (r.crashed ? `${r.crashed} crashed` : 'no worker to crash'))}>
              Crash a busy worker
            </Button>
            <Button size="sm" kind="ghost" icon={Trash2} onClick={() => run(() => A.post('/workers/cleanup'))}>
              Clear dead
            </Button>
          </>
        }
      >
        <div className="workers">
          {(d?.workers || []).map((w) => (
            <div key={w.id} className={`worker ${w.status}`}>
              <div className="row between">
                <b className="mono">{w.id}</b>
                <Badge kind={w.status === 'busy' ? 'info' : w.status === 'idle' ? 'ok' : w.status === 'stopped' ? 'neutral' : 'bad'}>
                  {w.status}
                </Badge>
              </div>
              <div className="tiny muted">
                {w.kind} · {w.processed} done{w.current_job ? ` · on #${w.current_job}` : ''}
              </div>
              <div className="row" style={{ gap: 6 }}>
                {w.kind === 'thread' && !['crashed', 'stopped'].includes(w.status) && (
                  <>
                    <Button size="sm" kind="danger" icon={Bomb} onClick={() => run(() => A.post(`/workers/crash?worker_id=${w.id}`), `${w.id} crashed`)}>
                      crash
                    </Button>
                    <Button size="sm" kind="ghost" icon={Square} onClick={() => run(() => A.post(`/workers/${w.id}/stop`), `${w.id} stopping`)}>
                      stop
                    </Button>
                  </>
                )}
                {w.kind === 'process' && !['killed', 'crashed', 'stopped'].includes(w.status) && (
                  <Button size="sm" kind="danger" icon={Skull} onClick={() => run(() => A.post(`/workers/${w.id}/kill`), (r) => `kill -9 → pid ${r.pid}`)}>
                    kill -9
                  </Button>
                )}
              </div>
            </div>
          ))}
          {!d?.workers?.length && <div className="empty">No workers registered.</div>}
        </div>
      </Card>

      <div className="board">
        {[
          ['scheduled', 'Scheduled', 'var(--warn)'],
          ['ready', 'Ready', 'var(--info)'],
          ['running', 'Running', 'var(--accent)'],
          ['done', 'Done', 'var(--ok)'],
          ['dead', 'Dead letter', 'var(--bad)'],
        ].map(([k, label, color]) => (
          <div className="lane" key={k}>
            <div className="lane-head">
              <span className="row" style={{ gap: 7 }}>
                <span className="dot" style={{ background: color }} />
                {label}
              </span>
              <span className="mono">{lanes[k].length}</span>
            </div>
            <div className="lane-body">
              {lanes[k].map((j) => (
                <JobCard key={j.id} job={j} lease={leases[j.id]} onOpen={setOpen} />
              ))}
              {!lanes[k].length && <div className="empty tiny">empty</div>}
            </div>
          </div>
        ))}
      </div>

      <div className="grid g3">
        <Card
          title="Dead-letter queue"
          icon={Inbox}
          hint="Jobs that ran out of attempts, with their last error (R4)."
          actions={
            <Button size="sm" icon={RotateCcw} disabled={!d?.dlq?.length} onClick={() => run(() => A.post('/dlq/replay-all'), (r) => `Replayed ${r.replayed}`)}>
              Replay all
            </Button>
          }
        >
          <div className="col" style={{ gap: 6, maxHeight: 260, overflow: 'auto' }}>
            {(d?.dlq || []).map((j) => (
              <div key={j.id} className="row between" style={{ padding: '6px 8px', borderRadius: 8, background: 'var(--surface-2)' }}>
                <div className="grow" style={{ cursor: 'pointer' }} onClick={() => setOpen(j.id)}>
                  <div className="small">
                    <b>{j.type}</b> <span className="mono faint">#{j.id}</span>
                  </div>
                  <div className="tiny" style={{ color: 'var(--bad)' }}>{j.last_error}</div>
                </div>
                <Button size="sm" icon={RotateCcw} onClick={() => run(() => A.post(`/dlq/${j.id}/replay`), `Replayed #${j.id}`)}>
                  replay
                </Button>
              </div>
            ))}
            {!d?.dlq?.length && <div className="empty">Nothing dead. Try the "Retries → DLQ" scenario.</div>}
          </div>
        </Card>

        <Card title="Side effects: payments" icon={CreditCard} hint="Each charge row is real money moved. A retried job must not charge twice (R7).">
          <table className="table">
            <thead>
              <tr>
                <th>Order</th>
                <th>Handler</th>
                <th className="right">Charged</th>
              </tr>
            </thead>
            <tbody>
              {charges.map((c) => (
                <tr key={c.order_id}>
                  <td className="mono">{c.order_id}</td>
                  <td>{c.idempotent ? <Badge kind="ok">idempotent</Badge> : <Badge kind="warn">naive</Badge>}</td>
                  <td className="right">
                    <span className="num" style={{ color: c.times_charged > 1 ? 'var(--bad)' : 'var(--ok)', fontWeight: 700 }}>
                      {c.times_charged}×
                    </span>{' '}
                    <span className="faint small">₹{c.total}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {!charges.length && <div className="empty">Run the Idempotency or Zombie scenario.</div>}
          {charges.some((c) => c.times_charged > 1) && (
            <Callout kind="bad">The naive handler re-ran after a crash and charged the customer again. At-least-once delivery plus a non-idempotent job means double charges.</Callout>
          )}
        </Card>

        <Card title="Event stream" icon={History} hint="Everything that happens to every job, newest first.">
          <div className="feed" style={{ maxHeight: 300 }}>
            {(d?.events || []).map((e) => (
              <div className="feed-row" key={e.id} onClick={() => setOpen(e.job_id)} style={{ cursor: 'pointer' }}>
                <span className="t">{fmtTime(e.ts)}</span>
                <span>
                  <Badge kind={EVENT_KIND[e.kind] || 'neutral'}>{e.kind.replaceAll('_', ' ')}</Badge>
                </span>
                <span className="small muted" style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  <span className="mono">#{e.job_id}</span> {e.worker ? `${e.worker} · ` : ''}
                  {e.detail}
                </span>
              </div>
            ))}
          </div>
        </Card>
      </div>
      <div className="row" style={{ justifyContent: 'flex-end' }}>
        <Button kind="ghost" size="sm" icon={Trash2} onClick={() => run(() => A.post('/reset'), 'Queue wiped')}>
          Wipe all jobs
        </Button>
      </div>
      {open && <JobDrawer id={open} onClose={() => setOpen(null)} />}
    </div>
  )
}
