import {
  ArrowRightLeft, BadgeCheck, Clock3, Database, Landmark, ListChecks, ShieldAlert, ShieldCheck, Swords, UserPlus, XCircle,
} from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import { Badge, Button, Callout, Card, Field, Range, Segmented, StepLine } from '../components/ui'
import { api, fmtDate, fmtTime, useAction, usePoll } from '../lib/api'

const A = api(3)
const SYM = { INR: '₹', USD: '$', EUR: '€', GBP: '£' }
const money = (minor, cur = 'INR') =>
  `${minor < 0 ? '−' : ''}${SYM[cur] || ''}${(Math.abs(minor) / 100).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`

function MoveMoney({ accounts, onDone }) {
  const [run, busy] = useAction()
  const [kind, setKind] = useState('transfer')
  const [f, setF] = useState({ from: '', to: '', amount: 200, description: '' })
  const custs = accounts.filter((a) => a.kind !== 'system')
  useEffect(() => {
    if (custs.length && !f.from) setF((x) => ({ ...x, from: custs[0].id, to: custs[1]?.id || custs[0].id }))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [custs.length])
  const submit = () =>
    run(async () => {
      if (kind === 'transfer')
        await A.post('/transfer', { from_account: f.from, to_account: f.to, amount: f.amount, description: f.description })
      else await A.post(`/${kind}`, { account: f.from, amount: f.amount, description: f.description })
      onDone()
    }, kind === 'transfer' ? 'Transfer posted (2+ entries, one transaction)' : `${kind} posted`)
  return (
    <Card title="Move money" icon={ArrowRightLeft} hint="Every operation is one balanced transaction. Overdrafts are rejected before anything is written (R5).">
      <Segmented value={kind} onChange={setKind} options={[['transfer', 'Transfer'], ['deposit', 'Deposit'], ['withdraw', 'Withdraw']]} />
      <div className="grid g2">
        <Field label={kind === 'transfer' ? 'From' : 'Account'}>
          <select className="select" value={f.from} onChange={(e) => setF({ ...f, from: e.target.value })}>
            {custs.map((a) => (
              <option key={a.id} value={a.id}>
                {a.name} ({a.currency})
              </option>
            ))}
          </select>
        </Field>
        {kind === 'transfer' ? (
          <Field label="To">
            <select className="select" value={f.to} onChange={(e) => setF({ ...f, to: e.target.value })}>
              {custs.map((a) => (
                <option key={a.id} value={a.id}>
                  {a.name} ({a.currency})
                </option>
              ))}
            </select>
          </Field>
        ) : (
          <div />
        )}
        <Field label="Amount">
          <input className="input" type="number" min="0.01" step="0.01" value={f.amount} onChange={(e) => setF({ ...f, amount: Number(e.target.value) })} />
        </Field>
        <Field label="Description">
          <input className="input" value={f.description} placeholder="optional" onChange={(e) => setF({ ...f, description: e.target.value })} />
        </Field>
      </div>
      <div className="row between">
        <span className="small faint">Different currencies go through FX pool accounts, so each currency still nets to zero.</span>
        <Button kind="primary" icon={ArrowRightLeft} onClick={submit} busy={busy} disabled={!custs.length}>
          Post
        </Button>
      </div>
    </Card>
  )
}

function OpenAccount({ onDone }) {
  const [run, busy] = useAction()
  const [f, setF] = useState({ name: '', kind: 'customer', currency: 'INR', credit_limit: 10000 })
  return (
    <Card title="Open account" icon={UserPlus}>
      <div className="grid g2">
        <Field label="Name">
          <input className="input" value={f.name} placeholder="e.g. Ananya" onChange={(e) => setF({ ...f, name: e.target.value })} />
        </Field>
        <Field label="Currency">
          <select className="select" value={f.currency} onChange={(e) => setF({ ...f, currency: e.target.value })}>
            {Object.keys(SYM).map((c) => (
              <option key={c}>{c}</option>
            ))}
          </select>
        </Field>
        <Field label="Kind">
          <Segmented value={f.kind} onChange={(kind) => setF({ ...f, kind })} options={[['customer', 'Customer'], ['credit', 'Credit']]} />
        </Field>
        {f.kind === 'credit' && (
          <Field label="Credit limit">
            <input className="input" type="number" value={f.credit_limit} onChange={(e) => setF({ ...f, credit_limit: Number(e.target.value) })} />
          </Field>
        )}
      </div>
      <Button kind="primary" icon={UserPlus} busy={busy} disabled={!f.name.trim()} onClick={() => run(() => A.post('/accounts', f).then(onDone), `Opened ${f.name}`)}>
        Open account
      </Button>
    </Card>
  )
}

function TimeTravel({ account }) {
  const [timeline] = usePoll(() => A.get(`/accounts/${account.id}/timeline`), 3000, [account.id])
  const first = timeline?.[0]?.ts
  const nowTs = Date.now() / 1000
  const [frac, setFrac] = useState(1)
  const at = first ? first + (nowTs - first) * frac : nowTs
  const [audit, setAudit] = useState(null)
  useEffect(() => {
    setFrac(1)
  }, [account.id])
  useEffect(() => {
    const t = setTimeout(() => {
      A.get(`/accounts/${account.id}/audit${frac < 0.999 ? `?at=${at}` : ''}`).then(setAudit).catch(() => {})
    }, 120)
    return () => clearTimeout(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [account.id, frac, timeline?.length])
  const points = useMemo(() => (timeline || []).map((p) => ({ x: p.ts, y: p.balance / 100 })), [timeline])
  const steps = audit?.steps || []
  return (
    <Card
      title={`Time travel · ${account.name}`}
      icon={Clock3}
      glow
      hint="Balance at any moment = replay of the events up to that moment (R4). The audit trail explains every step (R6)."
    >
      <StepLine points={points} marker={frac < 0.999 ? at : null} height={150} fmt={(v) => `${SYM[account.currency]}${Math.round(v).toLocaleString('en-IN')}`} />
      <Range label={frac >= 0.999 ? 'As of: now' : `As of: ${fmtDate(at)}`} value={frac} min={0} max={1} step={0.001} onChange={setFrac} format={() => ''} />
      <div className="row between">
        <div>
          <div className="stat-label">Balance as of {frac >= 0.999 ? 'now' : fmtDate(at)}</div>
          <div className="stat-value" style={{ color: (audit?.replayed_balance ?? 0) < 0 ? 'var(--bad)' : undefined }}>
            {audit ? money(audit.replayed_balance, account.currency) : '–'}
          </div>
        </div>
        {audit && (
          <Badge kind={audit.consistent ? 'ok' : 'bad'}>
            {audit.consistent ? <BadgeCheck size={12} /> : <XCircle size={12} />}
            replay {audit.consistent ? 'matches' : 'differs from'} snapshot balance
          </Badge>
        )}
      </div>
      <div className="scroll" style={{ maxHeight: 280 }}>
        <table className="table">
          <thead>
            <tr>
              <th>v</th>
              <th>When</th>
              <th>Event</th>
              <th className="right">Amount</th>
              <th className="right">Balance</th>
            </tr>
          </thead>
          <tbody>
            {[...steps].reverse().map((s) => (
              <tr key={s.version} title={s.explanation}>
                <td className="num faint">{s.version}</td>
                <td className="small muted nowrap">{fmtDate(s.ts)}</td>
                <td className="small">
                  <Badge kind={s.kind === 'deposit' ? 'ok' : s.kind === 'withdrawal' ? 'warn' : 'info'}>{s.kind}</Badge>{' '}
                  {s.description} <span className="faint">{s.counterparties.length ? `↔ ${s.counterparties.join(', ')}` : ''}</span>
                </td>
                <td className="right num" style={{ color: s.amount < 0 ? 'var(--bad)' : 'var(--ok)' }}>
                  {s.amount > 0 ? '+' : ''}
                  {money(s.amount, account.currency)}
                </td>
                <td className="right num">{money(s.balance_after, account.currency)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {!steps.length && <div className="empty">No events before this moment.</div>}
      </div>
    </Card>
  )
}

function RaceDemo() {
  const [cfg, setCfg] = useState({ mode: 'naive', start_balance: 300, amount: 200, concurrent: 10, think_ms: 30 })
  const [res, setRes] = useState(null)
  const [run, busy] = useAction()
  const go = () => run(async () => setRes(await A.post('/demo/race', cfg)))
  return (
    <Card
      title="Race: concurrent transfers"
      icon={Swords}
      glow
      hint={`${cfg.concurrent} transfers of ₹${cfg.amount} fire at the same instant from an account holding ₹${cfg.start_balance}. It can afford ${Math.floor(cfg.start_balance / cfg.amount)}. (R7)`}
    >
      <Segmented
        value={cfg.mode}
        onChange={(mode) => setCfg({ ...cfg, mode })}
        options={[['naive', 'Naive (the bug)'], ['optimistic', 'Optimistic (versions)'], ['pessimistic', 'Pessimistic (lock)']]}
      />
      <div className="small muted">
        {cfg.mode === 'naive' && 'Check the balance, think, then write: no re-check. Several transfers see ₹300 and all pass.'}
        {cfg.mode === 'optimistic' && 'Read the account version, check, then append only if the version is unchanged; otherwise re-read and retry.'}
        {cfg.mode === 'pessimistic' && 'Take the write lock first, then read + check + write. Everyone else waits in line.'}
      </div>
      <div className="grid g2">
        <Range label="Concurrent transfers" value={cfg.concurrent} min={2} max={10} onChange={(concurrent) => setCfg({ ...cfg, concurrent })} />
        <Range label="Think time (check → write)" value={cfg.think_ms} min={0} max={150} step={5} onChange={(think_ms) => setCfg({ ...cfg, think_ms })} format={(v) => `${v}ms`} />
      </div>
      <Button kind="primary" icon={Swords} onClick={go} busy={busy}>
        Fire {cfg.concurrent} transfers at once
      </Button>
      {res && (
        <>
          <div className="attempts" style={{ gridTemplateColumns: `repeat(${res.results.length}, 1fr)` }}>
            {res.results.map((r) => (
              <div key={r.i} className={`attempt ${r.ok ? 'ok' : 'no'}`} title={r.ok ? 'succeeded' : r.reason}>
                {r.ok ? '✓' : '✕'}
              </div>
            ))}
          </div>
          <div className="grid g3">
            <div>
              <div className="stat-label">Succeeded</div>
              <div className="stat-value" style={{ color: res.succeeded > res.can_afford ? 'var(--bad)' : 'var(--ok)' }}>
                {res.succeeded}
                <span className="faint" style={{ fontSize: 15 }}> / {res.can_afford} affordable</span>
              </div>
            </div>
            <div>
              <div className="stat-label">Source balance</div>
              <div className="stat-value" style={{ color: res.final_a < 0 ? 'var(--bad)' : undefined }}>
                ₹{res.final_a.toLocaleString('en-IN')}
              </div>
            </div>
            <div>
              <div className="stat-label">Version retries</div>
              <div className="stat-value">{res.optimistic_retries}</div>
            </div>
          </div>
          <Callout kind={res.invariant_held ? 'ok' : 'bad'} icon={res.invariant_held ? ShieldCheck : ShieldAlert}>
            {res.invariant_held
              ? `Invariant held: the account never went below zero. ${res.mode === 'optimistic' ? `Losers detected a version conflict ${res.optimistic_retries} times, re-read and were correctly rejected.` : 'Transfers ran one at a time behind the lock.'}`
              : `Overdraft! ${res.succeeded} transfers each saw ₹${cfg.start_balance} and passed the check. Double-entry still balances (money moved, none was created), but the business rule broke.`}
          </Callout>
        </>
      )}
    </Card>
  )
}

function Integrity() {
  const [res, setRes] = useState(null)
  const [run, busy] = useAction()
  const checks = res
    ? [
        ['Every transaction sums to zero', res.every_transaction_balances],
        [`All money in the system nets to zero (${res.sum_per_currency.map((c) => `${c.currency} ${c.total}`).join(', ') || 'no entries'})`, res.sum_per_currency.every((c) => c.total === 0)],
        ['No customer account overdrawn', !res.customers_overdrawn.length],
        ['No credit line over its limit', !res.credit_over_limit.length],
        ['Every snapshot equals a full replay', res.snapshots_match_replay],
        ['History is immutable (UPDATE was refused)', res.entries_immutable],
      ]
    : []
  return (
    <Card title="Integrity check" icon={ListChecks} actions={<Button size="sm" icon={ShieldCheck} busy={busy} onClick={() => run(async () => setRes(await A.get('/integrity')))}>Run</Button>}>
      {!res && <div className="empty">Verifies double-entry, snapshots and immutability across the whole ledger.</div>}
      {checks.map(([label, ok]) => (
        <div className="check" key={label}>
          {ok ? <BadgeCheck size={16} color="var(--ok)" /> : <XCircle size={16} color="var(--bad)" />}
          {label}
        </div>
      ))}
      {res && (
        <div className="small faint">
          {res.transactions} transactions · {res.entries} entries · {res.snapshots} snapshots · {res.optimistic_retries_so_far} optimistic retries so far
        </div>
      )}
    </Card>
  )
}

export default function Ledger() {
  const [d, , refresh] = usePoll(() => A.get('/dashboard'), 1500)
  const [sel, setSel] = useState(null)
  const [run, busy] = useAction()
  const accounts = d?.accounts || []
  const custs = accounts.filter((a) => a.kind !== 'system')
  const system = accounts.filter((a) => a.kind === 'system' && a.version > 0)
  const selected = accounts.find((a) => a.id === sel) || custs[0]

  return (
    <div className="col" style={{ gap: 16 }}>
      <Card
        title="Accounts"
        icon={Landmark}
        hint="No account stores a balance. Each one is computed from its events: the latest snapshot plus the events after it."
        actions={
          <>
            <Button size="sm" kind="primary" icon={Database} busy={busy} onClick={() => run(() => A.post('/demo/seed').then(refresh), (r) => 'Loaded 30 days of demo history')}>
              Load demo data
            </Button>
            <Button size="sm" kind="ghost" onClick={() => run(() => A.post('/reset').then(refresh), 'Ledger reset')}>
              Reset
            </Button>
          </>
        }
      >
        {!custs.length && (
          <Callout>
            The ledger is empty. Click <b>Load demo data</b> for five accounts with a month of back-dated history (good for time travel), or
            open your own accounts below.
          </Callout>
        )}
        <div className="accounts">
          {custs.map((a) => (
            <div key={a.id} className={`acct ${selected?.id === a.id ? 'sel' : ''}`} onClick={() => setSel(a.id)}>
              <div className="row between">
                <b>{a.name}</b>
                <Badge kind={a.kind === 'credit' ? 'warn' : 'neutral'}>
                  {a.kind} · {a.currency}
                </Badge>
              </div>
              <div className={`bal ${a.balance < 0 ? 'neg' : ''}`}>{money(a.balance, a.currency)}</div>
              <div className="tiny faint">
                v{a.version} · {a.snapshot_version ? `snapshot v${a.snapshot_version} + ${a.replayed_events} events` : `${a.replayed_events} events replayed`}
                {a.kind === 'credit' && ` · limit ${money(a.credit_limit, a.currency)}`}
              </div>
            </div>
          ))}
        </div>
        {!!system.length && (
          <div className="row wrap small faint" style={{ gap: 14 }}>
            <span>System accounts:</span>
            {system.map((a) => (
              <span key={a.id} className="mono">
                {a.id} {money(a.balance, a.currency)}
              </span>
            ))}
          </div>
        )}
      </Card>

      <div className="grid" style={{ gridTemplateColumns: 'minmax(0, 1.5fr) minmax(0, 1fr)' }}>
        {selected ? <TimeTravel account={selected} /> : <Card title="Time travel" icon={Clock3}><div className="empty">Open or seed an account first.</div></Card>}
        <div className="col" style={{ gap: 16 }}>
          <MoveMoney accounts={accounts} onDone={refresh} />
          <OpenAccount onDone={refresh} />
        </div>
      </div>

      <div className="grid g3">
        <RaceDemo />
        <Integrity />
        <Card title="Latest transactions" icon={ArrowRightLeft} hint="Immutable events with their legs (each sums to zero).">
          <div className="feed" style={{ maxHeight: 360 }}>
            {(d?.transactions || []).map((t) => (
              <div key={t.id} style={{ padding: '7px 6px', borderBottom: '1px solid var(--border)' }}>
                <div className="row between small">
                  <span>
                    <Badge kind={t.kind === 'deposit' ? 'ok' : t.kind === 'withdrawal' ? 'warn' : 'info'}>{t.kind}</Badge>{' '}
                    {t.description}
                  </span>
                  <span className="mono tiny faint">{fmtTime(t.ts)}</span>
                </div>
                <div className="mono tiny" style={{ marginTop: 4 }}>
                  {t.legs.map((l) => (
                    <div key={l.account + l.amount} className="row between">
                      <span className="faint">{l.account}</span>
                      <span style={{ color: l.amount < 0 ? 'var(--bad)' : 'var(--ok)' }}>
                        {l.amount > 0 ? '+' : ''}
                        {money(l.amount, l.currency)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ))}
            {!d?.transactions?.length && <div className="empty">No transactions yet.</div>}
          </div>
          {!!d?.rejections?.length && (
            <>
              <div className="divider" />
              <div className="stat-label">Rejected (nothing written)</div>
              {d.rejections.slice(0, 4).map((r) => (
                <div key={r.id} className="tiny" style={{ color: 'var(--bad)' }}>
                  {fmtTime(r.ts)} · {r.reason}
                </div>
              ))}
            </>
          )}
        </Card>
      </div>
    </div>
  )
}
