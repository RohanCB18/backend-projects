import { CheckCircle2, CloudOff, FlaskConical, History, RefreshCcw, RotateCcw, Scissors, Timer, Trash2, Wifi, WifiOff } from 'lucide-react'
import { Fragment, useEffect, useLayoutEffect, useMemo, useReducer, useRef, useState } from 'react'
import { Badge, Button, Card, Range, Switch } from '../components/ui'
import { api, fmtTime, sleep, toast, useAction, usePoll } from '../lib/api'
import { SyncClient } from '../lib/syncClient'

const A = api(4)
const DOC = 'demo'
const PEOPLE = [
  { name: 'Alice', color: '#f472b6' },
  { name: 'Bob', color: '#38bdf8' },
  { name: 'Carol', color: '#a3e635' },
]

function useClient(client) {
  const [, force] = useReducer((x) => x + 1, 0)
  useEffect(() => client.subscribe(force), [client])
}

function Editor({ client }) {
  useClient(client)
  const ta = useRef(null)
  const mirror = useRef(null)
  const anchor = useRef({ start: null, end: null })
  const text = client.text()
  const prev = useRef(text)

  // remember the caret as CRDT element ids, which survive remote edits (indexes don't)
  const remember = () => {
    const el = ta.current
    if (!el) return
    anchor.current = { start: client.rga.idBefore(el.selectionStart), end: client.rga.idBefore(el.selectionEnd) }
    client.sendCursor(anchor.current.end)
  }

  // after a remote change, put the caret back where it logically was
  useLayoutEffect(() => {
    const el = ta.current
    if (!el || prev.current === text) return
    prev.current = text
    if (document.activeElement === el && anchor.current.start !== undefined) {
      const s = client.rga.posAfter(anchor.current.start)
      const e = client.rga.posAfter(anchor.current.end)
      el.setSelectionRange(s, e)
    }
  }, [text, client])

  const onChange = (e) => {
    const next = e.target.value
    client.edit(prev.current, next)
    prev.current = client.text()
    remember()
  }

  // remote cursors, drawn in a transparent mirror div under the textarea
  const marks = useMemo(() => {
    const out = []
    for (const p of client.peers) {
      if (p.site === client.site || p.cursor === undefined) continue
      out.push({ pos: client.rga.posAfter(p.cursor ?? null), name: p.name, color: p.color })
    }
    return out.sort((a, b) => a.pos - b.pos)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client.peers, text])

  const pieces = []
  let last = 0
  marks.forEach((m, i) => {
    pieces.push(<Fragment key={`t${i}`}>{text.slice(last, m.pos)}</Fragment>)
    pieces.push(
      <span key={`c${i}`} className="rcursor" style={{ '--rc': m.color }}>
        <b>{m.name}</b>
      </span>,
    )
    last = m.pos
  })
  pieces.push(<Fragment key="end">{text.slice(last)}{'\n'}</Fragment>)

  const unsent = client.outbox.size
  return (
    <div className={`editor ${client.online ? '' : 'offline'}`} style={{ '--who': client.color }}>
      <div className="editor-head">
        <div className="row" style={{ gap: 8 }}>
          <span className="avatar">{client.name[0]}</span>
          <div>
            <b>{client.name}</b>
            <div className="tiny faint mono">{client.site}</div>
          </div>
        </div>
        <div className="row" style={{ gap: 8 }}>
          {client.connected ? (
            <Badge kind="ok" dot>
              live
            </Badge>
          ) : client.online ? (
            <Badge kind="warn" dot>
              reconnecting
            </Badge>
          ) : (
            <Badge kind="bad">
              <CloudOff size={11} /> offline
            </Badge>
          )}
          <Switch on={client.online} onChange={(on) => client.setOnline(on)} />
        </div>
      </div>
      <div className="pad">
        <div className="mirror" ref={mirror} aria-hidden>
          {pieces}
        </div>
        <textarea
          ref={ta}
          value={text}
          spellCheck={false}
          placeholder={`${client.name} types here…`}
          onChange={onChange}
          onSelect={remember}
          onKeyUp={remember}
          onClick={remember}
          onScroll={(e) => mirror.current && (mirror.current.scrollTop = e.target.scrollTop)}
        />
      </div>
      <div className="editor-foot">
        <span>
          seq {client.seq} · {unsent ? <b style={{ color: 'var(--warn)' }}>{unsent} unsent</b> : 'all acked'}
        </span>
        <span className="row" style={{ gap: 6 }}>
          <Timer size={12} />
          <input
            type="range"
            min="0"
            max="2000"
            step="100"
            value={client.latency}
            style={{ width: 80 }}
            title="artificial network latency"
            onChange={(e) => {
              client.latency = Number(e.target.value)
              client.emit()
            }}
          />
          <span className="mono">{client.latency}ms</span>
          <button
            className="btn ghost sm"
            title="send an edit, then kill the socket before the ack arrives"
            disabled={!client.connected}
            onClick={() => {
              client.dropMidEdit(` [${client.name}: sent, ack lost]`)
              toast(`${client.name}: edit sent, socket killed before the ack. Reconnecting will resend it.`, 'info')
            }}
          >
            <Scissors size={12} /> drop mid-edit
          </button>
        </span>
      </div>
    </div>
  )
}

function HistoryPanel({ onRevert }) {
  const [h] = usePoll(() => A.get(`/docs/${DOC}/history`), 1500)
  const maxSeq = h?.seq || 0
  const [seq, setSeq] = useState(null)
  const [preview, setPreview] = useState('')
  const [run, busy] = useAction()
  const cur = seq ?? maxSeq
  useEffect(() => {
    const t = setTimeout(() => A.get(`/docs/${DOC}/at/${cur}`).then((r) => setPreview(r.text)).catch(() => {}), 100)
    return () => clearTimeout(t)
  }, [cur])
  const color = (site) => PEOPLE.find((p) => site.startsWith(p.name.toLowerCase()))?.color || 'var(--text-3)'
  const edits = [...(h?.edits || [])].reverse()
  return (
    <Card
      title="History & time travel"
      icon={History}
      hint="The op log is the document. Scrub to any point, then revert: the revert is applied as NEW edits, so history only grows (R4)."
    >
      <Range
        label={seq === null || seq >= maxSeq ? `Live (op ${maxSeq})` : `After op ${cur} of ${maxSeq}`}
        value={cur}
        min={0}
        max={Math.max(maxSeq, 1)}
        onChange={(v) => setSeq(v >= maxSeq ? null : v)}
        format={() => ''}
      />
      <div className="preview">{preview || <span className="faint">(empty document)</span>}</div>
      <div className="row between">
        <span className="small faint">{edits.length} edit groups · {maxSeq} ops</span>
        <Button
          icon={RotateCcw}
          kind="primary"
          size="sm"
          busy={busy}
          disabled={seq === null || seq >= maxSeq}
          onClick={() => run(() => A.post(`/docs/${DOC}/revert`, { seq: cur }).then(() => setSeq(null)).then(onRevert), `Reverted to op ${cur}`)}
        >
          Revert to this version
        </Button>
      </div>
      <div className="hist">
        {edits.map((e) => (
          <div
            key={e.seq_start}
            className={`hist-row ${cur >= e.seq_start && cur <= e.seq_end ? 'sel' : ''}`}
            onClick={() => setSeq(e.seq_end >= maxSeq ? null : e.seq_end)}
          >
            <span className="dot" style={{ background: color(e.site) }} />
            <span className="mono tiny faint">{fmtTime(e.ts)}</span>
            <span className="small" style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              <b>{e.site.split('-')[0]}</b>{' '}
              {e.inserted && <span style={{ color: 'var(--ok)' }}>+“{e.inserted.length > 40 ? e.inserted.slice(0, 40) + '…' : e.inserted}”</span>}{' '}
              {e.deleted > 0 && <span style={{ color: 'var(--bad)' }}>−{e.deleted} chars</span>}
            </span>
            <span className="mono tiny faint">#{e.seq_end}</span>
          </div>
        ))}
        {!edits.length && <div className="empty">Start typing in any editor.</div>}
      </div>
    </Card>
  )
}

export default function Collab() {
  const [clients, setClients] = useState(null)
  const [server] = usePoll(() => A.get(`/docs/${DOC}`), 700)
  const [, tick] = useReducer((x) => x + 1, 0)

  useEffect(() => {
    const cs = PEOPLE.map((p) => new SyncClient({ doc: DOC, ...p }))
    cs.forEach((c) => c.connect())
    const unsubs = cs.map((c) => c.subscribe(tick))
    setClients(cs)
    return () => {
      unsubs.forEach((u) => u())
      cs.forEach((c) => c.close())
    }
  }, [])

  if (!clients) return null
  const texts = clients.map((c) => c.text())
  const unsent = clients.reduce((s, c) => s + c.outbox.size, 0)
  const allOnline = clients.every((c) => c.connected)
  const same = texts.every((t) => t === texts[0]) && server && server.text === texts[0]
  const state = same && !unsent ? 'ok' : !allOnline ? 'offline' : 'syncing'

  const setAllLatency = (ms) => clients.forEach((c) => ((c.latency = ms), c.emit()))

  const catDemo = async () => {
    const [alice, bob] = clients
    await A.post(`/docs/${DOC}/reset`)
    setAllLatency(0)
    await sleep(300)
    alice.insertAt(0, 'cat')
    await sleep(600)
    setAllLatency(1500)
    alice.insertAt(3, 's') // "cats"
    bob.insertAt(1, 'h') // "chat"  (concurrently: neither has seen the other's edit)
    toast('Alice appends "s" at 3, Bob inserts "h" at 1, concurrently with 1.5 s latency. Watch them converge to "chats".', 'info')
    await sleep(4200)
    setAllLatency(0)
  }

  const raceDemo = async () => {
    setAllLatency(1200)
    clients.forEach((c, i) => c.insertAt(0, ['Alice was here. ', 'Bob was here. ', 'Carol was here. '][i]))
    toast('All three insert at position 0 at the same moment, 1.2 s apart from each other…', 'info')
    await sleep(4000)
    setAllLatency(0)
  }

  const offlineDemo = async () => {
    const [alice, bob, carol] = clients
    carol.setOnline(false)
    toast('Carol is offline. Everyone keeps typing…', 'info')
    await sleep(400)
    alice.insertAt(0, '[Alice online edit] ')
    bob.insertAt(bob.text().length, ' [Bob online edit]')
    carol.insertAt(0, '[Carol wrote this offline] ')
    carol.insertAt(carol.text().length, ' [more offline work]')
    await sleep(2500)
    carol.setOnline(true)
    toast('Carol reconnected: missed ops applied, outbox replayed. Nobody lost anything.', 'ok')
  }

  return (
    <div className="col" style={{ gap: 16 }}>
      <div className={`converge ${state === 'ok' ? 'ok' : state === 'offline' ? '' : 'diverged'}`}>
        {state === 'ok' ? <CheckCircle2 size={22} color="var(--ok)" /> : state === 'offline' ? <WifiOff size={22} color="var(--text-3)" /> : <RefreshCcw size={22} color="var(--warn)" />}
        <div className="grow">
          <b>
            {state === 'ok'
              ? 'Converged: all 3 replicas and the server hold the identical document'
              : state === 'offline'
                ? 'A client is offline; replicas may differ until it reconnects'
                : 'Syncing… replicas temporarily differ'}
          </b>
          <div className="small muted">
            {texts.map((t, i) => `${PEOPLE[i].name}: ${t.length} chars`).join(' · ')} · server: {server?.text.length ?? '–'} chars, {server?.tombstones ?? 0} tombstones · {unsent} unsent ops
          </div>
        </div>
        <Badge kind={same ? 'ok' : 'warn'}>{same ? 'identical' : 'diverged'}</Badge>
      </div>

      <div className="row wrap">
        <Button icon={FlaskConical} kind="primary" onClick={catDemo}>
          The "cat → chats" example
        </Button>
        <Button icon={Wifi} onClick={raceDemo}>
          3-way insert race (1.2 s latency)
        </Button>
        <Button icon={CloudOff} onClick={offlineDemo}>
          Offline merge
        </Button>
        <span className="grow" />
        <Button kind="ghost" icon={Trash2} onClick={() => A.post(`/docs/${DOC}/reset`).then(() => toast('Document cleared', 'ok'))}>
          Clear document
        </Button>
      </div>

      <div className="editors">
        {clients.map((c) => (
          <Editor key={c.site} client={c} />
        ))}
      </div>

      <div className="grid" style={{ gridTemplateColumns: 'minmax(0, 1.3fr) minmax(0, 1fr)' }}>
        <HistoryPanel onRevert={() => {}} />
        <Card title="How it stays consistent" icon={CheckCircle2}>
          <div className="col small muted" style={{ gap: 10 }}>
            <div>
              <b style={{ color: 'var(--text)' }}>Ids, not indexes.</b> Each character gets an id <code>(lamport, site)</code> and is
              inserted <i>after</i> another character's id. Positions shift under concurrent edits; ids never do.
            </div>
            <div>
              <b style={{ color: 'var(--text)' }}>Same rule everywhere.</b> Concurrent inserts after the same character are ordered by
              id, newest first, on every replica, so any delivery order gives the same text.
            </div>
            <div>
              <b style={{ color: 'var(--text)' }}>Tombstones.</b> Deletes only hide a character, so later edits can still refer to it.
              The server holds {server?.tombstones ?? 0} of them right now.
            </div>
            <div>
              <b style={{ color: 'var(--text)' }}>Outbox + dedupe.</b> Unacked ops are resent on reconnect; the server stores each op id
              once. Try <i>drop mid-edit</i> under any editor.
            </div>
            <div>
              <b style={{ color: 'var(--text)' }}>Presence on its own channel.</b> The coloured carets come from a separate WebSocket, so
              cursor chatter never delays edits.
            </div>
          </div>
        </Card>
      </div>
    </div>
  )
}
