// One collaborative-editing client: a local RGA replica + an outbox of ops the
// server hasn't acknowledged yet + a WebSocket to the sync engine (project 04).
//
// Offline / disconnects: edits always go to the local replica and the outbox
// first. Whenever we (re)connect we say "hello, I have everything up to seq N",
// apply what we missed, and resend the whole outbox. The server ignores ops it
// already has (unique op ids), so nothing is lost and nothing is duplicated.
import { RGA, diffToOps, opKey } from './rga'

const wsBase = () => `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/api/4`

export class SyncClient {
  constructor({ doc, name, color }) {
    this.doc = doc
    this.name = name
    this.color = color
    this.site = `${name.toLowerCase()}-${Math.random().toString(36).slice(2, 6)}`
    this.rga = new RGA(this.site)
    this.outbox = new Map()
    this.seq = 0
    this.online = true
    this.connected = false
    this.latency = 0
    this.peers = []
    this.listeners = new Set()
    this.lastAt = { out: 0, in: 0 }
    this.closed = false
    this.stats = { sent: 0, received: 0, resent: 0 }
  }

  subscribe(fn) {
    this.listeners.add(fn)
    return () => this.listeners.delete(fn)
  }

  emit(remote = false) {
    for (const fn of this.listeners) fn({ remote })
  }

  text() {
    return this.rga.text()
  }

  // ---- connection ---------------------------------------------------------------------
  connect() {
    if (this.closed || !this.online || this.ws) return
    const q = `site=${encodeURIComponent(this.site)}&name=${encodeURIComponent(this.name)}`
    const ws = new WebSocket(`${wsBase()}/ws/${this.doc}?${q}`)
    this.ws = ws
    ws.onopen = () => {
      this.connected = true
      ws.send(JSON.stringify({ type: 'hello', since: this.seq }))
      if (this.outbox.size) {
        this.stats.resent += this.outbox.size
        this.sendOps([...this.outbox.values()])
      }
      this.emit()
    }
    ws.onmessage = (ev) => this.later('in', () => this.receive(JSON.parse(ev.data)))
    ws.onclose = () => {
      if (this.ws === ws) this.ws = null
      this.connected = false
      this.emit()
      if (this.online && !this.closed) setTimeout(() => this.connect(), 1000) // auto-reconnect
    }
    // presence lives on its own socket so cursor chatter never delays edits
    const pq = `${q}&color=${encodeURIComponent(this.color)}`
    this.pws?.close()
    const pws = new WebSocket(`${wsBase()}/presence/${this.doc}?${pq}`)
    this.pws = pws
    pws.onmessage = (ev) => {
      const m = JSON.parse(ev.data)
      if (m.type === 'presence') {
        this.peers = m.peers
        this.emit()
      }
    }
    pws.onclose = () => {
      if (this.pws === pws) this.pws = null
    }
  }

  disconnect() {
    const ws = this.ws
    this.ws = null
    this.connected = false
    ws?.close()
    this.pws?.close()
    this.pws = null
    this.peers = []
    this.emit()
  }

  setOnline(on) {
    this.online = on
    if (on) this.connect()
    else this.disconnect()
    this.emit()
  }

  close() {
    this.closed = true
    this.disconnect()
  }

  // Deliver in order but `latency` ms later (simulates a slow network).
  later(dir, fn) {
    if (!this.latency) return fn()
    const at = Math.max(Date.now() + this.latency, this.lastAt[dir])
    this.lastAt[dir] = at
    setTimeout(fn, at - Date.now())
  }

  // ---- messages ---------------------------------------------------------------------------
  receive(msg) {
    if (msg.type === 'sync' || msg.type === 'ops') {
      let changed = false
      for (const op of msg.ops) {
        changed = this.rga.apply(op) || changed
        this.seq = Math.max(this.seq, op.seq)
      }
      if (msg.type === 'sync') this.seq = Math.max(this.seq, msg.seq)
      this.stats.received += msg.ops.length
      this.emit(changed)
    } else if (msg.type === 'ack') {
      for (const k of msg.keys) this.outbox.delete(k)
      this.seq = Math.max(this.seq, msg.seq)
      this.emit()
    } else if (msg.type === 'reset') {
      this.rga = new RGA(this.site)
      this.outbox.clear()
      this.seq = 0
      this.emit(true)
    }
  }

  sendOps(ops) {
    if (!ops.length) return
    const ws = this.ws
    this.later('out', () => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'ops', ops }))
        this.stats.sent += ops.length
      }
    })
  }

  // ---- local edits ------------------------------------------------------------------------
  commit(ops) {
    for (const op of ops) this.outbox.set(opKey(op), op)
    if (this.connected) this.sendOps(ops)
    this.emit()
    return ops
  }

  edit(oldText, newText) {
    return this.commit(diffToOps(this.rga, oldText, newText))
  }

  insertAt(pos, text) {
    return this.commit(this.rga.insert(Math.min(pos, this.rga.visible().length), text))
  }

  // Send an edit, then kill the socket before the server's ack can arrive.
  dropMidEdit(text) {
    const ops = this.rga.insert(this.rga.visible().length, text)
    for (const op of ops) this.outbox.set(opKey(op), op)
    const ws = this.ws
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'ops', ops }))
    this.ws = null
    this.connected = false
    ws?.close() // reconnect happens automatically in a second; the outbox is resent
    this.emit()
  }

  sendCursor(id) {
    if (this.pws && this.pws.readyState === WebSocket.OPEN) this.pws.send(JSON.stringify({ cursor: id }))
  }
}
