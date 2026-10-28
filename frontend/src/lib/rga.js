// RGA sequence CRDT: the browser twin of 04-collab-sync/app/rga.py.
// Same op format, same integration rule; a pytest runs both on identical op
// streams and checks they agree. See the Python file for the full explanation.
//
//   insert: { t: "i", id: [counter, site], after: [counter, site] | null, ch }
//   delete: { t: "d", id: [counter, site], target: [counter, site] }

export const keyOf = (id) => `${id[0]}@${id[1]}`
export const opKey = (op) => keyOf(op.id)

// (counter, site) tuple comparison, identical to Python's
export function cmpId(a, b) {
  if (a[0] !== b[0]) return a[0] - b[0]
  return a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0
}

export class RGA {
  constructor(site) {
    this.site = site
    this.clock = 0
    this.elems = [] // { id, ch, deleted } in document order, tombstones included
    this.index = new Map() // key -> elem
    this.deletesSeen = new Set()
    this.pending = []
  }

  text() {
    let s = ''
    for (const e of this.elems) if (!e.deleted) s += e.ch
    return s
  }

  visible() {
    return this.elems.filter((e) => !e.deleted)
  }

  nextId() {
    this.clock += 1
    return [this.clock, this.site]
  }

  insert(pos, text) {
    const vis = this.visible()
    if (pos < 0 || pos > vis.length) throw new RangeError(`bad position ${pos}`)
    let after = pos > 0 ? vis[pos - 1].id : null
    const ops = []
    for (const ch of text) {
      const op = { t: 'i', id: this.nextId(), after, ch }
      this.apply(op)
      ops.push(op)
      after = op.id
    }
    return ops
  }

  delete(pos, length = 1) {
    const vis = this.visible()
    if (pos < 0 || pos + length > vis.length) throw new RangeError(`bad range ${pos}+${length}`)
    const ops = []
    for (const e of vis.slice(pos, pos + length)) {
      const op = { t: 'd', id: this.nextId(), target: e.id }
      this.apply(op)
      ops.push(op)
    }
    return ops
  }

  // Idempotent. Ops whose dependencies haven't arrived wait in `pending`.
  apply(op) {
    const changed = this.tryApply(op)
    if (changed === null) {
      const k = opKey(op)
      if (!this.pending.some((p) => opKey(p) === k)) this.pending.push(op)
      return false
    }
    if (changed) this.drainPending()
    return changed
  }

  tryApply(op) {
    this.clock = Math.max(this.clock, op.id[0])
    const k = keyOf(op.id)
    if (op.t === 'i') {
      if (this.index.has(k)) return false
      let i = 0
      if (op.after !== null) {
        const ref = this.index.get(keyOf(op.after))
        if (!ref) return null
        i = this.elems.indexOf(ref) + 1
      }
      while (i < this.elems.length && cmpId(this.elems[i].id, op.id) > 0) i++
      const e = { id: op.id, ch: op.ch, deleted: false }
      this.elems.splice(i, 0, e)
      this.index.set(k, e)
      return true
    }
    if (op.t === 'd') {
      if (this.deletesSeen.has(k)) return false
      const target = this.index.get(keyOf(op.target))
      if (!target) return null
      this.deletesSeen.add(k)
      const already = target.deleted
      target.deleted = true
      return !already
    }
    throw new Error(`unknown op type ${op.t}`)
  }

  drainPending() {
    let progress = true
    while (progress && this.pending.length) {
      progress = false
      for (const op of [...this.pending]) {
        if (this.tryApply(op) !== null) {
          this.pending.splice(this.pending.indexOf(op), 1)
          progress = true
        }
      }
    }
  }

  // id of the visible element just before visible index `pos` (for caret anchoring)
  idBefore(pos) {
    const vis = this.visible()
    return pos > 0 && pos <= vis.length ? vis[pos - 1].id : null
  }

  // visible index right after element `id` (or 0 for null). Deleted anchors fall back
  // to the nearest visible element before them.
  posAfter(id) {
    if (id === null) return 0
    let pos = 0
    for (const e of this.elems) {
      if (!e.deleted) pos++
      if (e.id[0] === id[0] && e.id[1] === id[1]) return pos
    }
    return pos
  }
}

// Turn "old text -> new text" (one textarea input event) into CRDT ops.
// A single edit is always one contiguous replace, so prefix/suffix diff is exact.
export function diffToOps(doc, oldText, newText) {
  let start = 0
  while (start < oldText.length && start < newText.length && oldText[start] === newText[start]) start++
  let endOld = oldText.length
  let endNew = newText.length
  while (endOld > start && endNew > start && oldText[endOld - 1] === newText[endNew - 1]) {
    endOld--
    endNew--
  }
  const ops = []
  if (endOld > start) ops.push(...doc.delete(start, endOld - start))
  if (endNew > start) ops.push(...doc.insert(start, newText.slice(start, endNew)))
  return ops
}
