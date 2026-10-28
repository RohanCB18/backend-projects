import { useCallback, useEffect, useRef, useState } from 'react'

// Tiny fetch wrapper: api(1).get('/stats'), api(3).post('/transfer', body)
export class ApiError extends Error {
  constructor(message, status, body) {
    super(message)
    this.status = status
    this.body = body
  }
}

async function request(project, method, path, body, { raw = false } = {}) {
  const res = await fetch(`/api/${project}${path}`, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  if (raw) return res
  const text = await res.text()
  let data = null
  try {
    data = text ? JSON.parse(text) : null
  } catch {
    data = text
  }
  if (!res.ok) {
    const msg = (data && (data.error || data.detail)) || `${res.status} ${res.statusText}`
    throw new ApiError(typeof msg === 'string' ? msg : JSON.stringify(msg), res.status, data)
  }
  return data
}

export const api = (project) => ({
  get: (path) => request(project, 'GET', path),
  post: (path, body = {}) => request(project, 'POST', path, body),
  put: (path, body = {}) => request(project, 'PUT', path, body),
  del: (path) => request(project, 'DELETE', path),
  raw: (method, path, body) => request(project, method, path, body, { raw: true }),
})

// Poll an endpoint every `ms` while the component is mounted.
export function usePoll(fn, ms, deps = []) {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const fnRef = useRef(fn)
  fnRef.current = fn
  const tick = useCallback(async () => {
    try {
      setData(await fnRef.current())
      setError(null)
    } catch (e) {
      setError(e)
    }
  }, [])
  useEffect(() => {
    let alive = true
    let timer
    const loop = async () => {
      await tick()
      if (alive) timer = setTimeout(loop, ms)
    }
    loop()
    return () => {
      alive = false
      clearTimeout(timer)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ms, tick, ...deps])
  return [data, error, tick]
}

// Global toast bus
const listeners = new Set()
export function toast(message, kind = 'info') {
  for (const l of listeners) l({ id: Math.random(), message, kind })
}
export function useToasts() {
  const [items, setItems] = useState([])
  useEffect(() => {
    const l = (t) => {
      setItems((xs) => [...xs, t])
      setTimeout(() => setItems((xs) => xs.filter((x) => x.id !== t.id)), 4200)
    }
    listeners.add(l)
    return () => listeners.delete(l)
  }, [])
  return items
}

// Run an async action with a busy flag and error toast.
export function useAction() {
  const [busy, setBusy] = useState(false)
  const run = useCallback(async (fn, okMessage) => {
    setBusy(true)
    try {
      const r = await fn()
      if (okMessage) toast(typeof okMessage === 'function' ? okMessage(r) : okMessage, 'ok')
      return r
    } catch (e) {
      toast(e.message || String(e), 'bad')
      return undefined
    } finally {
      setBusy(false)
    }
  }, [])
  return [run, busy]
}

export const fmtTime = (ts) =>
  new Date(ts * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })
export const fmtDate = (ts) =>
  new Date(ts * 1000).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false })
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
