import { Moon, Sun } from 'lucide-react'
import { useEffect, useState } from 'react'
import { Offline, Toasts } from './components/ui'
import { usePoll } from './lib/api'
import Cache from './projects/Cache'
import Collab from './projects/Collab'
import Home from './projects/Home'
import JobQueue from './projects/JobQueue'
import Ledger from './projects/Ledger'
import { PROJECTS } from './projects/meta'
import RateLimiter from './projects/RateLimiter'

const PAGES = { 1: RateLimiter, 2: JobQueue, 3: Ledger, 4: Collab, 5: Cache }

function useHash() {
  const read = () => {
    const m = window.location.hash.match(/^#\/?(\d)/)
    return m ? Number(m[1]) : 0
  }
  const [page, setPage] = useState(read)
  useEffect(() => {
    const on = () => setPage(read())
    window.addEventListener('hashchange', on)
    return () => window.removeEventListener('hashchange', on)
  }, [])
  const go = (n) => {
    window.location.hash = n ? `/${n}` : '/'
  }
  return [page, go]
}

async function checkHealth() {
  const entries = await Promise.all(
    PROJECTS.map(async (p) => {
      try {
        const r = await fetch(`/api/${p.n}/health`, { signal: AbortSignal.timeout(2500) })
        return [p.n, r.ok]
      } catch {
        return [p.n, false]
      }
    }),
  )
  return Object.fromEntries(entries)
}

export default function App() {
  const [page, go] = useHash()
  const [health] = usePoll(checkHealth, 3000)
  const [theme, setTheme] = useState(() => {
    try {
      return localStorage.getItem('theme') || 'dark'
    } catch {
      return 'dark'
    }
  })

  useEffect(() => {
    document.documentElement.dataset.theme = theme
    try {
      localStorage.setItem('theme', theme)
    } catch {
      /* private mode */
    }
  }, [theme])

  // keyboard: 1-5 pick a project, 0 / h goes home (not while typing)
  useEffect(() => {
    const on = (e) => {
      const t = e.target
      if (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || e.metaKey || e.ctrlKey || e.altKey) return
      if (/^[1-5]$/.test(e.key)) go(Number(e.key))
      if (e.key === '0' || e.key === 'h') go(0)
    }
    window.addEventListener('keydown', on)
    return () => window.removeEventListener('keydown', on)
  }, [go])

  const meta = PROJECTS.find((p) => p.n === page)
  const Page = PAGES[page]

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand" onClick={() => go(0)} style={{ cursor: 'pointer' }}>
          <div className="brand-mark">BM</div>
          <div>
            <h1>Backend Mastery</h1>
            <p>5 systems, built from first principles</p>
          </div>
        </div>
        <div className="nav-label">Projects</div>
        {PROJECTS.map((p) => (
          <button
            key={p.n}
            className={`nav-item ${page === p.n ? 'active' : ''}`}
            style={{ '--c': p.color }}
            onClick={() => go(p.n)}
            title={`${p.title} (press ${p.n})`}
          >
            <span className="nav-num">0{p.n}</span>
            <span>
              <div className="nav-title">{p.short}</div>
              <div className="nav-sub">{p.level}</div>
            </span>
            <span className={`dot ${health ? (health[p.n] ? 'ok' : 'bad') : ''}`} title={health?.[p.n] ? 'backend up' : 'backend down'} />
          </button>
        ))}
        <div className="sidebar-foot">
          <div className="faint tiny">
            Press <span className="kbd">1</span>–<span className="kbd">5</span> to switch, <span className="kbd">h</span> for home
          </div>
          <button className="btn ghost sm" onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')} style={{ justifyContent: 'flex-start' }}>
            {theme === 'dark' ? <Sun size={14} /> : <Moon size={14} />}
            {theme === 'dark' ? 'Light theme' : 'Dark theme'}
          </button>
        </div>
      </aside>

      <main className="main" style={meta ? { '--accent': meta.color } : undefined}>
        {!meta && <Home onOpen={go} health={health} />}
        {meta && (
          <>
            <header className="page-head">
              <div>
                <div className="eyebrow">
                  Project 0{meta.n} · {meta.level}
                </div>
                <h2 className="page-title">{meta.title}</h2>
                <p className="page-sub">{meta.tagline}</p>
                <div className="faint small mono" style={{ marginTop: 8 }}>
                  {meta.stack}
                </div>
              </div>
            </header>
            <div className="reqs">
              {meta.reqs.map(([name, how], i) => (
                <span className="req" key={name}>
                  <b>R{i + 1}</b>
                  {name}
                  <span className="tip">{how}</span>
                </span>
              ))}
            </div>
            {health && !health[meta.n] && <Offline project={meta.n} port={meta.port} />}
            <Page key={page} />
          </>
        )}
      </main>
      <Toasts />
    </div>
  )
}
