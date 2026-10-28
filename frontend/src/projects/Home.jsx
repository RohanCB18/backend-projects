import { ArrowRight, CheckCircle2, Terminal } from 'lucide-react'
import { PROJECTS } from './meta'

export default function Home({ onOpen, health }) {
  const up = health ? Object.values(health).filter(Boolean).length : null
  return (
    <>
      <section className="hero">
        <div className="eyebrow" style={{ color: 'var(--p2)' }}>
          Backend Mastery · 5 projects
        </div>
        <h2>
          From rate limiting to
          <br />a self-healing cluster.
        </h2>
        <p>
          Five backend systems built from first principles with FastAPI, each with its own tests and a live demo here.
          Pick a project to watch its guarantees hold while you try to break them.
        </p>
        <div className="row wrap" style={{ marginTop: 18, gap: 8 }}>
          <span className={`badge ${up === 5 ? 'ok' : up ? 'warn' : 'neutral'}`}>
            <span className={`dot ${up === 5 ? 'ok live' : 'warn'}`} style={{ width: 6, height: 6 }} />
            {up === null ? 'checking backends…' : `${up}/5 backends online`}
          </span>
          <span className="badge neutral">
            <Terminal size={12} /> python dev.py
          </span>
          <span className="badge neutral">no Docker required</span>
        </div>
      </section>

      <div className="ladder">
        {PROJECTS.map((p) => (
          <button key={p.n} className="pcard" style={{ '--c': p.color }} onClick={() => onOpen(p.n)}>
            <div className="row between">
              <div className="big">0{p.n}</div>
              <span className={`dot ${health ? (health[p.n] ? 'ok' : 'bad') : ''}`} />
            </div>
            <h3>{p.title}</h3>
            <p>{p.tagline}</p>
            <div className="col" style={{ gap: 4, marginTop: 4 }}>
              {p.reqs.slice(0, 3).map(([r]) => (
                <span key={r} className="row tiny muted" style={{ gap: 6 }}>
                  <CheckCircle2 size={12} color={p.color} /> {r}
                </span>
              ))}
              <span className="tiny faint">+ {p.reqs.length - 3} more requirements</span>
            </div>
            <div className="foot">
              <div className="col" style={{ gap: 5 }}>
                <span className="level">{p.level}</span>
                <span className="bars5">
                  {[1, 2, 3, 4, 5].map((i) => (
                    <i key={i} className={i <= p.rank ? 'on' : ''} />
                  ))}
                </span>
              </div>
              <ArrowRight size={18} color={p.color} />
            </div>
          </button>
        ))}
      </div>
    </>
  )
}
