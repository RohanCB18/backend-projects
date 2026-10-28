# Backend Mastery: 5 projects + one console

Five backend systems built from first principles with **FastAPI**, plus a single
React console (`frontend/`) where you pick project **1–5** and watch each one's
guarantees hold while you try to break them.

| # | Project | Level | Core idea you build yourself |
|---|---------|-------|------------------------------|
| 01 | [Token Bucket Rate Limiter](01-rate-limiter/) | Beginner | atomic read-refill-check-write, sliding window, 600 req/s load test |
| 02 | [Reliable Job Queue](02-job-queue/) | Easy–Medium | leases, retries + backoff, dead-letter queue, idempotent jobs |
| 03 | [Event-Sourced Ledger](03-ledger/) | Intermediate | append-only double entry, derived balances, optimistic concurrency |
| 04 | [Collaborative Sync Engine](04-collab-sync/) | Advanced | RGA sequence CRDT (Python + JS), offline merge, history/revert |
| 05 | [Self-Healing Distributed Cache](05-distributed-cache/) | Expert | consistent hashing, gossip failure detection, replication, rebalancing |

No library solves any project's core problem: no rate-limit library, no
Celery, no CRDT library, no Redis Cluster.

## Run it

Needs Python 3.11+ and Node 18+. **No Docker required.**

```bash
python -m venv .venv
```
```bash
.venv\Scripts\pip install -r requirements.txt
```
```bash
.venv\Scripts\python dev.py
```

Then open **http://127.0.0.1:5173** and press `1`–`5` to switch projects (`h` for home).
`dev.py` installs the frontend's npm packages on first run, starts all five
backends and the UI, and stops everything (whole process trees) on Ctrl+C.

* `python dev.py 1 4`: only backends 01 and 04 (+ UI)
* `python dev.py --no-ui`: backends only
* Each backend also has interactive API docs at `http://127.0.0.1:910N/docs`.

Use `127.0.0.1`, not `localhost`: on Windows "localhost" tries IPv6 first and
every new connection waits ~200 ms before falling back.

| Service | Port |
|---|---|
| 01 rate limiter | 9101 |
| 02 job queue | 9102 |
| 03 ledger | 9103 |
| 04 collab sync | 9104 (HTTP + WebSockets) |
| 05 cache controller | 9105 (cache nodes on 9201+) |
| console (Vite) | 5173 (proxies `/api/N/...` to backend N) |

## Tests

Every requirement of every project has a test or a script that demonstrates it.

```bash
cd 01-rate-limiter && ..\.venv\Scripts\python -m pytest -q
```

Same command inside each project folder. Approximate run times: 01 ~30 s
(includes a real 600 req/s load test), 02 ~15 s, 03 ~5 s, 04 ~12 s
(includes 400 random property-test histories), 05 ~70 s (real node
processes, two chaos runs with live node kills).

## Design choices that apply to several projects

* **SQLite instead of Postgres/Redis.** It is built into Python, runs
  natively on Windows and needs no server, which makes every project
  start with one command. Each project still implements its own concurrency
  control explicitly (`BEGIN IMMEDIATE` transactions, version checks,
  `UPDATE … RETURNING` claims), and the multi-process tests prove it holds
  across OS processes, not just threads.
  01 also ships an optional Redis + Lua backend (`RL_BACKEND=redis`, Redis via
  `docker compose up -d`) to show the classic approach.
* **Windows timer resolution.** Sleeps and timeouts on Windows default to
  15.6 ms ticks; each server requests 1 ms (`timeBeginPeriod(1)`) at startup.

## Known limitation

Project 05 runs six Python processes that talk to each other constantly. On
the development machine, while that cluster was running, *other* services'
600 req/s bursts suffered multi-second TCP stalls (a Windows networking
effect; decisions stayed 100% correct). For a clean 01 load-test number run
only that service: `python dev.py 1`.
