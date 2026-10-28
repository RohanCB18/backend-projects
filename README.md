# Backend Mastery: 5 projects + one console

Five backend systems built from first principles with **FastAPI**, plus a single
React console (`frontend/`) where you pick project **1–5** and watch each one's
guarantees hold while you try to break them.

| # | Project | Level | Storage | Core idea you build yourself |
|---|---------|-------|---------|------------------------------|
| 01 | [Token Bucket Rate Limiter](01-rate-limiter/) | Beginner | Redis | atomic Lua decision, sliding window, 600 req/s load test |
| 02 | [Reliable Job Queue](02-job-queue/) | Easy–Medium | Postgres | `FOR UPDATE SKIP LOCKED` claims, leases, backoff, dead-letter queue, idempotency |
| 03 | [Event-Sourced Ledger](03-ledger/) | Intermediate | Postgres | append-only double entry, derived balances, optimistic concurrency |
| 04 | [Collaborative Sync Engine](04-collab-sync/) | Advanced | Postgres | RGA sequence CRDT (Python + JS), offline merge, history/revert |
| 05 | [Self-Healing Distributed Cache](05-distributed-cache/) | Expert | (it *is* the store) | consistent hashing, gossip, replication, rebalancing |

No library solves any project's core problem: no rate-limit library, no
Celery, no CRDT library, no Redis Cluster. Project 05 deliberately uses no
Redis: building a Redis-like cluster is the point of it.

## Run it

Needs Python 3.11+, Node 18+, and **Docker Desktop running** (for Redis and Postgres).

```bash
python -m venv .venv
```
```bash
.venv\Scripts\pip install -r requirements.txt
```
```bash
.venv\Scripts\python dev.py
```

`dev.py` starts Redis and Postgres (`docker compose up -d --wait`), installs the
frontend's npm packages on first run, starts all five backends and the UI, and
stops the services (whole process trees) on Ctrl+C. Then open
**http://127.0.0.1:5173** and press `1`–`5` to switch projects (`h` for home).

* `python dev.py 1 4`: only backends 01 and 04 (+ UI)
* `python dev.py --no-ui`: backends only
* `docker compose stop`: stop Redis/Postgres when you're done
* Each backend also has interactive API docs at `http://127.0.0.1:910N/docs`.

Use `127.0.0.1`, not `localhost`: on Windows "localhost" tries IPv6 first and
every new connection waits ~200 ms before falling back.

| Service | Port |
|---|---|
| Redis (docker) | 16380 |
| Postgres (docker; databases `jobqueue`, `ledger`, `collab`; user/password `backend`) | 15432 |
| 01 rate limiter | 9101 |
| 02 job queue | 9102 |
| 03 ledger | 9103 |
| 04 collab sync | 9104 (HTTP + WebSockets) |
| 05 cache controller | 9105 (cache nodes on 9201+) |
| console (Vite) | 5173 (proxies `/api/N/...` to backend N) |

The Redis/Postgres ports are deliberately not the defaults (6379/5432), so they
never clash with a Redis or PostgreSQL already installed on the machine.

## Tests

Every requirement of every project has a test or a script that demonstrates it.
With the containers up (`docker compose up -d --wait`):

```bash
cd 01-rate-limiter && ..\.venv\Scripts\python -m pytest -q
```

Same command inside each project folder. Approximate run times: 01 ~25 s
(includes a real 600 req/s load test), 02 ~25 s, 03 ~15 s, 04 ~15 s
(includes 400 random property-test histories), 05 ~70 s (real node processes,
two chaos runs with live node kills). Tests for 02–04 each create a throwaway
Postgres schema and drop it afterwards, so they never touch the demo data.

## Design choices that apply to several projects

* **The database does the concurrency control.** 01 runs each decision as one
  Lua script (Redis executes scripts atomically). 02 claims jobs with
  `SELECT … FOR UPDATE SKIP LOCKED`. 03 locks the touched accounts in sorted
  order for the instant of the append and re-checks versions. Multi-process
  tests prove these hold across OS processes, not just threads.
* **Windows timer resolution.** Sleeps and timeouts on Windows default to
  15.6 ms ticks; each server requests 1 ms (`timeBeginPeriod(1)`) at startup.

## Troubleshooting

* **Docker Desktop crashes on start** with "An unexpected error occurred …
  dockerInference": quit it, rename `%LOCALAPPDATA%\Docker\run` (e.g. to
  `run_old`), start Docker Desktop again. It recreates the folder. (Docker
  leaves broken socket files there on this machine; `dev.py` prints the same hint.)
* **Load-test numbers.** On Windows, local TCP connections occasionally stall
  for several seconds under bursts, especially with Docker and project 05's
  six-process cluster running. Decisions stay 100% correct (the test replays
  every one); only the measured rate drops. The pytest load test checks
  correctness on every attempt and takes the best of 3 for the rate; for a clean number in the UI, run `python dev.py 1`.
