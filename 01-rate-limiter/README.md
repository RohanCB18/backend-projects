# 01 · Token Bucket Rate Limiter Service

A standalone service other APIs ask: *may client X make a request right now?*

```bash
..\.venv\Scripts\python -m uvicorn app.main:app --port 9101
```
```bash
..\.venv\Scripts\python loadtest.py --rps 600 --duration 5
```
```bash
..\.venv\Scripts\python -m pytest -q
```

## API

| Endpoint | What it does |
|---|---|
| `GET /check/{key}` / `POST /check {key, cost}` | 200 `ALLOW` or 429 `DENY`, always with rate-limit headers |
| `PUT /admin/clients/{key}` | per-client config: `mode`, `rate`, `burst` (bucket) or `limit`, `window` (sliding) |
| `GET /admin/clients[/{key}]` | configs, plus live bucket state |
| `GET /stats` | per-second ALLOW/DENY counts per client (dashboard) |
| `POST /loadtest` | runs `loadtest.py` in a child process and returns the verified report |

## Design decisions

* **Lazy refill, no timers.** Store `tokens` and `ts`. On each request:
  `tokens = min(burst, tokens + (now - ts) * rate)`, then try to take one.
* **One transaction per decision.** Load config, read bucket, refill, check,
  write: all inside one `BEGIN IMMEDIATE` SQLite transaction. IMMEDIATE takes
  the write lock up front, so two requests can never both see "1 token left",
  even from different processes.
* **One database thread.** Inside the server every SQLite call runs on a single
  dedicated thread, so decisions queue up fairly at ~0.1 ms each. With many
  threads, or many server processes sharing the file, losers wait in SQLite's
  sleep-and-retry busy handler, and under heavy load that collapsed throughput.
  Multiple processes are still *correct* (tested); one is just faster.
* **Sliding window = exact log.** One row per allowed request; delete rows older
  than the window, count the rest. No 2× burst at fixed-window boundaries.
* **Optional Redis backend** (`app/redis_backend.py`, `RL_BACKEND=redis`): the
  same logic as a Lua script, which Redis runs atomically.

## How each requirement is proven

| Req | Proof |
|---|---|
| R1 ALLOW/DENY via token bucket | `test_r1_*`: drains exactly `burst`, then refills at `rate` |
| R2 per-client limits | `test_r2_limits_are_per_client` |
| R3 survives restart | `test_r3_state_survives_a_restart` (new limiter object, same file) |
| R4 race-condition safe | `test_r4_concurrent_requests_never_double_spend` (400 concurrent → exactly 50) and `test_r4_no_double_spend_across_processes` (4 OS processes → exactly 120) |
| R5 sliding window | `test_r5_*`, including "no boundary burst" |
| R6 headers | `test_r6_headers_on_allow_and_deny` (`X-RateLimit-Limit/Remaining/Reset`, `Retry-After`) |
| R7 500+ req/s load test | `tests/test_loadtest.py` runs `loadtest.py` at 600 req/s against a real server |

**How the load test checks correctness.** Every response carries the server's
decision time. The script sorts all requests by that time and replays them
through a ten-line reference model of the algorithm, then compares ALLOW/DENY
request by request. Typical result: `600 req/s, 0 decisions differ from the
model`. It also checks the invariants: `allowed ≤ burst + rate × span`, and no
window holds more than `limit` ALLOWs.
