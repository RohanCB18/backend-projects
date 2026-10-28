# 01 · Token Bucket Rate Limiter Service (Redis)

A standalone service other APIs ask: *may client X make a request right now?*
All state lives in Redis.

```bash
docker compose up -d --wait
```
```bash
..\.venv\Scripts\python -m uvicorn app.main:app --port 9101
```
```bash
..\.venv\Scripts\python loadtest.py --rps 600 --duration 5
```
```bash
..\.venv\Scripts\python -m pytest -q
```

The first command runs in the repo root (it starts Redis on 127.0.0.1:16380).
Set `REDIS_URL` to use another Redis.

## API

| Endpoint | What it does |
|---|---|
| `GET /check/{key}` / `POST /check {key, cost}` | 200 `ALLOW` or 429 `DENY`, always with rate-limit headers |
| `PUT /admin/clients/{key}` | per-client config: `mode`, `rate`, `burst` (bucket) or `limit`, `window` (sliding) |
| `GET /admin/clients[/{key}]` | configs, plus live bucket state |
| `GET /stats` | per-second ALLOW/DENY counts per client (dashboard) |
| `POST /loadtest` | runs `loadtest.py` in a child process and returns the verified report |

## Design decisions (`app/limiter.py`)

* **One Lua script per decision.** Load config, read bucket, refill, check,
  write, count stats: all in one script. Redis runs a script atomically (no
  other command runs in between), so two requests can never both see "1 token
  left", even from different limiter processes sharing the Redis. It's also a
  single network round trip.
* **Lazy refill, no timers.** Store `tokens` and `ts`. On each request:
  `tokens = min(burst, tokens + (now - ts) * rate)`, then try to take one.
* **Redis's clock, not the caller's.** The script reads `redis.call('TIME')`, so
  several limiter instances on different machines agree on "now".
* **Time never runs backwards per key.** Redis here runs in Docker's VM, whose
  clock was measured stepping back by ~25 ms during a sync. If the script
  stored that earlier time, those milliseconds would be counted twice once the
  clock recovered (extra tokens). So `now = max(now, last ts)`, in both modes.
* **Full-precision numbers.** Redis's Lua `tostring()` keeps 14 significant
  digits, which silently rounds timestamps to 0.1 ms. Everything is written with
  `%.17g`. (The load test's replay check caught this one.)
* **Sliding window = exact log.** A sorted set with one member per allowed
  request; drop members older than the window, count the rest. No 2× burst at
  fixed-window boundaries.
* **Persistence.** Redis runs with append-only-file persistence (`docker-compose.yml`),
  so bucket state also survives a Redis restart.
* **Performance.** `hiredis` (C parser) roughly halves the CPU per request. One
  server process does ~600 req/s here; more processes are correct too (they
  share Redis), but on Windows many connections through Docker's port forwarder
  caused stalls, so one process is the default.

## How each requirement is proven

| Req | Proof |
|---|---|
| R1 ALLOW/DENY via token bucket | `test_r1_*`: drains exactly `burst`, then refills at `rate` |
| R2 per-client limits | `test_r2_limits_are_per_client` |
| R3 survives restart | `test_r3_state_survives_a_service_restart` (new limiter + new connection, same Redis) |
| R4 race-condition safe | `test_r4_concurrent_requests_never_double_spend` (400 concurrent → exactly 50) and `test_r4_no_double_spend_across_service_instances` (4 OS processes → exactly 120) |
| R5 sliding window | `test_r5_*`, including "no boundary burst" |
| R6 headers | `test_r6_headers_on_allow_and_deny` (`X-RateLimit-Limit/Remaining/Reset`, `Retry-After`) |
| R7 500+ req/s load test | `tests/test_loadtest.py` runs `loadtest.py` at 600 req/s against a real server |

**How the load test checks correctness.** The script also assigns every
decision a per-client sequence number (`INCR`, inside the same atomic script),
which is the exact execution order. The load test replays all requests in that
order through a ten-line reference model of the algorithm and compares
ALLOW/DENY request by request. Typical result: `~590 req/s, 0 decisions differ
from the model`. It also checks the invariants: `allowed ≤ burst + rate × span`,
and no window holds more than `limit` ALLOWs.
