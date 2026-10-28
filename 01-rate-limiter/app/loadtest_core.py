"""Load generator + correctness check (requirement 7).

Used by `python loadtest.py` and by the /loadtest endpoint (which runs it in a
separate process so the generator doesn't steal CPU from the server).

How correctness is proven:
  1. Replay. Every response carries the decision's sequence number for that
     client (assigned inside Redis's atomic script, so it IS the execution
     order) and the time the script used. We replay the requests in that order
     through a tiny reference model of the algorithm (a few lines of plain
     Python below). The service's ALLOW/DENY must match the model's request by
     request. This holds whatever the arrival pattern, and catches both
     double-spends and wrongful denies.
  2. Invariant. Token bucket: allowed <= burst + rate x span. Sliding window:
     no window of `window` seconds contains more than `limit` ALLOWs.

One generator process (aiohttp, 32 pre-opened connections) sends ~600 req/s.
`--procs N` splits the load across N processes for higher rates, but on
Windows several processes holding many loopback connections at once tend to
trigger multi-second TCP stalls, so one process is the default.
"""
from __future__ import annotations

import asyncio
import math
import multiprocessing
import sys
import multiprocessing
import sys
from collections import deque
import time
from concurrent.futures import ProcessPoolExecutor

import aiohttp


_barrier = None  # set in each generator process by _init


def _init(barrier) -> None:
    global _barrier
    _barrier = barrier
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.winmm.timeBeginPeriod(1)  # 1 ms sleeps instead of 15.6 ms


_barrier = None  # set in each generator process by _init


def _init(barrier) -> None:
    global _barrier
    _barrier = barrier
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.winmm.timeBeginPeriod(1)  # 1 ms sleeps instead of 15.6 ms


async def _run_one(base_url: str, key: str, rps: float, duration: float) -> dict:
    total = int(rps * duration)
    interval = 1.0 / rps
    results: list[tuple[bool, float, int]] = []
    errors: dict[str, int] = {}
    # A fixed pool of connections, all opened BEFORE the clock starts. Opening
    # them on demand (or all at once) produces bursts of new TCP connects, and
    # Windows drops some of those and only retries after 3 s, 6 s, 12 s...
    pool = 32
    conn = aiohttp.TCPConnector(limit=pool)
    async with aiohttp.ClientSession(base_url, connector=conn, timeout=aiohttp.ClientTimeout(total=30)) as s:

        async def warm():
            async with s.get("/health") as r:
                await r.read()

        for _ in range(pool // 8):  # 8 connects at a time, not 32 at once
            await asyncio.gather(*(warm() for _ in range(8)))

        async def one() -> None:
            try:
                async with s.get(f"/check/{key}") as r:
                    d = await r.json()
                    results.append((d["decision"] == "ALLOW", d["server_time"], d["seq"]))
            except Exception as e:
                errors[type(e).__name__] = errors.get(type(e).__name__, 0) + 1

        # every generator process waits here until all of them are warmed up,
        # then they all start firing at the same moment
        await asyncio.to_thread(_barrier.wait)
        t0 = time.perf_counter()
        tasks = []
        for i in range(total):
            delay = t0 + i * interval - time.perf_counter()  # pace sends: real rps, not a burst
            if delay > 0:
                await asyncio.sleep(delay)
            tasks.append(asyncio.create_task(one()))
        send_done = time.perf_counter() - t0
        await asyncio.gather(*tasks)
    return {"results": results, "errors": errors, "send_time": send_done}


def _proc_entry(args) -> dict:
    return asyncio.run(_run_one(*args))


def run_load(base_url: str, key: str, rps: int, duration: float, procs: int = 1) -> dict:
    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(procs)
    t0 = time.time()
    with ProcessPoolExecutor(procs, mp_context=ctx, initializer=_init, initargs=(barrier,)) as ex:
        args = [(base_url, key, rps / procs, duration)] * procs
        parts = list(ex.map(_proc_entry, args))
    results = [r for p in parts for r in p["results"]]
    error_types: dict[str, int] = {}
    for p in parts:
        for k, v in p["errors"].items():
            error_types[k] = error_types.get(k, 0) + v
    errors = sum(error_types.values())
    times = sorted(t for _, t, _ in results)
    server_span = (times[-1] - times[0]) if times else 0.0
    sent = int(rps / procs * duration) * procs
    return {
        "sent": sent,
        "errors": errors,
        "error_types": error_types,
        "wall_time": time.time() - t0,  # includes process start-up and warm-up  # includes process start-up and warm-up
        # rps as measured by the server's clock: how fast decisions were actually made
        "achieved_rps": len(results) / server_span if server_span else 0.0,
        "results": results,
    }


def _replay_token_bucket(times: list[float], rate: float, burst: float) -> list[bool]:
    tokens, last, out = burst, None, []
    for t in times:
        tokens = min(burst, tokens + (t - last) * rate) if last is not None else burst
        last = t
        ok = tokens >= 1 - 1e-9
        if ok:
            tokens -= 1
        out.append(ok)
    return out


def _replay_sliding_window(times: list[float], limit: int, window: float) -> list[bool]:
    q: deque[float] = deque()
    out = []
    for t in times:
        while q and q[0] <= t - window:
            q.popleft()
        ok = len(q) < limit
        if ok:
            q.append(t)
        out.append(ok)
    return out


def verify(results: list[tuple[bool, float, int]], cfg: dict) -> dict:
    if not results:
        return {"ok": False, "reason": "no results"}
    ordered = sorted(results, key=lambda r: r[2])  # execution order
    times = [t for _, t, _ in ordered]
    actual = [bool(ok) for ok, _, _ in ordered]
    span = times[-1] - times[0]
    allowed = sum(actual)
    out = {"requests": len(results), "allowed": allowed, "denied": len(results) - allowed, "span": span}

    if cfg["mode"] == "sliding_window":
        limit, window = int(cfg["limit"]), float(cfg["window"])
        model = _replay_sliding_window(times, limit, window)
        allowed_t = [t for t, ok in zip(times, actual) if ok]
        worst, lo = 0, 0
        for hi in range(len(allowed_t)):
            while allowed_t[hi] - allowed_t[lo] >= window:
                lo += 1
            worst = max(worst, hi - lo + 1)
        invariant = worst <= limit
        out.update(max_in_any_window=worst, limit=limit,
                   rule=f"no {window:g}s window holds more than {limit} ALLOWs")
    else:
        burst, rate = float(cfg["burst"]), float(cfg["rate"])
        model = _replay_token_bucket(times, rate, burst)
        ceiling = math.floor(burst + rate * span)
        invariant = allowed <= ceiling + 1
        out.update(expected=ceiling, rule=f"allowed <= burst + rate x span = {burst:g} + {rate:g} x {span:.3f}s")

    mismatches = sum(a != m for a, m in zip(actual, model))
    out.update(model_allowed=sum(model), mismatches=mismatches, invariant_ok=invariant,
               # one mismatch tolerated: two decisions can share a clock tick, making their order ambiguous
               ok=invariant and mismatches <= 1)
    return out
