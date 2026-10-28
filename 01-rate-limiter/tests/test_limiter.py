"""Each test maps to a requirement from the spec (R1..R7).

Needs Redis on 127.0.0.1:16380 (docker compose up -d). Every test uses its own
key prefix, so tests never see each other's buckets.
"""
import asyncio
import multiprocessing as mp
import os
import uuid

import httpx
import pytest
from redis.asyncio import Redis

from app.limiter import RateLimiter
from app.main import app

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:16380/0")


@pytest.fixture
def prefix():
    return f"test:{uuid.uuid4().hex[:8]}:"


@pytest.fixture
async def make(prefix):
    """Factory: each call gives a NEW limiter with its own Redis connection over
    the same keys. Two of them = the service before and after a restart, or two
    instances of the service."""
    clients = []

    def factory():
        r = Redis.from_url(REDIS_URL, decode_responses=True, max_connections=500)
        clients.append(r)
        return RateLimiter(r, prefix)

    yield factory
    keys = [k async for k in clients[0].scan_iter(f"{prefix}*")]
    if keys:
        await clients[0].delete(*keys)
    for c in clients:
        await c.aclose()


@pytest.fixture
def lim(make):
    return make()


async def test_r1_allow_until_bucket_empty_then_deny(lim):
    await lim.set_config("a", {"rate": 0.001, "burst": 5})
    decisions = [(await lim.check("a")).allowed for _ in range(7)]
    assert decisions == [True] * 5 + [False] * 2


async def test_r1_tokens_refill_lazily_over_time(lim):
    await lim.set_config("a", {"rate": 10, "burst": 1})  # one token every 100 ms
    assert (await lim.check("a")).allowed
    assert not (await lim.check("a")).allowed
    await asyncio.sleep(0.15)
    assert (await lim.check("a")).allowed


async def test_r2_limits_are_per_client(lim):
    await lim.set_config("small", {"rate": 0.001, "burst": 1})
    await lim.set_config("big", {"rate": 0.001, "burst": 3})
    assert [(await lim.check("small")).allowed for _ in range(3)] == [True, False, False]
    assert [(await lim.check("big")).allowed for _ in range(3)] == [True, True, True]


async def test_r3_state_survives_a_service_restart(make):
    first = make()
    await first.set_config("a", {"rate": 0.001, "burst": 3})
    for _ in range(3):
        await first.check("a")
    # "restart": a brand-new limiter + connection; nothing shared in Python memory
    fresh = make()
    assert not (await fresh.check("a")).allowed
    assert (await fresh.get_config("a"))["burst"] == 3


async def test_r4_concurrent_requests_never_double_spend(lim):
    await lim.set_config("hot", {"rate": 0.0001, "burst": 50})
    results = await asyncio.gather(*(lim.check("hot") for _ in range(400)))
    assert sum(d.allowed for d in results) == 50  # exactly the burst, never 51


def _hammer(prefix: str, n: int) -> int:
    async def go():
        r = Redis.from_url(REDIS_URL, decode_responses=True)
        lim = RateLimiter(r, prefix)
        out = sum([(d.allowed) for d in await asyncio.gather(*(lim.check("hot") for _ in range(n)))])
        await r.aclose()
        return out
    return asyncio.run(go())


async def test_r4_no_double_spend_across_service_instances(make, prefix):
    """Four OS processes = four instances of the limiter sharing one Redis
    (the distributed-mode stretch goal). Only Redis's atomic script can keep
    them from double-spending."""
    await make().set_config("hot", {"rate": 0.0001, "burst": 120})
    with mp.get_context("spawn").Pool(4) as pool:
        allowed = sum(pool.starmap(_hammer, [(prefix, 100)] * 4))
    assert allowed == 120


async def test_r5_sliding_window_mode(lim):
    await lim.set_config("sw", {"mode": "sliding_window", "limit": 3, "window": 0.3})
    assert [(await lim.check("sw")).allowed for _ in range(4)] == [True, True, True, False]
    await asyncio.sleep(0.35)
    assert (await lim.check("sw")).allowed


async def test_r5_sliding_window_has_no_boundary_burst(lim):
    """A fixed window would allow 2x the limit across a boundary; sliding must not."""
    await lim.set_config("sw", {"mode": "sliding_window", "limit": 5, "window": 0.4})
    times = []
    loop = asyncio.get_running_loop()
    for _ in range(60):
        if (await lim.check("sw")).allowed:
            times.append(loop.time())
        await asyncio.sleep(0.01)
    for i in range(len(times)):
        in_window = [t for t in times if times[i] - 0.4 < t <= times[i]]
        assert len(in_window) <= 5


async def test_r6_headers_on_allow_and_deny(lim):
    app.state.limiter = lim
    await lim.set_config("h", {"rate": 0.001, "burst": 1})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        ok = await c.get("/check/h")
        no = await c.get("/check/h")
    assert ok.status_code == 200 and ok.json()["decision"] == "ALLOW"
    assert no.status_code == 429 and no.json()["decision"] == "DENY"
    for r in (ok, no):
        for h in ("X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"):
            assert h in r.headers
    assert ok.headers["X-RateLimit-Limit"] == "1"
    assert ok.headers["X-RateLimit-Remaining"] == "0"
    assert "Retry-After" in no.headers
