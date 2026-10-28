"""01 - Token Bucket Rate Limiter Service.

Other services call GET/POST /check for a client key and get ALLOW (200) or
DENY (429). Every response, allowed or not, carries the X-RateLimit-* headers.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from redis.asyncio import BlockingConnectionPool, Redis

from .limiter import RateLimiter

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:16380/0")


def make_limiter() -> RateLimiter:
    # A bounded pool: under load, requests wait briefly for a free connection
    # instead of opening hundreds of new sockets at once.
    pool = BlockingConnectionPool.from_url(REDIS_URL, decode_responses=True, max_connections=50, timeout=10)
    return RateLimiter(Redis(connection_pool=pool), prefix=os.getenv("RL_PREFIX", "rl:"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    if sys.platform == "win32":
        # Windows sleeps/timeouts default to 15.6 ms ticks; ask for 1 ms.
        import ctypes
        ctypes.windll.winmm.timeBeginPeriod(1)
    app.state.limiter = make_limiter()
    await app.state.limiter.ping()  # fail fast if Redis isn't up
    yield
    await app.state.limiter.close()


app = FastAPI(title="01 Token Bucket Rate Limiter", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
                   expose_headers=["X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset",
                                   "X-RateLimit-Mode", "Retry-After"])


def limiter(request: Request) -> RateLimiter:
    return request.app.state.limiter


class CheckBody(BaseModel):
    key: str
    cost: int = Field(1, ge=1)


class ClientConfig(BaseModel):
    mode: Literal["token_bucket", "sliding_window"] = "token_bucket"
    rate: float = Field(5.0, gt=0, description="token bucket: tokens refilled per second")
    burst: int = Field(10, ge=1, description="token bucket: bucket capacity")
    limit: int = Field(10, ge=1, description="sliding window: max requests per window")
    window: float = Field(1.0, gt=0, description="sliding window: window length in seconds")


async def _decide(request: Request, key: str, cost: int = 1) -> JSONResponse:
    d = await limiter(request).check(key, cost)
    return JSONResponse(d.to_dict(), status_code=200 if d.allowed else 429, headers=d.headers())


@app.get("/check/{key}")
async def check_get(key: str, request: Request):
    return await _decide(request, key)


@app.post("/check")
async def check_post(body: CheckBody, request: Request):
    return await _decide(request, body.key, body.cost)


# ---- admin -----------------------------------------------------------------
@app.get("/admin/clients")
async def list_clients(request: Request):
    return await limiter(request).list_configs()


@app.get("/admin/clients/{key}")
async def get_client(key: str, request: Request):
    lim = limiter(request)
    return {"key": key, "config": await lim.get_config(key), "state": await lim.peek(key)}


@app.put("/admin/clients/{key}")
async def put_client(key: str, cfg: ClientConfig, request: Request):
    return {"key": key, "config": await limiter(request).set_config(key, cfg.model_dump())}


@app.delete("/admin/clients/{key}")
async def delete_client(key: str, request: Request):
    await limiter(request).delete_config(key)
    return {"deleted": key}


@app.post("/admin/clients/{key}/reset")
async def reset_client(key: str, request: Request):
    await limiter(request).reset(key)
    return {"reset": key}


@app.get("/stats")
async def stats(request: Request, seconds: int = 60):
    return await limiter(request).stats(seconds)


# ---- load test (requirement 7, runnable from the UI) ------------------------
class LoadTestBody(BaseModel):
    key: str = "loadtest"
    rps: int = Field(600, ge=1, le=3000)
    duration: float = Field(3.0, gt=0, le=15)
    config: ClientConfig = ClientConfig(rate=50, burst=100)


@app.post("/loadtest")
async def loadtest(body: LoadTestBody):
    """Runs loadtest.py in a child process: the load generator must not share
    this server's event loop, or it would be measuring itself."""
    c = body.config
    base = os.getenv("SELF_URL", "http://127.0.0.1:9101")
    cmd = [sys.executable, "loadtest.py", "--json", "--url", base, "--key", body.key,
           "--rps", str(body.rps), "--duration", str(body.duration), "--mode", c.mode,
           "--rate", str(c.rate), "--burst", str(c.burst), "--limit", str(c.limit), "--window", str(c.window)]
    # plain subprocess in a worker thread: with --workers on Windows uvicorn runs a
    # selector event loop, which can't spawn asyncio subprocesses
    proc = await asyncio.to_thread(subprocess.run, cmd, cwd=HERE, capture_output=True, text=True, timeout=120)
    if not proc.stdout.strip():
        return JSONResponse({"error": proc.stderr[-2000:]}, status_code=500)
    return json.loads(proc.stdout.strip().splitlines()[-1])


@app.get("/health")
async def health(request: Request):
    return {"ok": await limiter(request).ping(), "backend": "redis"}
