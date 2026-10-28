"""02 - Reliable Job Queue with Dead-Letter Recovery (HTTP API + in-process workers)."""
from __future__ import annotations

import os
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .handlers import HANDLERS
from .queue import JobQueue
from .worker import WorkerPool

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.getenv("JQ_DB", os.path.join(HERE, "data", "jobqueue.db"))
LEASE = float(os.getenv("JQ_LEASE", "5"))
BACKOFF = float(os.getenv("JQ_BACKOFF", "1"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.winmm.timeBeginPeriod(1)
    q = JobQueue(DB, lease_seconds=LEASE, base_backoff=BACKOFF)
    app.state.q = q
    app.state.pool = WorkerPool(q)
    app.state.procs: dict[str, subprocess.Popen] = {}
    app.state.pool.forget_dead()
    app.state.pool.add(int(os.getenv("JQ_WORKERS", "3")))
    yield
    app.state.pool.shutdown()
    for p in app.state.procs.values():
        p.kill()


app = FastAPI(title="02 Reliable Job Queue", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def Q(request: Request) -> JobQueue:
    return request.app.state.q


class JobIn(BaseModel):
    type: str
    payload: dict[str, Any] = {}
    priority: int = 0
    delay: float = Field(0, ge=0)
    max_attempts: int = Field(4, ge=1, le=20)
    depends_on: int | None = None


@app.post("/jobs")
async def enqueue(job: JobIn, request: Request):
    if job.type not in HANDLERS:
        raise HTTPException(400, f"unknown job type; choose one of {sorted(HANDLERS)}")
    return await run_in_threadpool(Q(request).enqueue, **job.model_dump())


@app.get("/jobs")
async def list_jobs(request: Request, status: str | None = None, limit: int = 200):
    return await run_in_threadpool(Q(request).jobs, status, limit)


@app.get("/jobs/{job_id}")
async def get_job(job_id: int, request: Request):
    j = await run_in_threadpool(Q(request).job, job_id)
    if not j:
        raise HTTPException(404)
    return j


@app.get("/job-types")
async def job_types():
    return sorted(HANDLERS)


# ---- inspection (requirement 6) ---------------------------------------------
@app.get("/stats")
async def stats(request: Request):
    return await run_in_threadpool(Q(request).stats)


@app.get("/dashboard")
async def dashboard(request: Request):
    """Everything the UI needs in one poll."""
    q = Q(request)

    def build():
        return {
            "stats": q.stats(),
            "workers": q.workers(),
            "jobs": q.jobs(limit=80),
            "dlq": q.dead_letters(50),
            "events": q.events(limit=60),
            "side_effects": q.side_effects(),
        }

    return await run_in_threadpool(build)


@app.get("/dlq")
async def dlq(request: Request):
    return await run_in_threadpool(Q(request).dead_letters)


@app.post("/dlq/{job_id}/replay")
async def replay(job_id: int, request: Request):
    if not await run_in_threadpool(Q(request).replay, job_id):
        raise HTTPException(404, "not in the dead-letter queue")
    return {"replayed": job_id}


@app.post("/dlq/replay-all")
async def replay_all(request: Request):
    return {"replayed": await run_in_threadpool(Q(request).replay_all)}


# ---- workers ------------------------------------------------------------------
@app.get("/workers")
async def workers(request: Request):
    return await run_in_threadpool(Q(request).workers)


@app.post("/workers")
async def add_workers(request: Request, n: int = 1):
    return {"added": request.app.state.pool.add(n)}


@app.post("/workers/{worker_id}/stop")
async def stop_worker(worker_id: str, request: Request):
    return {"stopping": request.app.state.pool.stop(worker_id)}


@app.post("/workers/crash")
async def crash_worker(request: Request, worker_id: str | None = None):
    """Thread worker dies instantly, mid-job, without releasing its lease."""
    return {"crashed": request.app.state.pool.crash(worker_id)}


@app.post("/workers/process")
async def spawn_process_worker(request: Request):
    """Start a real worker OS process (python -m app.worker)."""
    procs = request.app.state.procs
    wid = f"proc-{len(procs) + 1}-{int(time.time()) % 10000}"
    procs[wid] = subprocess.Popen(
        [sys.executable, "-m", "app.worker", "--id", wid, "--db", DB, "--lease", str(LEASE), "--backoff", str(BACKOFF)],
        cwd=HERE,
    )
    return {"spawned": wid, "pid": procs[wid].pid}


@app.post("/workers/{worker_id}/kill")
async def kill_process_worker(worker_id: str, request: Request):
    """TerminateProcess / SIGKILL: no cleanup code runs in the worker at all."""
    p = request.app.state.procs.pop(worker_id, None)
    if not p:
        raise HTTPException(404, "no such worker process")
    p.kill()
    q = Q(request)
    await run_in_threadpool(q.worker_seen, worker_id, "killed", None)
    return {"killed": worker_id, "pid": p.pid}


@app.post("/workers/cleanup")
async def cleanup_workers(request: Request):
    await run_in_threadpool(request.app.state.pool.forget_dead)
    return {"ok": True}


# ---- side effects + demo scenarios ------------------------------------------
@app.get("/side-effects")
async def side_effects(request: Request):
    return await run_in_threadpool(Q(request).side_effects)


SCENARIOS: dict[str, list[dict]] = {
    # a realistic mix
    "mixed": [
        *({"type": "send_email", "payload": {"to": f"user{i}@college.edu", "duration": 1.2}} for i in range(6)),
        *({"type": "resize_image", "payload": {"image": f"img_{i}.png", "duration": 1.6}} for i in range(4)),
        {"type": "flaky", "payload": {"fail_rate": 0.5}},
        {"type": "flaky", "payload": {"fail_rate": 0.5}},
    ],
    # low-priority jobs first, then urgent ones that should jump the line
    "priority": [
        *({"type": "resize_image", "priority": 0, "payload": {"image": f"bulk_{i}.png", "duration": 1.0}} for i in range(8)),
        *({"type": "send_email", "priority": 10, "payload": {"to": f"vip{i}@college.edu", "duration": 0.8}} for i in range(3)),
    ],
    # delayed jobs: show up in 'scheduled' until their time comes
    "scheduled": [{"type": "send_email", "delay": d, "payload": {"to": f"reminder+{d}s@college.edu"}} for d in (3, 6, 9, 12)],
    # exponential backoff, then the dead-letter queue
    "dead_letter": [{"type": "always_fail", "max_attempts": 4, "payload": {"error": "card declined (fraud check)"}},
                    {"type": "flaky", "max_attempts": 3, "payload": {"fail_rate": 0.9}}],
    # worker dies mid-job; lease expires; another worker finishes it
    "crash": [{"type": "slow", "payload": {"duration": 4, "crash_on_attempt": 1}}],
    # the idempotency lesson: same crash, idempotent vs naive handler
    "idempotency": [
        {"type": "charge_order", "payload": {"order_id": "A-42", "amount": 499, "crash_on_attempt": 1}},
        {"type": "charge_order_naive", "payload": {"order_id": "N-42", "amount": 499, "crash_on_attempt": 1}},
    ],
    # slow worker that looks dead: stalls past its lease, someone else takes over
    "zombie": [{"type": "charge_order", "payload": {"order_id": "Z-7", "amount": 250, "stall_seconds": 8}}],
}


def _chain(q: JobQueue) -> list[int]:
    """Stretch goal: job dependencies. resize -> notify uploader -> notify admin."""
    a = q.enqueue("resize_image", {"image": "upload.png", "duration": 3})
    b = q.enqueue("send_email", {"to": "uploader@college.edu"}, depends_on=a["id"])
    c = q.enqueue("send_email", {"to": "admin@college.edu"}, depends_on=b["id"])
    return [a["id"], b["id"], c["id"]]


@app.get("/scenarios")
async def scenarios():
    return [*SCENARIOS, "dependencies"]


@app.post("/scenarios/{name}")
async def run_scenario(name: str, request: Request):
    q = Q(request)
    if name == "dependencies":
        return {"enqueued": await run_in_threadpool(_chain, q)}
    if name not in SCENARIOS:
        raise HTTPException(404)
    return {"enqueued": await run_in_threadpool(lambda: [q.enqueue(**{"payload": {}, **j})["id"] for j in SCENARIOS[name]])}


@app.post("/reset")
async def reset(request: Request):
    await run_in_threadpool(Q(request).purge)
    return {"ok": True}


@app.get("/health")
async def health():
    return {"ok": True}
