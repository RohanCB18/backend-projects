"""Demo job types. Each handler gets a context (ctx) and the job's payload.

Every handler accepts these payload knobs so failures can be staged on demand:
  duration          how long the "work" takes (seconds)
  fail_rate         probability of raising an error (0..1)
  crash_on_attempt  on that attempt number, the worker dies mid-job (like kill -9)
  stall_seconds     on attempt 1, freeze WITHOUT renewing the lease (a GC pause,
                    a network hang): the "slow worker that looks dead"

The interesting pair is charge_order vs charge_order_naive (requirement 7).
"""
from __future__ import annotations

import random
import time


class WorkerCrash(BaseException):
    """Simulated hard crash. BaseException so no handler can accidentally catch
    it: the worker thread just stops, leaving its lease to expire, exactly like
    a process that was killed."""


class LeaseLost(Exception):
    """Our lease expired and someone else may own the job now: stop working."""


class Ctx:
    def __init__(self, queue, job: dict, worker):
        self.queue = queue
        self.job = job
        self.worker = worker
        self.attempt = job["attempts"]
        self._last_beat = time.time()

    def log(self, msg: str) -> None:
        self.queue.store.tx(lambda con: self.queue._event(con, self.job["id"], "log", self.worker.id, msg))

    def work(self, seconds: float) -> None:
        """Do `seconds` of pretend work while renewing the lease regularly."""
        end = time.time() + seconds
        while time.time() < end:
            if self.worker.crash_requested:
                raise WorkerCrash()
            time.sleep(min(0.05, max(0.0, end - time.time())))
            if time.time() - self._last_beat > self.queue.lease_seconds / 3:
                if not self.queue.heartbeat(self.job["id"], self.worker.id):
                    raise LeaseLost("lease lost during work")
                self._last_beat = time.time()

    def stall(self, seconds: float) -> None:
        """Freeze without heartbeats. If this outlasts the lease, another worker
        will legitimately pick the job up while we're still 'running' it."""
        self.log(f"stalling {seconds:g}s without heartbeats (lease is {self.queue.lease_seconds:g}s)")
        end = time.time() + seconds
        while time.time() < end:
            if self.worker.crash_requested:
                raise WorkerCrash()
            time.sleep(0.05)

    def maybe_misbehave(self, p: dict) -> None:
        if p.get("crash_on_attempt") == self.attempt:
            self.log("simulated crash: worker process dies right now")
            raise WorkerCrash()
        if p.get("stall_seconds") and self.attempt == 1:
            self.stall(float(p["stall_seconds"]))
        if random.random() < float(p.get("fail_rate", 0)):
            raise RuntimeError("downstream service returned 503")


def send_email(ctx: Ctx, p: dict) -> dict:
    ctx.work(float(p.get("duration", 1.0)) / 2)
    ctx.maybe_misbehave(p)
    ctx.work(float(p.get("duration", 1.0)) / 2)
    # keyed by job id: a retried job never sends the same email twice
    ctx.queue.store.tx(lambda con: con.execute(
        "INSERT INTO emails (recipient, job_id, attempt, ts, idempotency_key) VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT (idempotency_key) DO NOTHING",
        (p.get("to", "user@example.com"), ctx.job["id"], ctx.attempt, time.time(), f"email:{ctx.job['id']}")))
    return {"sent_to": p.get("to", "user@example.com")}


def resize_image(ctx: Ctx, p: dict) -> dict:
    ctx.work(float(p.get("duration", 1.5)) / 2)
    ctx.maybe_misbehave(p)
    ctx.work(float(p.get("duration", 1.5)) / 2)
    return {"thumbnail": f"{p.get('image', 'photo.jpg')}@256px"}


def flaky(ctx: Ctx, p: dict) -> dict:
    ctx.work(float(p.get("duration", 0.6)))
    ctx.maybe_misbehave({"fail_rate": 0.6, **p})
    return {"ok": True}


def always_fail(ctx: Ctx, p: dict) -> dict:
    ctx.work(float(p.get("duration", 0.4)))
    raise RuntimeError(p.get("error", "payment gateway rejected the request"))


def slow(ctx: Ctx, p: dict) -> dict:
    ctx.work(float(p.get("duration", 8.0)) / 2)
    ctx.maybe_misbehave(p)
    ctx.work(float(p.get("duration", 8.0)) / 2)
    return {"ok": True}


def _charge(ctx: Ctx, p: dict, idempotent: bool) -> dict:
    order = str(p.get("order_id", ctx.job["id"]))
    amount = float(p.get("amount", 42.0))
    ctx.work(float(p.get("duration", 1.0)) / 2)

    # Step 1: the side effect (moving money).
    key = f"charge:{order}" if idempotent else None
    inserted = ctx.queue.store.tx(lambda con: con.execute(
        "INSERT INTO charges (order_id, amount, job_id, attempt, worker, ts, idempotency_key) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (idempotency_key) DO NOTHING",
        (order, amount, ctx.job["id"], ctx.attempt, ctx.worker.id, time.time(), key)).rowcount)
    if inserted:
        ctx.log(f"charged order {order}: Rs {amount:g}")
    else:
        ctx.log(f"order {order} already charged (idempotency key {key}); skipping")

    # Step 2: the dangerous window. Crash or stall here and the job will run again.
    ctx.maybe_misbehave(p)

    # Step 3: the rest of the job.
    ctx.work(float(p.get("duration", 1.0)) / 2)
    return {"order_id": order, "charged_now": bool(inserted)}


def charge_order(ctx: Ctx, p: dict) -> dict:
    return _charge(ctx, p, idempotent=True)


def charge_order_naive(ctx: Ctx, p: dict) -> dict:
    return _charge(ctx, p, idempotent=False)


HANDLERS = {
    "send_email": send_email,
    "resize_image": resize_image,
    "flaky": flaky,
    "always_fail": always_fail,
    "slow": slow,
    "charge_order": charge_order,
    "charge_order_naive": charge_order_naive,
}
