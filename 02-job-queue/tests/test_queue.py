"""One test (or more) per requirement R1..R7, plus the stretch goal.
Short leases and backoffs keep the suite fast; the logic is identical."""
import multiprocessing as mp
import os
import subprocess
import sys
import threading
import time

import uuid
from typing import NamedTuple

import httpx
import psycopg
import pytest

from app.queue import DEFAULT_DSN, JobQueue
from app.worker import Worker, WorkerPool

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DSN = os.getenv("JQ_DSN", DEFAULT_DSN)


class Db(NamedTuple):
    dsn: str
    schema: str


@pytest.fixture
def db():
    """A fresh Postgres schema per test: same tables, nothing shared."""
    d = Db(DSN, f"test_{uuid.uuid4().hex[:10]}")
    yield d
    # stop any worker threads the test started BEFORE their tables disappear
    workers = [t for t in threading.enumerate() if isinstance(t, Worker)]
    for w in workers:
        w.stop_requested = True
    for w in workers:
        w.join(timeout=15)
    with psycopg.connect(DSN, autocommit=True) as con:
        con.execute(f'DROP SCHEMA IF EXISTS "{d.schema}" CASCADE')


@pytest.fixture
def q(db):
    return JobQueue(db.dsn, lease_seconds=0.5, base_backoff=0.1, schema=db.schema)


def wait_until(pred, timeout=15.0, step=0.05):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(step)
    return False


# ---- R1: durable ------------------------------------------------------------
def test_r1_jobs_survive_a_restart(db, q):
    ids = [q.enqueue("send_email")["id"] for _ in range(3)]
    del q  # "crash": the process's memory is gone
    fresh = JobQueue(db.dsn, schema=db.schema)
    assert [j["id"] for j in fresh.jobs(status="queued")] == sorted(ids, reverse=True)
    assert fresh.claim("w")["id"] == ids[0]


# ---- R2: priorities and delays ----------------------------------------------
def test_r2_highest_priority_first_then_fifo(q):
    low = q.enqueue("send_email", priority=0)["id"]
    mid1 = q.enqueue("send_email", priority=5)["id"]
    high = q.enqueue("send_email", priority=10)["id"]
    mid2 = q.enqueue("send_email", priority=5)["id"]
    assert [q.claim("w")["id"] for _ in range(4)] == [high, mid1, mid2, low]


def test_r2_delayed_job_is_invisible_until_due(q):
    j = q.enqueue("send_email", delay=0.4)
    assert q.claim("w") is None
    assert q.stats()["scheduled"] == 1
    time.sleep(0.45)
    assert q.claim("w")["id"] == j["id"]


# ---- R3: retries with exponential backoff -------------------------------------
def test_r3_backoff_doubles_with_jitter_and_is_capped(db):
    q = JobQueue(db.dsn, schema=db.schema, base_backoff=1.0, max_backoff=8.0, jitter=0.2)
    for attempt, base in [(1, 1), (2, 2), (3, 4), (4, 8), (5, 8), (9, 8)]:
        samples = [q.backoff(attempt) for _ in range(200)]
        assert all(base * 0.8 <= s <= base * 1.2 for s in samples)
        assert len({round(s, 6) for s in samples}) > 100  # jitter really varies


def test_r3_failed_job_is_rescheduled_with_growing_delay(q):
    j = q.enqueue("flaky", max_attempts=5)
    gaps = []
    for _ in range(3):
        assert wait_until(lambda: (c := q.claim("w")) is not None and gaps.append(c) is None, timeout=3)
        claimed = gaps.pop()
        t = time.time()
        assert q.fail(claimed["id"], "w", "boom") == "retry"
        run_at = q.job(j["id"])["run_at"]
        gaps.append(run_at - t)
    # 0.1s, 0.2s, 0.4s (+/-20%)
    for gap, base in zip(gaps, (0.1, 0.2, 0.4)):
        assert base * 0.75 <= gap <= base * 1.25, gaps


# ---- R4: dead-letter queue -----------------------------------------------------
def test_r4_exhausted_job_goes_to_dlq_and_can_be_replayed(q):
    j = q.enqueue("always_fail", max_attempts=2)
    outcomes = []
    for _ in range(2):
        assert wait_until(lambda: (c := q.claim("w")) and outcomes.append(c) is None, timeout=3)
        outcomes[-1] = q.fail(j["id"], "w", "card declined")
    assert outcomes == ["retry", "dead"]
    dlq = q.dead_letters()
    assert [d["id"] for d in dlq] == [j["id"]] and dlq[0]["last_error"] == "card declined"

    assert q.replay(j["id"])
    again = q.claim("w")
    assert again["id"] == j["id"] and again["attempts"] == 1  # fresh attempts
    assert q.complete(j["id"], "w")
    assert q.dead_letters() == []


# ---- R5: never processed by two workers at once --------------------------------
def test_r5_lease_hides_job_then_expiry_hands_it_over_and_fences_the_old_owner(q):
    j = q.enqueue("send_email")
    assert q.claim("A")["id"] == j["id"]
    assert q.claim("B") is None                       # invisible while A's lease is valid
    time.sleep(0.6)                                   # A "dies"; lease (0.5s) runs out
    taken = q.claim("B")
    assert taken["id"] == j["id"] and taken["attempts"] == 2
    assert q.complete(j["id"], "A") is False          # zombie A is fenced off
    assert q.complete(j["id"], "B") is True
    kinds = [e["kind"] for e in q.job(j["id"])["events"]]
    assert "lease_expired" in kinds and "stale_complete_rejected" in kinds
    assert kinds.count("done") == 1


def test_r5_heartbeat_keeps_a_long_job_owned(q):
    j = q.enqueue("slow")
    q.claim("A")
    for _ in range(4):                                # 0.8s total > 0.5s lease
        time.sleep(0.2)
        assert q.heartbeat(j["id"], "A")
    assert q.claim("B") is None


def test_r5_concurrent_threads_never_claim_the_same_job(db):
    # long lease: we claim without completing, and must not let leases expire mid-test
    q = JobQueue(db.dsn, schema=db.schema, lease_seconds=60)
    n = 300
    for _ in range(n):
        q.enqueue("send_email")
    claimed: list[int] = []
    lock = threading.Lock()

    def worker(name):
        while (job := q.claim(name)) is not None:
            with lock:
                claimed.append(job["id"])

    threads = [threading.Thread(target=worker, args=(f"t{i}",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(claimed) == n and len(set(claimed)) == n


def _claim_all(db: Db, name: str, start_at: float) -> list[int]:
    q = JobQueue(db.dsn, schema=db.schema, lease_seconds=60)
    time.sleep(max(0.0, start_at - time.time()))  # all processes start claiming together
    out = []
    while (job := q.claim(name)) is not None:
        out.append(job["id"])
    return out


def test_r5_concurrent_processes_never_claim_the_same_job(db, q):
    n = 400
    for _ in range(n):
        q.enqueue("send_email")
    with mp.get_context("spawn").Pool(4) as pool:
        start_at = time.time() + 3
        parts = pool.starmap(_claim_all, [(db, f"p{i}", start_at) for i in range(4)])
    flat = [i for p in parts for i in p]
    assert len(flat) == n and len(set(flat)) == n
    assert sum(1 for p in parts if p) >= 2, "processes really competed for the same rows"


def test_r5_worker_process_killed_mid_job_is_recovered(db):
    """The real thing: a separate OS process holding the job is killed."""
    q = JobQueue(db.dsn, schema=db.schema, lease_seconds=1.0, base_backoff=0.1)
    j = q.enqueue("slow", {"duration": 2.0})
    proc = subprocess.Popen([sys.executable, "-m", "app.worker", "--dsn", db.dsn, "--schema", db.schema,
                             "--id", "victim", "--lease", "1"],
                            cwd=ROOT)
    try:
        assert wait_until(lambda: q.job(j["id"])["lease_owner"] == "victim", timeout=20)
        time.sleep(0.3)
        proc.kill()                                   # no cleanup code runs
        proc.wait()
    finally:
        if proc.poll() is None:
            proc.kill()
    assert q.job(j["id"])["status"] == "running"      # still "owned" by a dead process

    rescuer = Worker(q, "rescuer")
    rescuer.start()
    assert wait_until(lambda: q.job(j["id"])["status"] == "done", timeout=15)
    rescuer.stop_requested = True
    job = q.job(j["id"])
    assert job["attempts"] == 2
    assert [e["worker"] for e in job["events"] if e["kind"] == "claimed"] == ["victim", "rescuer"]


# ---- R6: inspection ------------------------------------------------------------
async def test_r6_stats_dlq_and_inflight_are_exposed_over_http(db):
    from app.main import app
    q = JobQueue(db.dsn, schema=db.schema, lease_seconds=30)
    app.state.q, app.state.pool, app.state.procs = q, WorkerPool(q), {}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        for _ in range(3):
            assert (await c.post("/jobs", json={"type": "send_email", "priority": 1})).status_code == 200
        await c.post("/jobs", json={"type": "send_email", "delay": 60})
        bad = q.enqueue("always_fail", max_attempts=1)
        q.claim("w1")
        stats = (await c.get("/stats")).json()
        assert stats["running"] == 1 and stats["ready"] == 3 and stats["scheduled"] == 1
        assert stats["in_flight"][0]["lease_owner"] == "w1"
        q.store.tx(lambda con: con.execute("UPDATE jobs SET status='dead' WHERE id=%s", (bad["id"],)))
        assert [d["id"] for d in (await c.get("/dlq")).json()] == [bad["id"]]
        assert (await c.post(f"/dlq/{bad['id']}/replay")).status_code == 200


# ---- R7: idempotency -------------------------------------------------------------
def test_r7_rerun_after_crash_charges_once_only_when_idempotent(q):
    q.enqueue("charge_order", {"order_id": "A-1", "amount": 100, "crash_on_attempt": 1, "duration": 0.2})
    q.enqueue("charge_order_naive", {"order_id": "N-1", "amount": 100, "crash_on_attempt": 1, "duration": 0.2})
    w1, w2 = Worker(q, "w1"), Worker(q, "w2")
    w1.start(), w2.start()
    # both workers crash right after charging; their leases expire...
    assert wait_until(lambda: not w1.is_alive() and not w2.is_alive(), timeout=10)
    pool = [Worker(q, "rescuer-1"), Worker(q, "rescuer-2")]
    for w in pool:
        w.start()
    assert wait_until(lambda: q.stats()["done"] == 2, timeout=15)
    for w in pool:
        w.stop_requested = True
    charges = {c["order_id"]: c["times_charged"] for c in q.side_effects()["charges"]}
    assert charges == {"A-1": 1, "N-1": 2}   # the naive job charged the customer twice


def test_r7_zombie_worker_that_stalls_past_its_lease_cannot_double_charge(q):
    j = q.enqueue("charge_order", {"order_id": "Z-1", "amount": 50, "stall_seconds": 1.5, "duration": 0.2})
    zombie = Worker(q, "zombie")
    zombie.start()
    assert wait_until(lambda: q.job(j["id"])["lease_owner"] == "zombie", timeout=5)
    time.sleep(0.2)
    helper = Worker(q, "helper")
    helper.start()
    assert wait_until(lambda: q.job(j["id"])["status"] == "done", timeout=10)
    # the zombie wakes up and is fenced off: either its next heartbeat or its
    # final complete() is rejected, so it can't overwrite the helper's result
    fenced = {"lease_lost", "stale_complete_rejected"}
    assert wait_until(lambda: any(e["kind"] in fenced and e["worker"] == "zombie"
                                  for e in q.job(j["id"])["events"]), timeout=10)
    zombie.stop_requested = helper.stop_requested = True
    assert q.side_effects()["charges"][0]["times_charged"] == 1
    assert q.job(j["id"])["result"] is not None


# ---- stretch: dependencies ---------------------------------------------------------
def test_stretch_dependent_job_waits_for_its_parent(q):
    a = q.enqueue("resize_image")
    b = q.enqueue("send_email", depends_on=a["id"], priority=100)
    first = q.claim("w")
    assert first["id"] == a["id"]           # b has higher priority but must wait
    assert q.claim("w") is None
    q.complete(a["id"], "w")
    assert q.claim("w")["id"] == b["id"]
