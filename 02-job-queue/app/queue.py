"""The queue itself. Every state change is one Postgres transaction.

Job lifecycle:

    queued --claim--> running --complete--> done
      ^                  |
      |  fail (attempts < max): run_at = now + backoff
      +------------------+
      |                  |  fail (attempts == max)
      |  lease expired   +---------------------------> dead  (the dead-letter queue)
      +--(worker died)---+                                |
      ^                                                   |
      +-----------------------replay----------------------+

Key ideas:
  * Durability (R1): jobs are rows in Postgres, not items in a Python list.
  * Priorities + delays (R2): claim picks the highest priority job whose
    run_at <= now.
  * Lease / visibility timeout (R5): claiming sets lease_until. A worker that
    dies stops renewing it; once it expires, the job becomes claimable again.
  * Fencing: complete()/fail() only count if the caller STILL holds the lease.
    A slow "zombie" worker that lost its lease can't overwrite the result of
    the worker that took over.
"""
from __future__ import annotations

import json
import random
import time

from .db import Store

DEFAULT_DSN = "postgresql://backend:backend@127.0.0.1:15432/jobqueue"  # docker-compose.yml

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id            BIGSERIAL PRIMARY KEY,
    type          TEXT    NOT NULL,
    payload       TEXT    NOT NULL DEFAULT '{}',
    priority      INTEGER NOT NULL DEFAULT 0,       -- higher runs first
    run_at        DOUBLE PRECISION NOT NULL,        -- not before this time (delays + backoff)
    status        TEXT    NOT NULL DEFAULT 'queued',-- queued | running | done | dead
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 4,
    lease_owner   TEXT,
    lease_until   DOUBLE PRECISION,
    depends_on    BIGINT REFERENCES jobs(id),       -- stretch goal: run only after that job is done
    last_error    TEXT,
    result        TEXT,
    created_at    DOUBLE PRECISION NOT NULL,
    updated_at    DOUBLE PRECISION NOT NULL,
    finished_at   DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS jobs_claim ON jobs (status, priority DESC, run_at, id);
CREATE INDEX IF NOT EXISTS jobs_lease ON jobs (status, lease_until);

CREATE TABLE IF NOT EXISTS job_events (
    id BIGSERIAL PRIMARY KEY, job_id BIGINT NOT NULL, ts DOUBLE PRECISION NOT NULL,
    kind TEXT NOT NULL, worker TEXT, detail TEXT
);
CREATE INDEX IF NOT EXISTS job_events_job ON job_events (job_id, id);
CREATE INDEX IF NOT EXISTS job_events_ts ON job_events (ts);

CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
    started_at DOUBLE PRECISION NOT NULL, last_seen DOUBLE PRECISION NOT NULL,
    current_job BIGINT, processed INTEGER NOT NULL DEFAULT 0
);

-- Side effects of the demo jobs live here so we can count them (R7).
CREATE TABLE IF NOT EXISTS charges (
    id BIGSERIAL PRIMARY KEY, order_id TEXT NOT NULL, amount DOUBLE PRECISION NOT NULL,
    job_id BIGINT NOT NULL, attempt INTEGER NOT NULL, worker TEXT, ts DOUBLE PRECISION NOT NULL,
    idempotency_key TEXT UNIQUE          -- NULL for the naive handler, so duplicates are possible there
);
CREATE TABLE IF NOT EXISTS emails (
    id BIGSERIAL PRIMARY KEY, recipient TEXT NOT NULL, job_id BIGINT NOT NULL,
    attempt INTEGER NOT NULL, ts DOUBLE PRECISION NOT NULL, idempotency_key TEXT UNIQUE
);
"""


class JobQueue:
    def __init__(self, dsn: str, lease_seconds: float = 5.0, base_backoff: float = 1.0,
                 max_backoff: float = 30.0, jitter: float = 0.2, schema: str = "public", pool_size: int = 20):
        self.store = Store(dsn, SCHEMA, schema=schema, max_size=pool_size)
        self.lease_seconds = lease_seconds
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self.jitter = jitter

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _event(con, job_id: int, kind: str, worker: str | None = None, detail: str | None = None) -> None:
        con.execute("INSERT INTO job_events (job_id, ts, kind, worker, detail) VALUES (%s, %s, %s, %s, %s)",
                    (job_id, time.time(), kind, worker, detail))

    def backoff(self, attempts: int) -> float:
        """1s, 2s, 4s, 8s ... capped, times a random factor in [1-j, 1+j].
        The jitter stops a crowd of failed jobs from retrying in lockstep
        (a "retry storm") against a downstream service that just recovered."""
        base = min(self.max_backoff, self.base_backoff * 2 ** (attempts - 1))
        return base * random.uniform(1 - self.jitter, 1 + self.jitter)

    # ------------------------------------------------------------------ producer
    def enqueue(self, type: str, payload: dict | None = None, priority: int = 0, delay: float = 0.0,
                max_attempts: int = 4, depends_on: int | None = None) -> dict:
        now = time.time()

        def fn(con):
            row = con.execute(
                "INSERT INTO jobs (type, payload, priority, run_at, max_attempts, depends_on, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING *",
                (type, json.dumps(payload or {}), priority, now + delay, max_attempts, depends_on, now, now),
            ).fetchone()
            detail = f"priority {priority}" + (f", delayed {delay:g}s" if delay else "") + (
                f", after #{depends_on}" if depends_on else "")
            self._event(con, row["id"], "enqueued", None, detail)
            return dict(row)

        return self.store.tx(fn)

    # ------------------------------------------------------------------ worker side
    def reap_expired_leases(self, con, now: float) -> None:
        """A running job whose lease ran out belongs to a worker that died (or
        froze). Put it back in the queue, or bury it if it's out of attempts."""
        expired = con.execute(
            "SELECT id, attempts, max_attempts, lease_owner FROM jobs WHERE status = 'running' AND lease_until < %s "
            "FOR UPDATE SKIP LOCKED",  # another worker reaping the same job? it skips it, we take it
            (now,),
        ).fetchall()
        for j in expired:
            if j["attempts"] >= j["max_attempts"]:
                con.execute("UPDATE jobs SET status = 'dead', lease_owner = NULL, lease_until = NULL, "
                            "last_error = 'lease expired on final attempt', updated_at = %s, finished_at = %s "
                            "WHERE id = %s", (now, now, j["id"]))
                self._event(con, j["id"], "dead", j["lease_owner"], "lease expired on final attempt")
            else:
                con.execute("UPDATE jobs SET status = 'queued', run_at = %s, lease_owner = NULL, lease_until = NULL, "
                            "last_error = 'lease expired (worker presumed dead)', updated_at = %s WHERE id = %s",
                            (now, now, j["id"]))
                self._event(con, j["id"], "lease_expired", j["lease_owner"], "back to queue")

    def claim(self, worker_id: str) -> dict | None:
        """Atomically take the best ready job.

        FOR UPDATE row-locks the job we pick; SKIP LOCKED makes every other
        worker running this at the same moment skip rows that are already
        locked and take the next one instead of waiting. So N workers claim N
        different jobs in parallel, and no job is ever handed out twice."""
        now = time.time()

        def fn(con):
            self.reap_expired_leases(con, now)
            row = con.execute(
                """
                UPDATE jobs
                   SET status = 'running', lease_owner = %s, lease_until = %s,
                       attempts = attempts + 1, updated_at = %s
                 WHERE id = (
                     SELECT j.id FROM jobs j
                      WHERE j.status = 'queued' AND j.run_at <= %s
                        AND (j.depends_on IS NULL OR EXISTS (
                             SELECT 1 FROM jobs d WHERE d.id = j.depends_on AND d.status = 'done'))
                      ORDER BY j.priority DESC, j.run_at, j.id
                      LIMIT 1
                      FOR UPDATE SKIP LOCKED)
                RETURNING *
                """,
                (worker_id, now + self.lease_seconds, now, now),
            ).fetchone()
            if row is None:
                return None
            self._event(con, row["id"], "claimed", worker_id, f"attempt {row['attempts']}/{row['max_attempts']}")
            return dict(row)

        return self.store.tx(fn)

    def heartbeat(self, job_id: int, worker_id: str) -> bool:
        """Extend the lease. Returns False if we no longer own the job."""
        now = time.time()
        n = self.store.tx(lambda con: con.execute(
            "UPDATE jobs SET lease_until = %s, updated_at = %s WHERE id = %s AND lease_owner = %s AND status = 'running'",
            (now + self.lease_seconds, now, job_id, worker_id)).rowcount)
        return n == 1

    def complete(self, job_id: int, worker_id: str, result: dict | None = None) -> bool:
        now = time.time()

        def fn(con):
            cur = con.execute(
                "UPDATE jobs SET status = 'done', result = %s, lease_owner = NULL, lease_until = NULL, "
                "updated_at = %s, finished_at = %s WHERE id = %s AND lease_owner = %s AND status = 'running'",
                (json.dumps(result or {}), now, now, job_id, worker_id),
            )
            if cur.rowcount == 1:
                self._event(con, job_id, "done", worker_id, None)
                return True
            # fencing: somebody else owns this job now, our result is discarded
            self._event(con, job_id, "stale_complete_rejected", worker_id, "lease lost before finishing")
            return False

        return self.store.tx(fn)

    def fail(self, job_id: int, worker_id: str, error: str) -> str:
        """Returns 'retry', 'dead', or 'stale' (we no longer held the lease)."""
        now = time.time()

        def fn(con):
            job = con.execute("SELECT attempts, max_attempts FROM jobs WHERE id = %s AND lease_owner = %s "
                              "AND status = 'running' FOR UPDATE", (job_id, worker_id)).fetchone()
            if job is None:
                self._event(con, job_id, "stale_fail_rejected", worker_id, error)
                return "stale"
            if job["attempts"] >= job["max_attempts"]:
                con.execute("UPDATE jobs SET status = 'dead', last_error = %s, lease_owner = NULL, lease_until = NULL, "
                            "updated_at = %s, finished_at = %s WHERE id = %s", (error, now, now, job_id))
                self._event(con, job_id, "dead", worker_id, error)
                return "dead"
            delay = self.backoff(job["attempts"])
            con.execute("UPDATE jobs SET status = 'queued', run_at = %s, last_error = %s, lease_owner = NULL, "
                        "lease_until = NULL, updated_at = %s WHERE id = %s", (now + delay, error, now, job_id))
            self._event(con, job_id, "retry_scheduled", worker_id, f"{error} -> retry in {delay:.1f}s")
            return "retry"

        return self.store.tx(fn)

    # ------------------------------------------------------------------ dead-letter queue (R4)
    def dead_letters(self, limit: int = 100) -> list[dict]:
        return self.store.query(
            "SELECT * FROM jobs WHERE status = 'dead' ORDER BY finished_at DESC LIMIT %s", (limit,))

    def replay(self, job_id: int) -> bool:
        """Give a dead job a fresh set of attempts."""
        now = time.time()

        def fn(con):
            cur = con.execute("UPDATE jobs SET status = 'queued', attempts = 0, run_at = %s, last_error = NULL, "
                              "finished_at = NULL, updated_at = %s WHERE id = %s AND status = 'dead'", (now, now, job_id))
            if cur.rowcount:
                self._event(con, job_id, "replayed", None, "from dead-letter queue")
            return cur.rowcount == 1

        return self.store.tx(fn)

    def replay_all(self) -> int:
        ids = [j["id"] for j in self.dead_letters(10_000)]
        return sum(self.replay(i) for i in ids)

    # ------------------------------------------------------------------ inspection (R6)
    def stats(self) -> dict:
        now = time.time()
        q = self.store.query
        counts = {r["status"]: r["n"] for r in q("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status")}
        ready = q("""SELECT COUNT(*) AS n FROM jobs j WHERE status = 'queued' AND run_at <= %s
                     AND (depends_on IS NULL OR EXISTS (SELECT 1 FROM jobs d WHERE d.id = j.depends_on
                          AND d.status = 'done'))""", (now,))[0]["n"]
        scheduled = q("SELECT COUNT(*) AS n FROM jobs WHERE status = 'queued' AND run_at > %s", (now,))[0]["n"]
        in_flight = q("SELECT id, type, priority, attempts, max_attempts, lease_owner, lease_until FROM jobs "
                      "WHERE status = 'running' ORDER BY lease_until")
        for j in in_flight:
            j["lease_remaining"] = j["lease_until"] - now
        per_sec = q("SELECT floor(ts)::bigint AS sec, kind, COUNT(*) AS n FROM job_events WHERE ts > %s "
                    "AND kind IN ('done', 'retry_scheduled', 'dead', 'enqueued') GROUP BY sec, kind",
                    (now - 60,))
        return {
            "now": now,
            "depth": counts.get("queued", 0),
            "ready": ready,
            "scheduled": scheduled,
            "waiting_on_dependency": counts.get("queued", 0) - ready - scheduled,
            "running": counts.get("running", 0),
            "done": counts.get("done", 0),
            "dead": counts.get("dead", 0),
            "in_flight": in_flight,
            "throughput": per_sec,
            "lease_seconds": self.lease_seconds,
        }

    def jobs(self, status: str | None = None, limit: int = 200) -> list[dict]:
        now = time.time()
        if status:
            rows = self.store.query("SELECT * FROM jobs WHERE status = %s ORDER BY id DESC LIMIT %s", (status, limit))
        else:
            rows = self.store.query("SELECT * FROM jobs ORDER BY id DESC LIMIT %s", (limit,))
        for r in rows:
            r["payload"] = json.loads(r["payload"])
            r["result"] = json.loads(r["result"]) if r["result"] else None
            r["runs_in"] = max(0.0, r["run_at"] - now) if r["status"] == "queued" else 0.0
        return rows

    def job(self, job_id: int) -> dict | None:
        rows = self.store.query("SELECT * FROM jobs WHERE id = %s", (job_id,))
        if not rows:
            return None
        j = rows[0]
        j["payload"] = json.loads(j["payload"])
        j["events"] = self.store.query("SELECT * FROM job_events WHERE job_id = %s ORDER BY id", (job_id,))
        return j

    def events(self, since_id: int = 0, limit: int = 200) -> list[dict]:
        return self.store.query("SELECT * FROM job_events WHERE id > %s ORDER BY id DESC LIMIT %s", (since_id, limit))

    # ------------------------------------------------------------------ worker registry (for the UI)
    def register_worker(self, worker_id: str, kind: str) -> None:
        now = time.time()
        self.store.tx(lambda con: con.execute(
            "INSERT INTO workers (id, kind, status, started_at, last_seen) VALUES (%s, %s, 'idle', %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET status = 'idle', last_seen = excluded.last_seen",
            (worker_id, kind, now, now)))

    def worker_seen(self, worker_id: str, status: str, current_job: int | None, processed_delta: int = 0) -> None:
        self.store.tx(lambda con: con.execute(
            "UPDATE workers SET status = %s, current_job = %s, last_seen = %s, processed = processed + %s WHERE id = %s",
            (status, current_job, time.time(), processed_delta, worker_id)))

    def workers(self) -> list[dict]:
        now = time.time()
        rows = self.store.query("SELECT * FROM workers ORDER BY started_at")
        for w in rows:
            w["seen_ago"] = now - w["last_seen"]
        return rows

    # ------------------------------------------------------------------ side-effect ledgers (R7)
    def side_effects(self) -> dict:
        q = self.store.query
        return {
            "charges": q("SELECT order_id, COUNT(*) AS times_charged, SUM(amount) AS total, "
                         "bool_or(idempotency_key IS NOT NULL) AS idempotent FROM charges GROUP BY order_id "
                         "ORDER BY MAX(id) DESC LIMIT 50"),
            "emails": q("SELECT recipient, COUNT(*) AS times_sent FROM emails GROUP BY recipient "
                        "ORDER BY MAX(id) DESC LIMIT 50"),
        }

    def purge(self) -> None:
        """Demo helper: wipe everything."""
        def fn(con):
            for t in ("jobs", "job_events", "charges", "emails"):
                con.execute(f"DELETE FROM {t}")
            con.execute("DELETE FROM workers WHERE kind != 'thread'")
        self.store.tx(fn)
