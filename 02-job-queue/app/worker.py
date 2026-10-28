"""Workers: claim a job, run its handler, report back. Repeat.

Workers run either as threads inside the API process (easy to show in the UI)
or as separate OS processes:

    python -m app.worker --id w-ext-1

A separate process is what the crash test kills for real.
"""
from __future__ import annotations

import argparse
import itertools
import json
import os
import random
import sys
import threading
import time
import traceback

from .handlers import HANDLERS, Ctx, LeaseLost, WorkerCrash
from .queue import JobQueue


class Worker(threading.Thread):
    def __init__(self, queue: JobQueue, worker_id: str, kind: str = "thread", poll: float = 0.2):
        super().__init__(daemon=True, name=worker_id)
        self.q = queue
        self.id = worker_id
        self.kind = kind
        self.poll = poll
        self.stop_requested = False   # graceful: finish current job, then exit
        self.crash_requested = False  # abrupt: die mid-job, leave the lease dangling
        self.state = "starting"
        self._last_seen = 0.0

    def _seen(self, status: str, job_id: int | None, processed: int = 0, force: bool = False) -> None:
        self.state = status
        if force or processed or time.time() - self._last_seen > 1.0:
            self.q.worker_seen(self.id, status, job_id, processed)
            self._last_seen = time.time()

    def run(self) -> None:
        self.q.register_worker(self.id, self.kind)
        self._seen("idle", None, force=True)
        while not self.stop_requested:
            if self.crash_requested:
                self._seen("crashed", None, force=True)
                return
            try:
                job = self.q.claim(self.id)
            except Exception:
                traceback.print_exc()
                time.sleep(self.poll)
                continue
            if job is None:
                self._seen("idle", None)
                time.sleep(self.poll)
                continue
            self._seen("busy", job["id"], force=True)
            if not self._run_job(job):
                return  # crashed
        self._seen("stopped", None, force=True)

    def _run_job(self, job: dict) -> bool:
        """Returns False if the worker crashed (thread must end)."""
        handler = HANDLERS.get(job["type"])
        try:
            if handler is None:
                raise ValueError(f"no handler for job type {job['type']!r}")
            result = handler(Ctx(self.q, job, self), json.loads(job["payload"]))
            if not self.q.complete(job["id"], self.id, result):
                print(f"[{self.id}] job {job['id']}: lease lost, result discarded", flush=True)
            self._seen("idle", None, processed=1)
        except WorkerCrash:
            # Do NOT touch the job: a crashed process can't tell anyone anything.
            self._seen("crashed", job["id"], force=True)
            return False
        except LeaseLost:
            # a heartbeat found someone else owns the job now: abandon it quietly
            self.q.store.tx(lambda con: self.q._event(con, job["id"], "lease_lost", self.id,
                                                      "heartbeat rejected; abandoning job"))
            self._seen("idle", None)
        except Exception as e:
            self.q.fail(job["id"], self.id, f"{type(e).__name__}: {e}")
            self._seen("idle", None, processed=1)
        return True


class WorkerPool:
    """The in-process workers the UI can add, stop and crash."""

    _ids = itertools.count(1)

    def __init__(self, queue: JobQueue):
        self.q = queue
        self.workers: dict[str, Worker] = {}

    def add(self, n: int = 1) -> list[str]:
        out = []
        for _ in range(n):
            w = Worker(self.q, f"worker-{next(self._ids)}")
            self.workers[w.id] = w
            w.start()
            out.append(w.id)
        return out

    def stop(self, worker_id: str) -> bool:
        w = self.workers.get(worker_id)
        if w:
            w.stop_requested = True
        return bool(w)

    def crash(self, worker_id: str | None = None) -> str | None:
        alive = [w for w in self.workers.values() if w.is_alive() and not w.crash_requested]
        if worker_id is None:
            busy = [w for w in alive if w.state == "busy"]
            pick = random.choice(busy or alive) if alive else None
        else:
            pick = self.workers.get(worker_id)
        if pick:
            pick.crash_requested = True
            return pick.id
        return None

    def forget_dead(self) -> None:
        for wid, w in list(self.workers.items()):
            if not w.is_alive():
                del self.workers[wid]
        self.q.store.tx(lambda con: con.execute(
            "DELETE FROM workers WHERE status IN ('crashed', 'stopped') OR last_seen < ?", (time.time() - 30,)))

    def shutdown(self) -> None:
        for w in self.workers.values():
            w.stop_requested = True


def main() -> None:
    p = argparse.ArgumentParser()
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    p.add_argument("--db", default=os.getenv("JQ_DB", os.path.join(here, "data", "jobqueue.db")))
    p.add_argument("--id", default=f"proc-{os.getpid()}")
    p.add_argument("--lease", type=float, default=float(os.getenv("JQ_LEASE", "5")))
    p.add_argument("--backoff", type=float, default=float(os.getenv("JQ_BACKOFF", "1")))
    a = p.parse_args()
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.winmm.timeBeginPeriod(1)
    w = Worker(JobQueue(a.db, lease_seconds=a.lease, base_backoff=a.backoff), a.id, kind="process")
    print(f"[{a.id}] worker process {os.getpid()} polling {a.db}", flush=True)
    w.run()


if __name__ == "__main__":
    main()
