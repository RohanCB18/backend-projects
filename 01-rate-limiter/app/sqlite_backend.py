"""Default storage backend: one SQLite file (in WAL mode).

The whole decision (load config -> read bucket -> refill -> check -> write) runs
inside ONE `BEGIN IMMEDIATE` transaction. IMMEDIATE takes SQLite's write lock up
front, so two requests for the same key, even from different OS processes
sharing the file, can never both read "1 token left". That is requirement 4.

Inside the server, every call runs on one dedicated database thread, so
decisions simply queue up in order. (A threading.Lock does the same for any
other threads using the object directly, as the tests do.) Several server
processes can share the file too: SQLite's file lock keeps them correct (a test
proves it with 4 processes), but under heavy load they spend time in SQLite's
sleep-and-retry busy handler, so one process is the fast default.

Durability: WAL + synchronous=NORMAL survives a crash or kill of the service
(requirement 3). Only a power cut could lose the last few milliseconds, which is
an acceptable trade-off for a rate limiter.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .core import DEFAULT_CONFIG, Decision

SCHEMA = """
CREATE TABLE IF NOT EXISTS client_config (client TEXT PRIMARY KEY, config TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bucket (client TEXT PRIMARY KEY, tokens REAL NOT NULL, ts REAL NOT NULL);
CREATE TABLE IF NOT EXISTS window_log (client TEXT NOT NULL, ts REAL NOT NULL);
CREATE INDEX IF NOT EXISTS window_log_idx ON window_log (client, ts);
CREATE TABLE IF NOT EXISTS stats (
    sec INTEGER NOT NULL, client TEXT NOT NULL,
    allowed INTEGER NOT NULL DEFAULT 0, denied INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (sec, client)
);
CREATE TABLE IF NOT EXISTS totals (
    client TEXT PRIMARY KEY, allowed INTEGER NOT NULL DEFAULT 0, denied INTEGER NOT NULL DEFAULT 0
);
"""


class SQLiteLimiter:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._local = threading.local()
        self._lock = threading.Lock()
        self._db_thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sqlite")
        con = self._con()
        con.executescript(SCHEMA)

    def _con(self) -> sqlite3.Connection:
        # one connection per thread: sqlite3 connections must not be shared across threads
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA busy_timeout=30000")
            self._local.con = con
        return con

    def _tx(self, fn):
        """Run fn(con) inside one write transaction."""
        con = self._con()
        with self._lock:
            con.execute("BEGIN IMMEDIATE")
            try:
                out = fn(con)
                con.execute("COMMIT")
                return out
            except BaseException:
                con.execute("ROLLBACK")
                raise

    # ---- the decision (requirement 1) --------------------------------------
    def check_sync(self, client: str, cost: int = 1) -> Decision:
        return self._tx(lambda con: self._decide(con, client, cost))

    def _decide(self, con: sqlite3.Connection, client: str, cost: int) -> Decision:
        cfg = self._config(con, client)
        now = time.time()  # read *after* taking the lock, so decisions are in time order

        if cfg["mode"] == "sliding_window":
            # Sliding window log: one row per allowed request. Forget rows older
            # than the window, count the rest. No 2x burst at window boundaries.
            limit, window = int(cfg["limit"]), float(cfg["window"])
            con.execute("DELETE FROM window_log WHERE client = ? AND ts <= ?", (client, now - window))
            count, oldest = con.execute(
                "SELECT COUNT(*), MIN(ts) FROM window_log WHERE client = ?", (client,)
            ).fetchone()
            allowed = count + cost <= limit
            if allowed:
                con.executemany("INSERT INTO window_log (client, ts) VALUES (?, ?)", [(client, now)] * cost)
                count += cost
                oldest = oldest if oldest is not None else now
            reset = max(0.0, oldest + window - now) if oldest is not None else 0.0
            d = Decision(allowed, limit, limit - count, 0.0 if allowed else reset, reset, cfg["mode"], now)
        else:
            # Token bucket with lazy refill: no timers, just "how long since we last looked?"
            rate, burst = float(cfg["rate"]), int(cfg["burst"])
            row = con.execute("SELECT tokens, ts FROM bucket WHERE client = ?", (client,)).fetchone()
            tokens, ts = row if row else (float(burst), now)  # new client: full bucket
            tokens = min(burst, tokens + max(0.0, now - ts) * rate)
            allowed = tokens >= cost
            if allowed:
                tokens -= cost
            con.execute(
                "INSERT INTO bucket (client, tokens, ts) VALUES (?, ?, ?) "
                "ON CONFLICT (client) DO UPDATE SET tokens = excluded.tokens, ts = excluded.ts",
                (client, tokens, now),
            )
            retry = 0.0 if allowed else (cost - tokens) / rate
            d = Decision(allowed, burst, tokens, retry, (burst - tokens) / rate, cfg["mode"], now)

        a, n = (1, 0) if d.allowed else (0, 1)
        con.execute(
            "INSERT INTO stats (sec, client, allowed, denied) VALUES (?, ?, ?, ?) ON CONFLICT (sec, client) "
            "DO UPDATE SET allowed = allowed + excluded.allowed, denied = denied + excluded.denied",
            (int(now), client, a, n),
        )
        con.execute(
            "INSERT INTO totals (client, allowed, denied) VALUES (?, ?, ?) ON CONFLICT (client) "
            "DO UPDATE SET allowed = allowed + excluded.allowed, denied = denied + excluded.denied",
            (client, a, n),
        )
        return d

    @staticmethod
    def _config(con: sqlite3.Connection, client: str) -> dict:
        row = con.execute("SELECT config FROM client_config WHERE client = ?", (client,)).fetchone()
        return {**DEFAULT_CONFIG, **json.loads(row[0])} if row else dict(DEFAULT_CONFIG)

    # ---- per-client configuration (requirements 2 and 5) -------------------
    def set_config_sync(self, client: str, cfg: dict) -> dict:
        merged = {**DEFAULT_CONFIG, **cfg}

        def fn(con):
            con.execute(
                "INSERT INTO client_config (client, config) VALUES (?, ?) "
                "ON CONFLICT (client) DO UPDATE SET config = excluded.config",
                (client, json.dumps(merged)),
            )
            self._reset(con, client)  # new limits start from a clean bucket

        self._tx(fn)
        return merged

    def delete_config_sync(self, client: str) -> None:
        def fn(con):
            con.execute("DELETE FROM client_config WHERE client = ?", (client,))
            self._reset(con, client)

        self._tx(fn)

    def reset_sync(self, client: str) -> None:
        self._tx(lambda con: self._reset(con, client))

    @staticmethod
    def _reset(con, client: str) -> None:
        con.execute("DELETE FROM bucket WHERE client = ?", (client,))
        con.execute("DELETE FROM window_log WHERE client = ?", (client,))

    def list_configs_sync(self) -> dict[str, dict]:
        rows = self._con().execute("SELECT client, config FROM client_config ORDER BY client").fetchall()
        return {c: {**DEFAULT_CONFIG, **json.loads(v)} for c, v in rows}

    def peek_sync(self, client: str) -> dict:
        """Current bucket/window state without consuming anything (for the UI)."""
        con = self._con()
        cfg = self._config(con, client)
        now = time.time()
        if cfg["mode"] == "sliding_window":
            (used,) = con.execute(
                "SELECT COUNT(*) FROM window_log WHERE client = ? AND ts > ?", (client, now - float(cfg["window"]))
            ).fetchone()
            return {"mode": cfg["mode"], "available": int(cfg["limit"]) - used, "capacity": int(cfg["limit"])}
        row = con.execute("SELECT tokens, ts FROM bucket WHERE client = ?", (client,)).fetchone()
        tokens = float(cfg["burst"]) if not row else min(float(cfg["burst"]), row[0] + (now - row[1]) * float(cfg["rate"]))
        return {"mode": cfg["mode"], "available": tokens, "capacity": int(cfg["burst"])}

    def stats_sync(self, seconds: int = 60) -> dict:
        con = self._con()
        now = int(time.time())
        secs = list(range(now - seconds + 1, now + 1))
        rows = con.execute(
            "SELECT sec, client, allowed, denied FROM stats WHERE sec > ?", (now - seconds,)
        ).fetchall()
        series: dict[str, list] = {}
        for sec, client, a, d in rows:
            row = series.setdefault(client, [{"t": x, "a": 0, "d": 0} for x in secs])
            i = sec - secs[0]
            if 0 <= i < len(secs):
                row[i]["a"], row[i]["d"] = a, d
        totals = {c: {"a": a, "d": d} for c, a, d in con.execute("SELECT client, allowed, denied FROM totals")}
        if now % 30 == 0:  # occasional cleanup of old per-second rows
            self._tx(lambda c: c.execute("DELETE FROM stats WHERE sec < ?", (now - 3600,)))
        return {"now": now, "series": series, "totals": totals}

    # ---- async facade used by the FastAPI app ------------------------------
    # sqlite3 calls block, so they must leave the event loop. They all go to ONE
    # dedicated thread with one connection: decisions queue up in order (fair,
    # ~0.1 ms each) instead of many threads fighting over SQLite's lock and
    # backing off with sleeps, which collapses throughput under heavy load.
    async def _run(self, fn, *args):
        return await asyncio.get_running_loop().run_in_executor(self._db_thread, fn, *args)

    async def check(self, client: str, cost: int = 1) -> Decision:
        return await self._run(self.check_sync, client, cost)

    async def get_config(self, client: str) -> dict:
        return await self._run(lambda: self._config(self._con(), client))

    async def set_config(self, client: str, cfg: dict) -> dict:
        return await self._run(self.set_config_sync, client, cfg)

    async def delete_config(self, client: str) -> None:
        await self._run(self.delete_config_sync, client)

    async def reset(self, client: str) -> None:
        await self._run(self.reset_sync, client)

    async def list_configs(self) -> dict[str, dict]:
        return await self._run(self.list_configs_sync)

    async def peek(self, client: str) -> dict:
        return await self._run(self.peek_sync, client)

    async def stats(self, seconds: int = 60) -> dict:
        return await self._run(self.stats_sync, seconds)

    async def ping(self) -> bool:
        return await self._run(lambda: self._con().execute("SELECT 1").fetchone()[0] == 1)

    async def close(self) -> None:
        self._db_thread.shutdown(wait=False)
