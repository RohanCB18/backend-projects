"""Tiny SQLite helper: one connection per thread, WAL mode, and a `tx()` that
runs a function inside a single write transaction.

BEGIN IMMEDIATE grabs SQLite's write lock at the start of the transaction, so
everything inside it is atomic with respect to every other thread AND every
other process using the same file. That is the primitive the whole queue's
"never claimed twice" guarantee rests on.
"""
from __future__ import annotations

import os
import sqlite3
import threading


class Store:
    def __init__(self, path: str, schema: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._local = threading.local()
        self._lock = threading.Lock()  # queue threads of this process in Python, not in SQLite's busy loop
        self.con().executescript(schema)

    def con(self) -> sqlite3.Connection:
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA busy_timeout=30000")
            con.execute("PRAGMA foreign_keys=ON")
            self._local.con = con
        return con

    def tx(self, fn):
        con = self.con()
        with self._lock:
            con.execute("BEGIN IMMEDIATE")
            try:
                out = fn(con)
                con.execute("COMMIT")
                return out
            except BaseException:
                con.execute("ROLLBACK")
                raise

    def query(self, sql: str, args: tuple = ()) -> list[dict]:
        return [dict(r) for r in self.con().execute(sql, args).fetchall()]
