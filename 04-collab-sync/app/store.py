"""Durable operation log (append-only) + the live documents rebuilt from it.

The op log IS the document: every insert/delete ever made, in server order.
  * history / time travel (R4): replay the log up to sequence N
  * reconnecting clients: "send me everything after seq N"
  * dedupe (R7): UNIQUE(doc, op_key) means a resent op is stored once
"""
from __future__ import annotations

import json
import os
import sqlite3
import time

from .rga import RGA, op_key

SCHEMA = """
CREATE TABLE IF NOT EXISTS ops (
    doc    TEXT NOT NULL,
    seq    INTEGER NOT NULL,
    op_key TEXT NOT NULL,
    site   TEXT NOT NULL,
    op     TEXT NOT NULL,
    ts     REAL NOT NULL,
    PRIMARY KEY (doc, seq),
    UNIQUE (doc, op_key)
);
"""


class DocStore:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        # The server touches the log only from the event-loop thread, so one connection is enough.
        self.con = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=NORMAL")
        self.con.executescript(SCHEMA)
        self.docs: dict[str, RGA] = {}
        self.seq: dict[str, int] = {}

    def doc(self, name: str) -> RGA:
        if name not in self.docs:
            d = RGA("server")
            seq = 0
            for (op, s) in self.con.execute("SELECT op, seq FROM ops WHERE doc = ? ORDER BY seq", (name,)):
                d.apply(json.loads(op))
                seq = s
            self.docs[name], self.seq[name] = d, seq
        return self.docs[name]

    def append(self, name: str, ops: list[dict]) -> list[dict]:
        """Store + apply new ops. Returns only the ops that were new, each tagged
        with its server sequence number. Duplicates (resends) are dropped."""
        d = self.doc(name)
        fresh = []
        now = time.time()
        self.con.execute("BEGIN")
        try:
            for op in ops:
                key = op_key(op)
                seq = self.seq[name] + 1
                cur = self.con.execute(
                    "INSERT OR IGNORE INTO ops (doc, seq, op_key, site, op, ts) VALUES (?, ?, ?, ?, ?, ?)",
                    (name, seq, key, op["id"][1], json.dumps(op), now))
                if cur.rowcount == 0:
                    continue  # already have it
                self.seq[name] = seq
                d.apply(op)
                fresh.append({**op, "seq": seq})
            self.con.execute("COMMIT")
        except BaseException:
            self.con.execute("ROLLBACK")
            self.docs.pop(name, None)  # rebuild from the log next time
            raise
        return fresh

    def since(self, name: str, seq: int) -> list[dict]:
        self.doc(name)
        return [{**json.loads(op), "seq": s} for op, s in
                self.con.execute("SELECT op, seq FROM ops WHERE doc = ? AND seq > ? ORDER BY seq", (name, seq))]

    def at(self, name: str, seq: int) -> RGA:
        """The document exactly as it was after op number `seq`."""
        d = RGA("history")
        for (op,) in self.con.execute("SELECT op FROM ops WHERE doc = ? AND seq <= ? ORDER BY seq", (name, seq)):
            d.apply(json.loads(op))
        return d

    def history(self, name: str) -> list[dict]:
        """Ops grouped into edits: consecutive ops by the same site within 1.5 s."""
        groups: list[dict] = []
        for op, seq, site, ts in self.con.execute(
                "SELECT op, seq, site, ts FROM ops WHERE doc = ? ORDER BY seq", (name,)):
            o = json.loads(op)
            g = groups[-1] if groups else None
            if g and g["site"] == site and ts - g["ts_end"] < 1.5:
                g["seq_end"], g["ts_end"] = seq, ts
            else:
                g = {"site": site, "seq_start": seq, "seq_end": seq, "ts": ts, "ts_end": ts, "inserted": "", "deleted": 0}
                groups.append(g)
            if o["t"] == "i":
                g["inserted"] += o["ch"]
            else:
                g["deleted"] += 1
        return groups

    def reset(self, name: str) -> None:
        self.con.execute("DELETE FROM ops WHERE doc = ?", (name,))
        self.docs.pop(name, None)
        self.seq.pop(name, None)
