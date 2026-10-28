"""Durable operation log (append-only, in Postgres) + the live documents rebuilt from it.

The op log IS the document: every insert/delete ever made, in server order.
  * history / time travel (R4): replay the log up to sequence N
  * reconnecting clients: "send me everything after seq N"
  * dedupe (R7): UNIQUE(doc, op_key) means a resent op is stored once

This class is NOT thread-safe on purpose: the server calls it from one
dedicated thread only (see main.py), which also keeps sequence numbers
gap-free without any locking.
"""
from __future__ import annotations

import json
import time
import zlib

import psycopg
from psycopg.rows import dict_row

from .rga import RGA, op_key

DEFAULT_DSN = "postgresql://backend:backend@127.0.0.1:15432/collab"  # docker-compose.yml

SCHEMA = """
CREATE TABLE IF NOT EXISTS ops (
    doc    TEXT NOT NULL,
    seq    BIGINT NOT NULL,
    op_key TEXT NOT NULL,
    site   TEXT NOT NULL,
    op     JSONB NOT NULL,
    ts     DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (doc, seq),
    UNIQUE (doc, op_key)
);
"""


class DocStore:
    def __init__(self, dsn: str = DEFAULT_DSN, schema: str = "public"):
        with psycopg.connect(dsn, autocommit=True) as con:
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        self.con = psycopg.connect(dsn, autocommit=True, row_factory=dict_row, options=f"-c search_path={schema}")
        with self.con.transaction():
            self.con.execute("SELECT pg_advisory_xact_lock(%s)", (zlib.crc32(schema.encode()),))
            self.con.execute(SCHEMA)
        self.docs: dict[str, RGA] = {}
        self.seq: dict[str, int] = {}

    def close(self) -> None:
        self.con.close()

    def doc(self, name: str) -> RGA:
        if name not in self.docs:
            d = RGA("server")
            seq = 0
            for row in self.con.execute("SELECT op, seq FROM ops WHERE doc = %s ORDER BY seq", (name,)):
                d.apply(row["op"])
                seq = row["seq"]
            self.docs[name], self.seq[name] = d, seq
        return self.docs[name]

    def append(self, name: str, ops: list[dict]) -> list[dict]:
        """Store + apply new ops. Returns only the ops that were new, each tagged
        with its server sequence number. Duplicates (resends) are dropped."""
        d = self.doc(name)
        fresh = []
        now = time.time()
        seq = self.seq[name]
        try:
            with self.con.transaction():
                for op in ops:
                    cur = self.con.execute(
                        "INSERT INTO ops (doc, seq, op_key, site, op, ts) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (doc, op_key) DO NOTHING",
                        (name, seq + 1, op_key(op), op["id"][1], psycopg.types.json.Jsonb(op), now))
                    if cur.rowcount == 0:
                        continue  # already have it
                    seq += 1
                    fresh.append({**op, "seq": seq})
        except BaseException:
            self.docs.pop(name, None)  # rebuild from the log next time
            raise
        # only touch the in-memory document once the ops are safely committed
        for op in fresh:
            d.apply(op)
        self.seq[name] = seq
        return fresh

    def since(self, name: str, seq: int) -> list[dict]:
        self.doc(name)
        return [{**r["op"], "seq": r["seq"]} for r in
                self.con.execute("SELECT op, seq FROM ops WHERE doc = %s AND seq > %s ORDER BY seq", (name, seq))]

    def at(self, name: str, seq: int) -> RGA:
        """The document exactly as it was after op number `seq`."""
        d = RGA("history")
        for r in self.con.execute("SELECT op FROM ops WHERE doc = %s AND seq <= %s ORDER BY seq", (name, seq)):
            d.apply(r["op"])
        return d

    def history(self, name: str) -> list[dict]:
        """Ops grouped into edits: consecutive ops by the same site within 1.5 s."""
        groups: list[dict] = []
        for r in self.con.execute("SELECT op, seq, site, ts FROM ops WHERE doc = %s ORDER BY seq", (name,)):
            o, seq, site, ts = r["op"], r["seq"], r["site"], r["ts"]
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
        self.con.execute("DELETE FROM ops WHERE doc = %s", (name,))
        self.docs.pop(name, None)
        self.seq.pop(name, None)
