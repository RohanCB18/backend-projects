"""Event-sourced, double-entry ledger.

Vocabulary
  transaction  one business event ("Alice paid Bob Rs 200"). Immutable.
  entry        one leg of a transaction on one account: (account, signed amount).
               Every transaction's entries sum to zero per currency (R2).
  version      per-account sequence number of entries: 1, 2, 3, ...

Rules this module enforces
  R1  entries/transactions are append-only. SQL triggers reject UPDATE/DELETE,
      so not even a bug in this file could rewrite history.
  R3  there is no balance column anywhere. balance = sum(entries), optionally
      starting from a snapshot (which is only a cache, rebuildable any time).
  R5  all legs of a transaction are inserted in ONE database transaction after
      the invariant check; a rejected transfer writes nothing.
  R7  concurrent writers: optimistic concurrency. We read (version, balance)
      without locks, check the rules, then append *only if every touched
      account is still at the version we read*. If not, someone else moved
      money in between: re-read and try again.

Money is stored as integer minor units (paise / cents), never floats:
0.1 + 0.2 != 0.3 in floating point, and a bank can't lose a paisa.
"""
from __future__ import annotations

import json
import random
import sqlite3
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal

from .db import Store

SNAPSHOT_EVERY = 5  # snapshot an account's balance every N entries (stretch goal)

# Static demo exchange rates: 1 unit of key currency = value INR
RATES_TO_INR = {"INR": Decimal("1"), "USD": Decimal("83.25"), "EUR": Decimal("90.10"), "GBP": Decimal("105.40")}

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    kind         TEXT NOT NULL CHECK (kind IN ('customer', 'credit', 'system')),
    currency     TEXT NOT NULL DEFAULT 'INR',
    credit_limit INTEGER NOT NULL DEFAULT 0,   -- minor units; how far below zero a 'credit' account may go
    sandbox      INTEGER NOT NULL DEFAULT 0,   -- race-demo accounts
    created_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    id              TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,            -- deposit | withdrawal | transfer | fx_transfer
    description     TEXT,
    ts              REAL NOT NULL,
    idempotency_key TEXT UNIQUE,
    meta            TEXT
);

CREATE TABLE IF NOT EXISTS entries (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,   -- global order of events
    txn_id     TEXT NOT NULL REFERENCES transactions(id),
    account_id TEXT NOT NULL REFERENCES accounts(id),
    amount     INTEGER NOT NULL,                    -- signed, minor units
    currency   TEXT NOT NULL,
    version    INTEGER NOT NULL,                    -- per-account sequence
    ts         REAL NOT NULL,
    UNIQUE (account_id, version)                    -- two writers can never both append "version 8"
);
CREATE INDEX IF NOT EXISTS entries_txn ON entries (txn_id);
CREATE INDEX IF NOT EXISTS entries_acct_ts ON entries (account_id, ts);

CREATE TABLE IF NOT EXISTS snapshots (
    account_id TEXT NOT NULL, version INTEGER NOT NULL, balance INTEGER NOT NULL, ts REAL NOT NULL,
    PRIMARY KEY (account_id, version)
);

CREATE TABLE IF NOT EXISTS rejections (
    id INTEGER PRIMARY KEY, ts REAL NOT NULL, kind TEXT, detail TEXT, reason TEXT
);

CREATE TRIGGER IF NOT EXISTS entries_append_only_u BEFORE UPDATE ON entries
BEGIN SELECT RAISE(ABORT, 'ledger entries are immutable'); END;
CREATE TRIGGER IF NOT EXISTS entries_append_only_d BEFORE DELETE ON entries
BEGIN SELECT RAISE(ABORT, 'ledger entries are immutable'); END;
CREATE TRIGGER IF NOT EXISTS transactions_append_only_u BEFORE UPDATE ON transactions
BEGIN SELECT RAISE(ABORT, 'transactions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS transactions_append_only_d BEFORE DELETE ON transactions
BEGIN SELECT RAISE(ABORT, 'transactions are immutable'); END;
"""


class Rejected(Exception):
    """Business rule violated (overdraft, unknown account, bad amount...)."""


class VersionConflict(Exception):
    """Someone appended to an account between our read and our write."""


@dataclass
class Leg:
    account: str
    amount: int  # minor units, signed
    currency: str


def to_minor(amount) -> int:
    d = Decimal(str(amount)) * 100
    if d != d.to_integral_value():
        raise Rejected("amount has more than 2 decimal places")
    return int(d)


def from_minor(v: int) -> float:
    return v / 100


class Ledger:
    def __init__(self, path: str):
        self.store = Store(path, SCHEMA)
        self.conflicts = 0  # how many optimistic retries happened (shown in the UI)
        for cur in RATES_TO_INR:
            self._ensure_system(f"world:{cur}", f"Outside world ({cur})", cur)
            self._ensure_system(f"fx:{cur}", f"FX pool ({cur})", cur)

    def _ensure_system(self, acc_id: str, name: str, cur: str) -> None:
        self.store.tx(lambda con: con.execute(
            "INSERT OR IGNORE INTO accounts (id, name, kind, currency, created_at) VALUES (?, ?, 'system', ?, ?)",
            (acc_id, name, cur, time.time())))

    # ------------------------------------------------------------------ accounts
    def open_account(self, name: str, kind: str = "customer", currency: str = "INR", credit_limit=0,
                     acc_id: str | None = None, sandbox: bool = False) -> dict:
        if currency not in RATES_TO_INR:
            raise Rejected(f"unsupported currency {currency}")
        if kind not in ("customer", "credit"):
            raise Rejected("kind must be customer or credit")
        acc_id = acc_id or f"{name.lower().replace(' ', '-')}-{uuid.uuid4().hex[:4]}"
        limit = to_minor(credit_limit) if kind == "credit" else 0
        try:
            self.store.tx(lambda con: con.execute(
                "INSERT INTO accounts (id, name, kind, currency, credit_limit, sandbox, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)", (acc_id, name, kind, currency, limit, int(sandbox), time.time())))
        except Exception as e:
            raise Rejected(str(e))
        return self.account(acc_id)

    def account_row(self, acc_id: str, con=None) -> dict:
        con = con or self.store.con()
        row = con.execute("SELECT * FROM accounts WHERE id = ?", (acc_id,)).fetchone()
        if row is None:
            raise Rejected(f"no such account: {acc_id}")
        return dict(row)

    def account(self, acc_id: str) -> dict:
        a = self.account_row(acc_id)
        version, balance, replayed, snap = self._state(self.store.con(), acc_id)
        return {**a, "balance": balance, "version": version, "replayed_events": replayed, "snapshot_version": snap,
                "balance_display": from_minor(balance), "credit_limit_display": from_minor(a["credit_limit"])}

    def accounts(self, include_system: bool = True, include_sandbox: bool = False) -> list[dict]:
        rows = self.store.query("SELECT id FROM accounts ORDER BY kind = 'system', created_at")
        out = [self.account(r["id"]) for r in rows]
        return [a for a in out if (include_system or a["kind"] != "system") and (include_sandbox or not a["sandbox"])]

    # ------------------------------------------------------------------ deriving state (R3, R4)
    def _state(self, con, acc_id: str, at: float | None = None) -> tuple[int, int, int, int]:
        """(version, balance, events_replayed, snapshot_version) as of time `at` (default: now).
        Start from the newest snapshot not after `at`, then fold the entries after it."""
        at = float("inf") if at is None else at
        snap = con.execute(
            "SELECT version, balance FROM snapshots WHERE account_id = ? AND ts <= ? ORDER BY version DESC LIMIT 1",
            (acc_id, at)).fetchone()
        sv, sb = (snap["version"], snap["balance"]) if snap else (0, 0)
        row = con.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(amount), 0) AS s, COALESCE(MAX(version), ?) AS v "
            "FROM entries WHERE account_id = ? AND version > ? AND ts <= ?", (sv, acc_id, sv, at)).fetchone()
        return row["v"], sb + row["s"], row["n"], sv

    def balance(self, acc_id: str, at: float | None = None) -> dict:
        self.account_row(acc_id)
        version, bal, replayed, snap = self._state(self.store.con(), acc_id, at)
        return {"account": acc_id, "at": at, "balance": bal, "balance_display": from_minor(bal), "version": version,
                "replayed_events": replayed, "from_snapshot_version": snap}

    def balance_full_replay(self, acc_id: str, at: float | None = None) -> int:
        """No snapshots: fold every event from the beginning (used to verify snapshots)."""
        at = float("inf") if at is None else at
        return self.store.con().execute(
            "SELECT COALESCE(SUM(amount), 0) FROM entries WHERE account_id = ? AND ts <= ?", (acc_id, at)).fetchone()[0]

    # ------------------------------------------------------------------ writing (R2, R5, R7)
    def _check_rules(self, acc: dict, balance_after: int) -> None:
        if acc["kind"] == "customer" and balance_after < 0:
            raise Rejected(f"insufficient funds in {acc['id']}: balance would be {from_minor(balance_after):,.2f}")
        if acc["kind"] == "credit" and balance_after < -acc["credit_limit"]:
            raise Rejected(f"credit limit exceeded on {acc['id']} (limit {from_minor(acc['credit_limit']):,.2f})")

    def post(self, kind: str, legs: list[Leg], description: str = "", mode: str = "optimistic",
             think_time: float = 0.0, at: float | None = None, idempotency_key: str | None = None,
             meta: dict | None = None, max_retries: int = 50) -> dict:
        """Append one balanced transaction.

        mode="optimistic"   read without locks, validate, append if versions unchanged, else retry
        mode="pessimistic"  take the write lock first, then read + validate + append
        mode="naive"        read without locks, validate, append WITHOUT re-checking (the bug, for the demo)

        think_time widens the gap between "check" and "write" (a fraud check, a
        network call...) so races become visible in demos and tests.
        """
        by_cur: dict[str, int] = {}
        for leg in legs:
            by_cur[leg.currency] = by_cur.get(leg.currency, 0) + leg.amount
        if any(by_cur.values()):
            raise Rejected(f"unbalanced transaction: {by_cur}")  # double-entry, R2
        if any(leg.amount == 0 for leg in legs):
            raise Rejected("zero-amount entry")

        if idempotency_key:
            existing = self.store.query("SELECT id FROM transactions WHERE idempotency_key = ?", (idempotency_key,))
            if existing:
                return {**self.transaction(existing[0]["id"]), "idempotent_replay": True}

        if mode == "pessimistic":
            def locked(con):
                expected = {}
                for leg in legs:
                    acc = self.account_row(leg.account, con)
                    v, bal, _, _ = self._state(con, leg.account)
                    self._check_rules(acc, bal + leg.amount)
                    expected[leg.account] = v
                if think_time:
                    time.sleep(think_time)  # holding the lock: everyone else waits
                return self._append(con, kind, legs, description, expected, at, idempotency_key, meta)
            try:
                return self.store.tx(locked)
            except Rejected as e:
                self._reject(kind, legs, str(e))
                raise

        for attempt in range(max_retries):
            # 1. read a snapshot of every touched account, without locking
            con = self.store.con()
            expected, checks = {}, []
            for leg in legs:
                acc = self.account_row(leg.account, con)
                v, bal, _, _ = self._state(con, leg.account)
                expected[leg.account] = v
                checks.append((acc, bal + leg.amount))
            # 2. validate business rules against that snapshot
            try:
                for acc, after in checks:
                    self._check_rules(acc, after)
            except Rejected as e:
                self._reject(kind, legs, str(e))
                raise
            if think_time:
                time.sleep(think_time)
            # 3. append, conditional on nothing having changed
            try:
                return self.store.tx(lambda con: self._append(
                    con, kind, legs, description, None if mode == "naive" else expected, at, idempotency_key, meta))
            except VersionConflict:
                self.conflicts += 1
                time.sleep(random.uniform(0, 0.005 * (attempt + 1)))  # jittered backoff, then re-read
            except sqlite3.IntegrityError as e:
                # the same idempotency key was committed by a concurrent request a moment ago
                if idempotency_key and "idempotency_key" in str(e):
                    existing = self.store.query("SELECT id FROM transactions WHERE idempotency_key = ?",
                                                (idempotency_key,))
                    return {**self.transaction(existing[0]["id"]), "idempotent_replay": True}
                raise
        raise Rejected("too much contention, gave up")

    def _append(self, con, kind, legs, description, expected, at, idempotency_key, meta) -> dict:
        now = time.time()
        # versions are re-read inside the write lock
        current = {}
        for leg in legs:
            v = con.execute("SELECT COALESCE(MAX(version), 0) FROM entries WHERE account_id = ?",
                            (leg.account,)).fetchone()[0]
            if expected is not None and v != expected[leg.account]:
                raise VersionConflict(leg.account)
            current[leg.account] = v
        ts = now if at is None else at
        if at is not None:  # back-dated import (demo seed): must not go before existing history
            last = con.execute(f"SELECT COALESCE(MAX(ts), 0) FROM entries WHERE account_id IN "
                               f"({','.join('?' * len(legs))})", [leg.account for leg in legs]).fetchone()[0]
            if at < last or at > now:
                raise Rejected("back-dated events must be after existing history and not in the future")
        txn_id = f"tx_{uuid.uuid4().hex[:12]}"
        con.execute("INSERT INTO transactions (id, kind, description, ts, idempotency_key, meta) VALUES (?, ?, ?, ?, ?, ?)",
                    (txn_id, kind, description, ts, idempotency_key, json.dumps(meta or {})))
        for leg in legs:
            current[leg.account] += 1
            version = current[leg.account]
            con.execute("INSERT INTO entries (txn_id, account_id, amount, currency, version, ts) VALUES (?, ?, ?, ?, ?, ?)",
                        (txn_id, leg.account, leg.amount, leg.currency, version, ts))
            if version % SNAPSHOT_EVERY == 0:
                _, bal, _, _ = self._state(con, leg.account)
                con.execute("INSERT OR REPLACE INTO snapshots (account_id, version, balance, ts) VALUES (?, ?, ?, ?)",
                            (leg.account, version, bal, ts))
        return {"id": txn_id, "kind": kind, "description": description, "ts": ts,
                "legs": [{"account": leg.account, "amount": leg.amount, "currency": leg.currency} for leg in legs]}

    def _reject(self, kind: str, legs: list[Leg], reason: str) -> None:
        detail = ", ".join(f"{leg.account} {leg.amount:+d}" for leg in legs)
        self.store.tx(lambda con: con.execute(
            "INSERT INTO rejections (ts, kind, detail, reason) VALUES (?, ?, ?, ?)", (time.time(), kind, detail, reason)))

    # ------------------------------------------------------------------ business operations
    def deposit(self, acc_id: str, amount, description: str = "Cash deposit", **kw) -> dict:
        cur = self.account_row(acc_id)["currency"]
        m = to_minor(amount)
        if m <= 0:
            raise Rejected("amount must be positive")
        return self.post("deposit", [Leg(f"world:{cur}", -m, cur), Leg(acc_id, m, cur)], description, **kw)

    def withdraw(self, acc_id: str, amount, description: str = "ATM withdrawal", **kw) -> dict:
        cur = self.account_row(acc_id)["currency"]
        m = to_minor(amount)
        if m <= 0:
            raise Rejected("amount must be positive")
        return self.post("withdrawal", [Leg(acc_id, -m, cur), Leg(f"world:{cur}", m, cur)], description, **kw)

    def transfer(self, src: str, dst: str, amount, description: str = "", **kw) -> dict:
        if src == dst:
            raise Rejected("cannot transfer to the same account")
        a, b = self.account_row(src), self.account_row(dst)
        m = to_minor(amount)
        if m <= 0:
            raise Rejected("amount must be positive")
        if a["currency"] == b["currency"]:
            legs = [Leg(src, -m, a["currency"]), Leg(dst, m, b["currency"])]
            return self.post("transfer", legs, description or f"{src} -> {dst}", **kw)
        # Cross-currency (stretch goal): money crosses through FX pool accounts so
        # each currency still balances on its own: USD legs sum to 0, INR legs sum to 0.
        rate = RATES_TO_INR[a["currency"]] / RATES_TO_INR[b["currency"]]
        converted = int((Decimal(m) * rate).quantize(Decimal(1)))
        legs = [Leg(src, -m, a["currency"]), Leg(f"fx:{a['currency']}", m, a["currency"]),
                Leg(f"fx:{b['currency']}", -converted, b["currency"]), Leg(dst, converted, b["currency"])]
        return self.post("fx_transfer", legs, description or f"{src} -> {dst} @ {rate:.4f}",
                         meta={"rate": str(rate), "from": a["currency"], "to": b["currency"]}, **kw)

    # ------------------------------------------------------------------ reading history (R6)
    def transaction(self, txn_id: str) -> dict:
        t = self.store.query("SELECT * FROM transactions WHERE id = ?", (txn_id,))
        if not t:
            raise Rejected("no such transaction")
        t = t[0]
        t["meta"] = json.loads(t["meta"] or "{}")
        t["legs"] = self.store.query("SELECT account_id AS account, amount, currency, version FROM entries "
                                     "WHERE txn_id = ? ORDER BY seq", (txn_id,))
        return t

    def transactions(self, limit: int = 50) -> list[dict]:
        rows = self.store.query("SELECT id FROM transactions ORDER BY ts DESC, rowid DESC LIMIT ?", (limit,))
        return [self.transaction(r["id"]) for r in rows]

    def audit(self, acc_id: str, at: float | None = None) -> dict:
        """Rebuild the balance step by step and explain every step."""
        acc = self.account_row(acc_id)
        at_ = float("inf") if at is None else at
        rows = self.store.query(
            """SELECT e.seq, e.version, e.amount, e.ts, e.txn_id, t.kind, t.description, t.meta,
                      (SELECT GROUP_CONCAT(o.account_id || ':' || o.amount, ',') FROM entries o
                        WHERE o.txn_id = e.txn_id AND o.account_id != e.account_id) AS others
                 FROM entries e JOIN transactions t ON t.id = e.txn_id
                WHERE e.account_id = ? AND e.ts <= ? ORDER BY e.version""", (acc_id, at_))
        running, steps = 0, []
        for r in rows:
            before = running
            running += r["amount"]
            counter = [o.split(":")[0] if not o.startswith(("world:", "fx:")) else ":".join(o.split(":")[:2])
                       for o in (r["others"] or "").split(",") if o]
            verb = "received" if r["amount"] > 0 else "sent"
            steps.append({
                "version": r["version"], "ts": r["ts"], "txn_id": r["txn_id"], "kind": r["kind"],
                "description": r["description"], "amount": r["amount"], "balance_before": before,
                "balance_after": running, "counterparties": counter,
                "explanation": f"v{r['version']}: {verb} {abs(r['amount']) / 100:,.2f} {acc['currency']} "
                               f"({r['kind']}, {r['description'] or 'no description'}) "
                               f"-> balance {before / 100:,.2f} -> {running / 100:,.2f}",
            })
        derived = self._state(self.store.con(), acc_id, at)[1]
        return {"account": acc, "as_of": at, "steps": steps, "replayed_balance": running,
                "snapshot_balance": derived, "consistent": running == derived}

    def timeline(self, acc_id: str) -> list[dict]:
        out, running = [], 0
        for r in self.store.query("SELECT ts, amount FROM entries WHERE account_id = ? ORDER BY version", (acc_id,)):
            running += r["amount"]
            out.append({"ts": r["ts"], "balance": running})
        return out

    def rejections(self, limit: int = 30) -> list[dict]:
        return self.store.query("SELECT * FROM rejections ORDER BY id DESC LIMIT ?", (limit,))

    # ------------------------------------------------------------------ integrity report
    def integrity(self) -> dict:
        q = self.store.query
        unbalanced = q("SELECT txn_id, currency, SUM(amount) AS s FROM entries GROUP BY txn_id, currency HAVING s != 0")
        per_currency = q("SELECT currency, SUM(amount) AS total FROM entries GROUP BY currency")
        negative = [a for a in self.accounts(include_sandbox=False) if a["kind"] == "customer" and a["balance"] < 0]
        over_limit = [a for a in self.accounts(include_sandbox=False)
                      if a["kind"] == "credit" and a["balance"] < -a["credit_limit"]]
        bad_snapshots = []
        for s in q("SELECT * FROM snapshots"):
            real = self.store.con().execute("SELECT SUM(amount) FROM entries WHERE account_id = ? AND version <= ?",
                                            (s["account_id"], s["version"])).fetchone()[0]
            if real != s["balance"]:
                bad_snapshots.append(s)
        triggers = q("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'trigger' AND name LIKE '%append_only%'")[0]["n"]
        immutable = triggers == 4
        if immutable and q("SELECT 1 FROM entries LIMIT 1"):
            try:  # actually try to rewrite history; the trigger must refuse
                self.store.tx(lambda con: con.execute(
                    "UPDATE entries SET amount = amount + 1 WHERE seq = (SELECT MIN(seq) FROM entries)"))
                immutable = False
            except sqlite3.DatabaseError:
                pass
        counts = q("SELECT (SELECT COUNT(*) FROM transactions) AS transactions, (SELECT COUNT(*) FROM entries) AS entries, "
                   "(SELECT COUNT(*) FROM snapshots) AS snapshots")[0]
        return {
            "ok": not unbalanced and all(c["total"] == 0 for c in per_currency) and not negative and not over_limit
                  and not bad_snapshots and immutable,
            "every_transaction_balances": not unbalanced,
            "unbalanced": unbalanced,
            "sum_per_currency": per_currency,
            "customers_overdrawn": [a["id"] for a in negative],
            "credit_over_limit": [a["id"] for a in over_limit],
            "snapshots_match_replay": not bad_snapshots,
            "entries_immutable": immutable,
            "optimistic_retries_so_far": self.conflicts,
            **counts,
        }

    def reset(self) -> None:
        """Demo helper. Real ledgers are never reset; here we drop and rebuild."""
        def fn(con):
            for t in ("entries", "transactions", "snapshots", "rejections", "accounts"):
                con.execute(f"DROP TABLE IF EXISTS {t}")
        self.store.tx(fn)
        self.store.con().executescript(SCHEMA)
        for cur in RATES_TO_INR:
            self._ensure_system(f"world:{cur}", f"Outside world ({cur})", cur)
            self._ensure_system(f"fx:{cur}", f"FX pool ({cur})", cur)
