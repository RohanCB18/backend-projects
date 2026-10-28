"""One test (or more) per requirement R1..R7, plus both stretch goals."""
import multiprocessing as mp
import os
import random
import threading
import time
import uuid
from typing import NamedTuple

import httpx
import psycopg
import pytest

from app.ledger import DEFAULT_DSN, SNAPSHOT_EVERY, Leg, Ledger, Rejected

DSN = os.getenv("LEDGER_DSN", DEFAULT_DSN)


class Db(NamedTuple):
    dsn: str
    schema: str


@pytest.fixture
def db():
    """A fresh Postgres schema per test: same tables and triggers, nothing shared."""
    d = Db(DSN, f"test_{uuid.uuid4().hex[:10]}")
    yield d
    with psycopg.connect(DSN, autocommit=True) as con:
        con.execute(f'DROP SCHEMA IF EXISTS "{d.schema}" CASCADE')


@pytest.fixture
def L(db):
    led = Ledger(db.dsn, schema=db.schema)
    yield led
    led.store.close()


def acct(L, name, **kw):
    return L.open_account(name, **kw)["id"]


# ---- R1: append-only -------------------------------------------------------------
def test_r1_history_cannot_be_rewritten(L):
    a = acct(L, "Alice")
    L.deposit(a, 100)
    for sql in ("UPDATE entries SET amount = 999999", "DELETE FROM entries", "TRUNCATE entries CASCADE",
                "UPDATE transactions SET description = 'nothing to see here'"):
        with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
            L.store.tx(lambda con: con.execute(sql))
    assert L.account(a)["balance"] == 10000


def test_r1_there_is_no_balance_column_to_update(L):
    cols = [r["column_name"] for r in L.store.query(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() "
        "AND table_name = 'accounts'")]
    assert "kind" in cols
    assert "balance" not in cols


# ---- R2: double entry --------------------------------------------------------------
def test_r2_every_transaction_sums_to_zero(L):
    a, b = acct(L, "Alice"), acct(L, "Bob")
    L.deposit(a, 500)
    t = L.transfer(a, b, 200)
    assert sorted(leg["amount"] for leg in t["legs"]) == [-20000, 20000]
    rows = L.store.query("SELECT txn_id, SUM(amount) AS s FROM entries GROUP BY txn_id")
    assert rows and all(r["s"] == 0 for r in rows)


def test_r2_unbalanced_transactions_are_refused(L):
    a = acct(L, "Alice")
    with pytest.raises(Rejected, match="unbalanced"):
        L.post("magic", [Leg(a, 100_00, "INR")])  # money from nowhere
    assert L.account(a)["balance"] == 0


# ---- R3: balance is derived ------------------------------------------------------------
def test_r3_balance_is_the_fold_of_events(L):
    a, b = acct(L, "Alice"), acct(L, "Bob")
    L.deposit(a, 1000)
    L.transfer(a, b, 250.75)
    L.withdraw(a, 100)
    events = [r["amount"] for r in L.store.query("SELECT amount FROM entries WHERE account_id = %s ORDER BY version", (a,))]
    assert events == [100000, -25075, -10000]
    assert L.account(a)["balance"] == sum(events) == 64925
    assert L.balance_full_replay(a) == 64925


# ---- R4: point in time -------------------------------------------------------------------
def test_r4_balance_as_of_a_past_moment(L):
    a = acct(L, "Alice")
    t0 = time.time() - 10 * 86400
    L.deposit(a, 1000, at=t0)                         # 10 days ago
    L.withdraw(a, 300, at=t0 + 3 * 86400)             # 7 days ago
    L.deposit(a, 50, at=t0 + 6 * 86400)               # 4 days ago
    assert L.balance(a, at=t0 - 1)["balance"] == 0
    assert L.balance(a, at=t0 + 86400)["balance"] == 100000
    assert L.balance(a, at=t0 + 5 * 86400)["balance"] == 70000
    assert L.balance(a)["balance"] == 75000


async def test_r4_point_in_time_over_http_with_iso_dates(db):
    from datetime import datetime, timedelta

    from app.main import app
    app.state.ledger = led = Ledger(db.dsn, schema=db.schema)
    a = acct(led, "Alice")
    last_week = datetime.now() - timedelta(days=7)
    led.deposit(a, 400, at=last_week.timestamp())
    led.deposit(a, 100)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get(f"/accounts/{a}/balance", params={"at": (last_week + timedelta(days=1)).isoformat()})
        assert r.json()["balance"] == 40000
        assert (await c.get(f"/accounts/{a}/balance")).json()["balance"] == 50000


# ---- R5: atomic rejection ------------------------------------------------------------------
def test_r5_overdraft_rejected_and_nothing_written(L):
    a, b = acct(L, "Alice"), acct(L, "Bob")
    L.deposit(a, 100)
    n_before = L.store.query("SELECT COUNT(*) AS n FROM entries")[0]["n"]
    with pytest.raises(Rejected, match="insufficient funds"):
        L.transfer(a, b, 100.01)
    assert L.store.query("SELECT COUNT(*) AS n FROM entries")[0]["n"] == n_before  # no half transfer
    assert L.account(b)["balance"] == 0
    assert L.rejections()[0]["reason"].startswith("insufficient funds")


def test_r5_credit_accounts_may_go_negative_up_to_their_limit(L):
    c, b = acct(L, "Card", kind="credit", credit_limit=500), acct(L, "Shop")
    L.transfer(c, b, 500)
    assert L.account(c)["balance"] == -50000
    with pytest.raises(Rejected, match="credit limit"):
        L.transfer(c, b, 0.01)


def test_r5_a_crash_mid_write_leaves_no_partial_transaction(L, monkeypatch):
    a, b = acct(L, "Alice"), acct(L, "Bob")
    L.deposit(a, 100)
    orig = L._append

    def exploding_append(con, kind, legs, *args):
        # write the first leg, then "crash" before the second
        con.execute("INSERT INTO transactions (id, kind, ts) VALUES ('tx_partial', 'transfer', 0)")
        con.execute("INSERT INTO entries (txn_id, account_id, amount, currency, version, ts) "
                    "VALUES ('tx_partial', %s, -5000, 'INR', 2, 0)", (a,))
        raise RuntimeError("power cut")

    monkeypatch.setattr(L, "_append", exploding_append)
    with pytest.raises(RuntimeError):
        L.transfer(a, b, 50)
    monkeypatch.setattr(L, "_append", orig)
    assert L.store.query("SELECT COUNT(*) AS n FROM entries WHERE txn_id = 'tx_partial'")[0]["n"] == 0
    assert L.account(a)["balance"] == 10000


# ---- R6: audit trail ------------------------------------------------------------------------
def test_r6_audit_reconstructs_and_explains_every_step(L):
    a, b = acct(L, "Alice"), acct(L, "Bob")
    L.deposit(a, 500, "Salary")
    L.transfer(a, b, 200, "Rent share")
    L.withdraw(a, 50, "ATM")
    audit = L.audit(a)
    assert [s["balance_after"] for s in audit["steps"]] == [50000, 30000, 25000]
    assert audit["steps"][1]["counterparties"] == [b]
    assert "Rent share" in audit["steps"][1]["explanation"]
    assert audit["consistent"] and audit["replayed_balance"] == L.account(a)["balance"]


# ---- R7: concurrency -----------------------------------------------------------------------------
def _race(L, mode, n=10, start=300, amount=200, think=0.03):
    a, b = acct(L, f"A-{mode}"), acct(L, f"B-{mode}")
    L.deposit(a, start)
    gate = threading.Barrier(n)
    ok = []

    def one():
        gate.wait()
        try:
            L.transfer(a, b, amount, mode=mode, think_time=think)
            ok.append(True)
        except Rejected:
            ok.append(False)

    ts = [threading.Thread(target=one) for _ in range(n)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return sum(ok), L.account(a)["balance"], L.account(b)["balance"]


def test_r7_the_bug_naive_check_then_write_overdraws(L):
    """Proves the race is real: without protection, several transfers pass the check."""
    succeeded, a_bal, b_bal = _race(L, "naive")
    assert succeeded > 1 and a_bal < 0
    assert a_bal + b_bal == 30000  # double-entry still holds; the overdraft rule didn't


@pytest.mark.parametrize("mode", ["optimistic", "pessimistic"])
def test_r7_concurrent_transfers_cannot_overdraw(L, mode):
    succeeded, a_bal, b_bal = _race(L, mode)
    assert succeeded == 1 and a_bal == 10000 and b_bal == 20000


def test_r7_optimistic_writer_retries_on_version_conflict(L):
    _race(L, "optimistic")
    assert L.conflicts > 0  # the losers really did detect the conflict and retry


def test_r7_random_concurrent_transfers_keep_every_invariant(L):
    accs = [acct(L, f"U{i}") for i in range(5)]
    for a in accs:
        L.deposit(a, 1000)

    def churn(seed):
        rnd = random.Random(seed)
        for _ in range(40):
            x, y = rnd.sample(accs, 2)
            try:
                L.transfer(x, y, rnd.choice([10, 50, 120, 400, 900]))
            except Rejected:
                pass

    ts = [threading.Thread(target=churn, args=(s,)) for s in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    bals = [L.account(a)["balance"] for a in accs]
    assert sum(bals) == 5 * 100000 and min(bals) >= 0
    assert L.integrity()["ok"]


def _proc_transfers(db, a, b, n):
    led = Ledger(db.dsn, schema=db.schema, pool_size=4)
    ok = 0
    for _ in range(n):
        try:
            led.transfer(a, b, 100)
            ok += 1
        except Rejected:
            pass
    return ok


def test_r7_concurrent_processes_cannot_overdraw(db, L):
    a, b = acct(L, "A"), acct(L, "B")
    L.deposit(a, 1500)  # affords exactly 15 transfers of 100
    with mp.get_context("spawn").Pool(4) as pool:
        ok = sum(pool.starmap(_proc_transfers, [(db, a, b, 10)] * 4))
    assert ok == 15 and L.account(a)["balance"] == 0 and L.account(b)["balance"] == 150000


# ---- stretch: snapshots and multi-currency ------------------------------------------------------
def test_stretch_snapshots_avoid_full_replay_and_match_it(L):
    a, b = acct(L, "Alice"), acct(L, "Bob")
    L.deposit(a, 10000)
    for _ in range(23):
        L.transfer(a, b, 7)
    info = L.account(a)
    assert info["snapshot_version"] == 20 and info["replayed_events"] == 4  # not 24
    assert info["balance"] == L.balance_full_replay(a) == 10000_00 - 23 * 700
    assert SNAPSHOT_EVERY == 5 and L.integrity()["snapshots_match_replay"]


def test_stretch_cross_currency_transfer_balances_per_currency(L):
    inr, usd = acct(L, "Priya"), acct(L, "Sam", currency="USD")
    L.deposit(inr, 1000)
    t = L.transfer(inr, usd, 832.50)  # 832.50 INR at 83.25 = 10.00 USD
    assert L.account(usd)["balance"] == 1000
    per_cur = {}
    for leg in t["legs"]:
        per_cur[leg["currency"]] = per_cur.get(leg["currency"], 0) + leg["amount"]
    assert per_cur == {"INR": 0, "USD": 0}
    assert L.integrity()["ok"]


def test_idempotency_key_makes_retries_safe(L):
    a, b = acct(L, "Alice"), acct(L, "Bob")
    L.deposit(a, 100)
    t1 = L.transfer(a, b, 30, idempotency_key="client-req-1")
    t2 = L.transfer(a, b, 30, idempotency_key="client-req-1")  # network retry
    assert t1["id"] == t2["id"] and L.account(b)["balance"] == 3000
