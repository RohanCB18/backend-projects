"""03 - Event-Sourced Ledger (double-entry bank core)."""
from __future__ import annotations

import os
import random
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .ledger import DEFAULT_DSN, RATES_TO_INR, Ledger, Rejected

DSN = os.getenv("LEDGER_DSN", DEFAULT_DSN)
SCHEMA = os.getenv("LEDGER_SCHEMA", "public")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.winmm.timeBeginPeriod(1)
    app.state.ledger = Ledger(DSN, schema=SCHEMA)
    yield
    app.state.ledger.store.close()


app = FastAPI(title="03 Event-Sourced Ledger", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.exception_handler(Rejected)
async def rejected(_: Request, exc: Rejected):
    return JSONResponse({"error": str(exc)}, status_code=422)


def L(request: Request) -> Ledger:
    return request.app.state.ledger


def parse_at(at: str | None) -> float | None:
    """Accepts epoch seconds or an ISO date/time ('2026-10-20' or '2026-10-20T18:00')."""
    if not at:
        return None
    try:
        return float(at)
    except ValueError:
        return datetime.fromisoformat(at).timestamp()


# ---- accounts ----------------------------------------------------------------
class AccountIn(BaseModel):
    name: str = Field(min_length=1, max_length=40)
    kind: Literal["customer", "credit"] = "customer"
    currency: str = "INR"
    credit_limit: float = 0


@app.get("/accounts")
async def accounts(request: Request, include_system: bool = True):
    return await run_in_threadpool(L(request).accounts, include_system)


@app.post("/accounts")
async def open_account(body: AccountIn, request: Request):
    return await run_in_threadpool(L(request).open_account, body.name, body.kind, body.currency, body.credit_limit)


@app.get("/accounts/{acc_id}")
async def account(acc_id: str, request: Request):
    return await run_in_threadpool(L(request).account, acc_id)


@app.get("/accounts/{acc_id}/balance")
async def balance(acc_id: str, request: Request, at: str | None = None):
    """R4: point-in-time balance. ?at=2026-10-20T18:00 or epoch seconds."""
    return await run_in_threadpool(L(request).balance, acc_id, parse_at(at))


@app.get("/accounts/{acc_id}/audit")
async def audit(acc_id: str, request: Request, at: str | None = None):
    """R6: how did this account reach its balance? Every step, explained."""
    return await run_in_threadpool(L(request).audit, acc_id, parse_at(at))


@app.get("/accounts/{acc_id}/timeline")
async def timeline(acc_id: str, request: Request):
    return await run_in_threadpool(L(request).timeline, acc_id)


# ---- money movement ------------------------------------------------------------
class MoveIn(BaseModel):
    account: str
    amount: float = Field(gt=0)
    description: str = ""


class TransferIn(BaseModel):
    from_account: str
    to_account: str
    amount: float = Field(gt=0)
    description: str = ""
    idempotency_key: str | None = None


@app.post("/deposit")
async def deposit(body: MoveIn, request: Request):
    return await run_in_threadpool(L(request).deposit, body.account, body.amount, body.description or "Cash deposit")


@app.post("/withdraw")
async def withdraw(body: MoveIn, request: Request):
    return await run_in_threadpool(L(request).withdraw, body.account, body.amount,
                                   body.description or "ATM withdrawal")


@app.post("/transfer")
async def transfer(body: TransferIn, request: Request):
    return await run_in_threadpool(lambda: L(request).transfer(
        body.from_account, body.to_account, body.amount, body.description, idempotency_key=body.idempotency_key))


@app.get("/transactions")
async def transactions(request: Request, limit: int = 50):
    return await run_in_threadpool(L(request).transactions, limit)


@app.get("/transactions/{txn_id}")
async def transaction(txn_id: str, request: Request):
    return await run_in_threadpool(L(request).transaction, txn_id)


@app.get("/rejections")
async def rejections(request: Request):
    return await run_in_threadpool(L(request).rejections)


@app.get("/integrity")
async def integrity(request: Request):
    return await run_in_threadpool(L(request).integrity)


@app.get("/fx")
async def fx():
    return {k: str(v) for k, v in RATES_TO_INR.items()}


@app.get("/dashboard")
async def dashboard(request: Request):
    led = L(request)

    def build():
        return {"accounts": led.accounts(include_system=True), "transactions": led.transactions(25),
                "rejections": led.rejections(12), "optimistic_retries": led.conflicts}

    return await run_in_threadpool(build)


# ---- demos ------------------------------------------------------------------------
class RaceIn(BaseModel):
    mode: Literal["naive", "optimistic", "pessimistic"] = "optimistic"
    start_balance: float = 300
    amount: float = 200
    concurrent: int = Field(10, ge=2, le=50)
    think_ms: float = Field(30, ge=0, le=500)


@app.post("/demo/race")
async def race(body: RaceIn, request: Request):
    """R7, live: N concurrent transfers out of an account that can afford only some."""
    led = L(request)

    def go():
        tag = uuid.uuid4().hex[:6]  # unique per run, even for several runs in the same second
        a = led.open_account(f"Race A {tag}", sandbox=True, acc_id=f"race-{tag}-a")
        b = led.open_account(f"Race B {tag}", sandbox=True, acc_id=f"race-{tag}-b")
        led.deposit(a["id"], body.start_balance, "race demo funding")
        before = led.conflicts
        results: list[dict] = []
        lock = threading.Lock()
        gate = threading.Barrier(body.concurrent)

        def one(i):
            gate.wait()  # release every thread at the same instant
            t0 = time.perf_counter()
            try:
                led.transfer(a["id"], b["id"], body.amount, f"race transfer #{i + 1}", mode=body.mode,
                             think_time=body.think_ms / 1000)
                r = {"i": i + 1, "ok": True}
            except Rejected as e:
                r = {"i": i + 1, "ok": False, "reason": str(e)}
            r["ms"] = (time.perf_counter() - t0) * 1000
            with lock:
                results.append(r)

        threads = [threading.Thread(target=one, args=(i,)) for i in range(body.concurrent)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        fa, fb = led.account(a["id"]), led.account(b["id"])
        afford = int(body.start_balance // body.amount)
        return {
            "mode": body.mode, "succeeded": sum(r["ok"] for r in results), "rejected": sum(not r["ok"] for r in results),
            "can_afford": afford, "final_a": fa["balance_display"], "final_b": fb["balance_display"],
            "invariant_held": fa["balance"] >= 0, "optimistic_retries": led.conflicts - before,
            "results": sorted(results, key=lambda r: r["i"]), "accounts": [a["id"], b["id"]],
        }

    return await run_in_threadpool(go)


@app.post("/demo/seed")
async def seed(request: Request):
    """Fresh ledger + demo accounts with 30 days of back-dated history, so
    point-in-time queries and the audit trail have something to show.
    (Back-dating is only allowed before any newer history exists, so we reset first.)"""
    led = L(request)

    def go():
        led.reset()
        rnd = random.Random(7)
        names = [("Aarav", "customer", "INR", 0), ("Diya", "customer", "INR", 0), ("Kabir", "credit", "INR", 50000),
                 ("Meera", "customer", "USD", 0), ("Rohan", "customer", "INR", 0)]
        accs = {}
        for name, kind, cur, limit in names:
            accs[name] = led.open_account(name, kind, cur, limit)["id"]
        start = time.time() - 30 * 86400
        t = start
        led.deposit(accs["Aarav"], 85000, "Salary", at=t + 100)
        led.deposit(accs["Diya"], 62000, "Salary", at=t + 200)
        led.deposit(accs["Meera"], 1500, "Freelance payout", at=t + 300)
        led.deposit(accs["Rohan"], 15000, "Scholarship", at=t + 400)
        t += 3600
        events = 0
        while t < time.time() - 600:
            t += rnd.uniform(3, 14) * 3600
            if t >= time.time() - 600:
                break
            who = rnd.choice(list(accs))
            other = rnd.choice([n for n in accs if n != who])
            roll = rnd.random()
            try:
                if roll < 0.45:
                    led.transfer(accs[who], accs[other], rnd.choice([250, 499, 1200, 2000, 3500, 800]),
                                 rnd.choice(["Dinner split", "Rent share", "Movie tickets", "Books", "Trip fund",
                                             "Concert", "Groceries"]), at=t)
                elif roll < 0.75:
                    led.withdraw(accs[who], rnd.choice([500, 1000, 2000]), rnd.choice(["ATM", "UPI to shop", "Fuel"]),
                                 at=t)
                else:
                    led.deposit(accs[who], rnd.choice([1000, 5000, 12000]), rnd.choice(["Refund", "Gift", "Bonus"]),
                                at=t)
                events += 1
            except Rejected:
                pass
        return {"accounts": accs, "events": events, "from": start}

    return await run_in_threadpool(go)


@app.post("/reset")
async def reset(request: Request):
    await run_in_threadpool(L(request).reset)
    return {"ok": True}


@app.get("/health")
async def health():
    return {"ok": True}
