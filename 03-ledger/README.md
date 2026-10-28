# 03 · Event-Sourced Ledger (Double-Entry Bank Core)

Money modelled the way banks do: an immutable stream of balanced events.
There is no balance column anywhere.

```bash
..\.venv\Scripts\python -m uvicorn app.main:app --port 9103
```
```bash
..\.venv\Scripts\python -m pytest -q
```

Try `POST /demo/seed` for 5 accounts with 30 days of back-dated history, then
`GET /accounts/{id}/audit?at=2026-10-20T18:00`.

## Model

* **transaction**: one business event ("Aarav paid Diya ₹200"), immutable.
* **entry**: one leg of it on one account, a signed amount. Every
  transaction's entries sum to zero *per currency*.
* **version**: per-account sequence of entries (1, 2, 3, …), `UNIQUE(account, version)`.
* Deposits come from an `world:INR` account and withdrawals go back to it, so
  money never appears from nothing. The whole system always sums to zero.
* Amounts are **integer paise**, never floats (0.1 + 0.2 ≠ 0.3).

## Design decisions

* **Append-only, enforced by the database.** SQL triggers reject every UPDATE
  and DELETE on `entries` and `transactions`; even a bug can't rewrite history.
* **Derived balances.** `balance = latest snapshot + sum(entries after it)`.
  Snapshots (every 5 entries) are just a cache, checked against full replays.
* **Point-in-time** = the same fold, limited to entries with `ts ≤ T`.
* **Atomic.** Validate first; then all legs go in one transaction or none do.
* **Optimistic concurrency.** Read each account's (version, balance) without
  locks, check the rules, then append only if every version is unchanged. If
  someone moved money in between, re-read and retry. A `pessimistic` mode (lock
  first) and a deliberately broken `naive` mode exist for the race demo.
* **Stretch: multi-currency.** A USD→INR transfer goes through FX pool accounts
  (`A −10 USD, fx:USD +10 USD, fx:INR −832.50 INR, B +832.50 INR`) so each
  currency still nets to zero. Also idempotency keys on transfers.

## How each requirement is proven

| Req | Proof |
|---|---|
| R1 append-only | `test_r1_history_cannot_be_rewritten` (UPDATE/DELETE refused), no balance column |
| R2 double entry | `test_r2_every_transaction_sums_to_zero`, `test_r2_unbalanced_transactions_are_refused` |
| R3 derived | `test_r3_balance_is_the_fold_of_events` |
| R4 point-in-time | `test_r4_balance_as_of_a_past_moment`, plus over HTTP with ISO dates |
| R5 atomic rejection | overdraft writes nothing; credit limits; a simulated crash mid-write leaves no half transaction |
| R6 audit trail | `test_r6_audit_reconstructs_and_explains_every_step` |
| R7 concurrency | `naive` mode **does** overdraw (the race is real); `optimistic` and `pessimistic` never do; random churn from 8 threads keeps every invariant; 4 OS processes can't overdraw |
| Stretch | snapshots match replay; cross-currency balances per currency; idempotent retries |

`GET /integrity` runs every check on the live ledger and actually tries to
rewrite history to prove the triggers refuse.
