# 02 · Reliable Job Queue with Dead-Letter Recovery (Postgres)

A background-job engine: durable jobs, priorities, delays, retries with
exponential backoff, a dead-letter queue, leases, and idempotent jobs.
Jobs live in Postgres (`docker compose up -d --wait` in the repo root; database
`jobqueue` on 127.0.0.1:15432, or set `JQ_DSN`).

```bash
..\.venv\Scripts\python -m uvicorn app.main:app --port 9102
```
```bash
..\.venv\Scripts\python -m app.worker --id my-worker
```
```bash
..\.venv\Scripts\python queuectl.py stats
```

The first command starts the API with 3 in-process worker threads. The second
adds an extra worker as a separate OS process. The CLI also has `inflight`,
`dlq`, `replay <id|all>`, `job <id>` and `enqueue <type>`.

## Lifecycle

```
queued --claim--> running --complete--> done
  ^                  | fail, attempts left: run_at = now + backoff
  +------------------+
  |                  | fail, out of attempts
  |  lease expired   +---------------------------> dead (DLQ) --replay--> queued
  +--(worker died)---+
```

## Design decisions

* **Durable.** Jobs are rows in Postgres. Kill the server mid-run and
  everything is still there; running jobs come back when their lease expires.
* **Atomic claim with `FOR UPDATE SKIP LOCKED`.**
  `UPDATE jobs SET status='running', lease_owner=%s, … WHERE id = (SELECT … ORDER BY
  priority DESC, run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING *`.
  `FOR UPDATE` row-locks the chosen job; `SKIP LOCKED` makes every other worker
  running the same query skip rows that are already locked instead of waiting.
  N workers claim N different jobs in parallel, and a job is never handed out
  twice. The lease reaper and `fail()` lock their rows the same way.
* **Lease + heartbeat.** Claiming sets `lease_until = now + 5 s`; long jobs renew
  it. A crashed worker stops renewing, the lease expires, and the next claim
  puts the job back in the queue.
* **Fencing.** `complete()`/`fail()` only count if the caller still holds the
  lease. A "zombie" that stalled past its lease can't overwrite the result of
  the worker that took over (its next heartbeat or its completion is rejected).
* **Backoff.** `base × 2^(attempt-1)`, capped, times a random factor in
  [0.8, 1.2] so failed jobs don't retry in lockstep (no retry storms).
* **Idempotency is the real lesson.** At-least-once delivery means a job *can*
  run twice. `charge_order` records the charge under a unique idempotency key
  (`INSERT … ON CONFLICT DO NOTHING`); `charge_order_naive` doesn't.
* **Stretch: dependencies.** `depends_on` makes a job claimable only once its
  parent is `done`.

## How each requirement is proven

| Req | Proof |
|---|---|
| R1 durable | `test_r1_jobs_survive_a_restart` |
| R2 priorities + delays | `test_r2_highest_priority_first_then_fifo`, `test_r2_delayed_job_is_invisible_until_due` |
| R3 backoff | `test_r3_backoff_doubles_with_jitter_and_is_capped`, `test_r3_failed_job_is_rescheduled_with_growing_delay` |
| R4 DLQ + replay | `test_r4_exhausted_job_goes_to_dlq_and_can_be_replayed` |
| R5 never twice at once | lease/fencing test, heartbeat test, 8 threads × 300 jobs and 4 processes × 400 jobs racing on the same rows (each claimed exactly once), and `test_r5_worker_process_killed_mid_job_is_recovered` (real `kill`) |
| R6 inspection | `GET /stats`, `/dlq`, `/dashboard`; `queuectl.py`; `test_r6_*` |
| R7 idempotency | `test_r7_rerun_after_crash_charges_once_only_when_idempotent` (idempotent: 1 charge, naive: 2) and the zombie-worker test |
| Stretch | `test_stretch_dependent_job_waits_for_its_parent` |

In the console, the scenario buttons stage each of these live, and process
workers can be killed with a real `kill -9` while they hold a job.
