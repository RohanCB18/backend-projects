"""Command-line inspection for the job queue (requirement 6).

    python queuectl.py stats
    python queuectl.py inflight
    python queuectl.py dlq
    python queuectl.py replay 17        (or: replay all)
    python queuectl.py job 17
    python queuectl.py enqueue send_email --priority 5 --delay 10
"""
import argparse
import datetime as dt
import json
import sys

import httpx


def fmt_ts(ts):
    return dt.datetime.fromtimestamp(ts).strftime("%H:%M:%S") if ts else "-"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://127.0.0.1:9102")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("stats")
    sub.add_parser("inflight")
    sub.add_parser("dlq")
    r = sub.add_parser("replay")
    r.add_argument("job")
    j = sub.add_parser("job")
    j.add_argument("job", type=int)
    e = sub.add_parser("enqueue")
    e.add_argument("type")
    e.add_argument("--priority", type=int, default=0)
    e.add_argument("--delay", type=float, default=0)
    e.add_argument("--payload", default="{}")
    a = p.parse_args()
    c = httpx.Client(base_url=a.url, timeout=10)

    if a.cmd == "stats":
        s = c.get("/stats").json()
        for k in ("depth", "ready", "scheduled", "waiting_on_dependency", "running", "done", "dead"):
            print(f"{k:<22}{s[k]}")
    elif a.cmd == "inflight":
        for j in c.get("/stats").json()["in_flight"]:
            print(f"#{j['id']:<5} {j['type']:<20} {j['lease_owner']:<14} attempt {j['attempts']}/{j['max_attempts']}"
                  f"  lease {j['lease_remaining']:5.1f}s left")
    elif a.cmd == "dlq":
        for j in c.get("/dlq").json():
            print(f"#{j['id']:<5} {j['type']:<20} attempts {j['attempts']}  died {fmt_ts(j['finished_at'])}  "
                  f"{j['last_error']}")
    elif a.cmd == "replay":
        r = c.post("/dlq/replay-all") if a.job == "all" else c.post(f"/dlq/{a.job}/replay")
        print(r.json())
    elif a.cmd == "job":
        j = c.get(f"/jobs/{a.job}").json()
        print(json.dumps({k: v for k, v in j.items() if k != "events"}, indent=2))
        for ev in j["events"]:
            print(f"  {fmt_ts(ev['ts'])}  {ev['kind']:<24} {ev['worker'] or '':<12} {ev['detail'] or ''}")
    elif a.cmd == "enqueue":
        print(c.post("/jobs", json={"type": a.type, "priority": a.priority, "delay": a.delay,
                                    "payload": json.loads(a.payload)}).json())
    return 0


if __name__ == "__main__":
    sys.exit(main())
