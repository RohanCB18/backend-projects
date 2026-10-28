"""Requirement 7 from the command line.

    python loadtest.py --rps 600 --duration 5
    python loadtest.py --mode sliding_window --limit 80 --window 1
    python loadtest.py --json          (machine-readable; used by the /loadtest endpoint)

Needs the limiter running (see README). Configures the client key, fires the
load from several processes, then checks the ALLOW count against the math.
"""
import argparse
import json
import sys

import httpx

from app.loadtest_core import run_load, verify


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://127.0.0.1:9101")
    p.add_argument("--key", default="loadtest")
    p.add_argument("--rps", type=int, default=600)
    p.add_argument("--duration", type=float, default=5)
    p.add_argument("--procs", type=int, default=1)
    p.add_argument("--mode", default="token_bucket", choices=["token_bucket", "sliding_window"])
    p.add_argument("--rate", type=float, default=50)
    p.add_argument("--burst", type=int, default=100)
    p.add_argument("--limit", type=int, default=80)
    p.add_argument("--window", type=float, default=1.0)
    p.add_argument("--json", action="store_true")
    a = p.parse_args()

    cfg = {"mode": a.mode, "rate": a.rate, "burst": a.burst, "limit": a.limit, "window": a.window}
    cfg = httpx.put(f"{a.url}/admin/clients/{a.key}", json=cfg).json()["config"]
    run = run_load(a.url, a.key, a.rps, a.duration, a.procs)
    report = {**{k: v for k, v in run.items() if k != "results"}, **verify(run["results"], cfg), "config": cfg}

    if a.json:
        print(json.dumps(report))
    else:
        print(f"mode            {cfg['mode']}")
        print(f"sent            {report['sent']} requests, {report['errors']} errors {report['error_types'] or ''}")
        print(f"achieved rate   {report['achieved_rps']:.0f} req/s (server clock)")
        print(f"allowed/denied  {report['allowed']} / {report['denied']}")
        print(f"invariant       {report['rule']}  -> {'ok' if report['invariant_ok'] else 'VIOLATED'}")
        if "max_in_any_window" in report:
            print(f"worst window    {report['max_in_any_window']} ALLOWs (limit {report['limit']})")
        print(f"replay model    {report['model_allowed']} ALLOWs; {report['mismatches']} decisions differ from the model")
        print("RESULT          " + ("PASS" if report["ok"] and report["errors"] == 0 else "FAIL"))
    return 0 if report["ok"] and report["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
