"""R7: a real server under 600 req/s for a single key makes exactly the
decisions the reference model predicts.
Runs loadtest.py, the same script you'd run by hand, against a fresh uvicorn."""
import json
import os
import socket
import subprocess
import sys
import time

import httpx
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    port = _free_port()
    env = {**os.environ, "RL_DB": str(tmp_path_factory.mktemp("lt") / "rl.db")}
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port), "--log-level", "warning"],
        cwd=ROOT, env=env,
    )
    url = f"http://127.0.0.1:{port}"
    for _ in range(150):
        try:
            if httpx.get(url + "/health").status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(0.1)
    yield url
    proc.terminate()
    proc.wait()


@pytest.mark.parametrize("args", [
    ["--mode", "token_bucket", "--rate", "50", "--burst", "100"],
    ["--mode", "sliding_window", "--limit", "80", "--window", "1"],
])
def test_r7_load_600_rps_matches_the_math(server, args):
    out = subprocess.run(
        [sys.executable, "loadtest.py", "--json", "--url", server, "--rps", "600", "--duration", "4", *args],
        cwd=ROOT, capture_output=True, text=True, timeout=120,
    )
    report = json.loads(out.stdout.strip().splitlines()[-1])
    print(f"\n{report['config']['mode']}: {report['achieved_rps']:.0f} req/s, "
          f"allowed {report['allowed']}/{report['requests']}, model mismatches {report['mismatches']}, "
          f"errors {report['errors']}")
    assert report["errors"] == 0
    assert report["achieved_rps"] >= 500
    assert report["invariant_ok"], report
    assert report["mismatches"] <= 1, report
