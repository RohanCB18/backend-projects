"""Integration tests on a real cluster of node processes (R1, R3, R4, R5, R6, R7).

Starts its own controller with nodes on ports 9301+ so it never collides with
a cluster you're running for the demo.
"""
import os
import socket
import subprocess
import sys
import time

import httpx
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def wait_until(pred, timeout=20.0, step=0.2):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(step)
    return False


@pytest.fixture(scope="module")
def ctl():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    env = {**os.environ, "CACHE_BASE_PORT": "9301", "CACHE_NODES": "5"}
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port),
                             "--log-level", "warning"], cwd=ROOT, env=env)
    c = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30)
    assert wait_until(lambda: c.get("/health").status_code == 200)
    # cluster formed: every node alive and sees all 5 in its ring
    assert wait_until(lambda: all(len(n.get("ring_nodes") or []) == 5 for n in c.get("/cluster").json()["nodes"]))
    yield c
    proc.kill()
    proc.wait()


def nodes(c):
    return {n["id"]: n for n in c.get("/cluster").json()["nodes"]}


def holders(c, key):
    out = []
    for nid, n in nodes(c).items():
        if n.get("process") == "running" and n.get("addr"):
            ks = httpx.get(f"http://{n['addr']}/internal/keys", params={"limit": 100000}).json()
            if any(k["key"] == key and not k["deleted"] for k in ks):
                out.append(nid)
    return sorted(out)


def test_r1_five_independent_processes_form_one_logical_cache(ctl):
    assert wait_until(lambda: all(n.get("status") == "alive" for n in nodes(ctl).values()))
    ns = nodes(ctl)
    assert len(ns) == 5
    assert len({n["addr"] for n in ns.values()}) == 5
    ctl.put("/kv/one-cache", json={"value": "same everywhere"})
    # any node can answer for any key
    for n in ns.values():
        r = httpx.get(f"http://{n['addr']}/kv/one-cache").json()
        assert r["value"] == "same everywhere"


def test_r3_every_key_lives_on_three_nodes_its_preference_list(ctl):
    put = ctl.put("/kv/replicated", json={"value": "v1"}).json()
    assert len(put["owners"]) == 3 and sorted(put["acked"]) == sorted(put["owners"])
    # exactly the preference list. (While ring views are still converging a
    # 4th node can briefly hold a spare copy; anti-entropy hands it off.)
    assert wait_until(lambda: holders(ctl, "replicated") == sorted(put["owners"]), timeout=15)


def test_r6_ttl_is_the_same_absolute_instant_on_every_replica(ctl):
    put = ctl.put("/kv/short-lived", json={"value": "bye soon", "ttl": 4}).json()
    ns = nodes(ctl)
    exps = set()
    for nid in put["owners"]:
        ks = httpx.get(f"http://{ns[nid]['addr']}/internal/keys", params={"limit": 100000}).json()
        exps |= {k["expires_at"] for k in ks if k["key"] == "short-lived"}
    assert exps == {put["expires_at"]}  # one absolute instant, stored identically on all 3 replicas
    assert ctl.get("/kv/short-lived").json()["found"]
    time.sleep(max(0.0, put["expires_at"] / 1000 - time.time()) + 0.3)
    assert not ctl.get("/kv/short-lived").json()["found"]
    assert holders(ctl, "short-lived") == []


def test_r3_r4_primary_dies_reads_still_succeed_and_failure_is_detected(ctl):
    put = ctl.put("/kv/survivor", json={"value": "still here"}).json()
    primary = put["owners"][0]
    ctl.post(f"/nodes/{primary}/kill")
    t0 = time.time()
    # immediately (before detection) reads still work via the replicas
    assert ctl.get("/kv/survivor").json()["value"] == "still here"
    # gossip marks it dead and removes it from every ring, with no manual step
    assert wait_until(lambda: all(primary not in (n.get("ring_nodes") or [primary])
                                  for n in nodes(ctl).values() if n.get("process") == "running"), timeout=10)
    detected_in = time.time() - t0
    assert detected_in < 5
    # self-healing: the key is back to 3 live copies
    assert wait_until(lambda: len(holders(ctl, "survivor")) == 3, timeout=15)
    ctl.post(f"/nodes/{primary}/restart")
    assert wait_until(lambda: all(len(n.get("ring_nodes") or []) == 5 for n in nodes(ctl).values()), timeout=20)


def test_r5_new_node_joins_takes_its_share_and_clients_never_notice(ctl):
    ctl.post("/seed", params={"count": 400})
    ctl.post("/traffic/start", json={"rps": 80, "keys": 200, "write_ratio": 0.3})
    time.sleep(1.5)
    new = ctl.post("/nodes").json()["started"]
    assert wait_until(lambda: all(len(n.get("ring_nodes") or []) == 6 for n in nodes(ctl).values()
                                  if n.get("process") == "running"), timeout=20)
    # it owns keys now (streamed during join + rebalance), roughly 1/6 of the primaries
    assert wait_until(lambda: nodes(ctl)[new]["keys"] > 50, timeout=15)
    time.sleep(2)
    report = ctl.post("/traffic/stop").json()
    assert report["failed"] == 0 and report["requests"] > 100
    # every seeded key is still readable
    missing = [i for i in range(0, 400, 7) if not ctl.get(f"/kv/user:{i}").json()["found"]]
    assert missing == []


@pytest.mark.parametrize("consistency", ["eventual", "quorum"])
def test_r7_chaos_kill_random_node_mid_traffic_zero_failed_requests(ctl, consistency):
    # make sure we start from a full healthy cluster
    for nid, n in nodes(ctl).items():
        if n.get("process") != "running":
            ctl.post(f"/nodes/{nid}/restart")
    assert wait_until(lambda: all(n.get("status") == "alive" for n in nodes(ctl).values()), timeout=20)
    time.sleep(1)
    ctl.post("/chaos", json={"duration": 10, "kill_at": 3, "rps": 120, "keys": 200, "consistency": consistency})
    assert wait_until(lambda: ctl.get("/chaos").json()["state"] != "running", timeout=40, step=0.5)
    res = ctl.get("/chaos").json()
    print(f"\n{consistency}: killed {res['victim']}: {res['traffic']} verify={res['verify']}")
    assert res["state"] == "done"
    assert res["traffic"]["failed"] == 0                 # zero failed client requests (one retry allowed)
    assert res["traffic"]["requests"] > 500
    assert res["verify"]["lost"] == 0                    # no acknowledged write lost
    if consistency == "quorum":
        assert res["traffic"]["stale_reads"] == 0 and res["verify"]["stale"] == 0
    ctl.post(f"/nodes/{res['victim']}/restart")
