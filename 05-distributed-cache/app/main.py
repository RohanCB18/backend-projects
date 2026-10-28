"""05 - Cluster controller (port 9105).

Not part of the cache itself: it plays the role of "operations" and "clients".
  * starts/kills/restarts real node processes (python -m app.node)
  * generates client traffic through CacheClient and measures it
  * runs the chaos test: traffic + random node kill, then verifies nothing was lost
  * aggregates every node's view of the cluster for the UI
"""
from __future__ import annotations

import asyncio
import os
import random
import subprocess
import sys
import time
from collections import deque
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .client import CacheClient, RequestFailed
from .ring import Ring, moved_fraction

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_PORT = int(os.getenv("CACHE_BASE_PORT", "9201"))
INITIAL_NODES = int(os.getenv("CACHE_NODES", "5"))
MAX_NODES = 8


class Cluster:
    def __init__(self):
        self.nodes: dict[str, dict] = {}  # id -> {port, proc, started}
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(2.0, connect=0.3))
        self.log: deque = deque(maxlen=80)

    def addr(self, nid: str) -> str:
        return f"127.0.0.1:{self.nodes[nid]['port']}"

    def running(self) -> list[str]:
        return [n for n, v in self.nodes.items() if v["proc"] and v["proc"].poll() is None]

    def note(self, msg: str) -> None:
        self.log.append({"ts": time.time(), "node": "controller", "kind": "ops", "msg": msg})

    def start(self, nid: str | None = None) -> str:
        if nid is None:
            if len(self.nodes) >= MAX_NODES:
                raise HTTPException(400, f"max {MAX_NODES} nodes")
            nid = f"n{len(self.nodes) + 1}"
            self.nodes[nid] = {"port": BASE_PORT + len(self.nodes), "proc": None}
        info = self.nodes[nid]
        if info["proc"] and info["proc"].poll() is None:
            raise HTTPException(400, f"{nid} is already running")
        seeds = ",".join(self.addr(n) for n in self.nodes if n != nid)
        info["proc"] = subprocess.Popen(
            [sys.executable, "-m", "app.node", "--id", nid, "--port", str(info["port"]), "--seeds", seeds,
             "--parent-pid", str(os.getpid())], cwd=HERE)
        info["started"] = time.time()
        self.note(f"started {nid} on port {info['port']} (pid {info['proc'].pid})")
        return nid

    def kill(self, nid: str) -> None:
        info = self.nodes.get(nid)
        if not info or not info["proc"] or info["proc"].poll() is not None:
            raise HTTPException(400, f"{nid} is not running")
        info["proc"].kill()  # TerminateProcess / SIGKILL: no goodbye, no handoff
        info["proc"].wait()
        self.note(f"KILLED {nid} (pid {info['proc'].pid})")

    async def status(self) -> list[dict]:
        async def one(nid):
            base = {"id": nid, "port": self.nodes[nid]["port"],
                    "process": "running" if nid in self.running() else "killed"}
            if base["process"] != "running":
                return base
            try:
                r = await self.http.get(f"http://{self.addr(nid)}/internal/status")
                return {**base, **r.json()}
            except Exception:
                return {**base, "status": "starting"}
        return await asyncio.gather(*(one(n) for n in self.nodes))


class Traffic:
    """Client traffic with per-key verification.

    Each key's value carries a counter ("k17:42"). At most one request per key
    is in flight, so for every read we know the newest value the cluster had
    acknowledged before the read started. Reading anything older is a stale read.
    """

    def __init__(self, cluster: Cluster):
        self.cluster = cluster
        self.task: asyncio.Task | None = None
        self.cfg: dict = {}
        self.reset_stats()

    def reset_stats(self) -> None:
        self.started = None
        self.counts = {"requests": 0, "ok": 0, "ok_after_retry": 0, "failed": 0, "stale_reads": 0,
                       "reads": 0, "writes": 0, "misses": 0}
        self.latencies: deque = deque(maxlen=3000)
        self.series: dict[int, dict] = {}
        self.failures: deque = deque(maxlen=20)
        self.acked: dict[str, int] = {}
        self.next_val: dict[str, int] = {}

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def start(self, rps: int, keys: int, write_ratio: float, consistency: str, concurrency: int | None = None) -> None:
        concurrency = concurrency or max(16, rps // 3)  # enough workers that latency doesn't cap the rate
        if self.running:
            self.task.cancel()
        self.reset_stats()
        self.cfg = {"rps": rps, "keys": keys, "write_ratio": write_ratio, "consistency": consistency}
        self.task = asyncio.create_task(self._run(rps, keys, write_ratio, consistency, concurrency))

    async def stop(self) -> None:
        if self.running:
            self.task.cancel()
            try:
                await self.task
            except (asyncio.CancelledError, Exception):
                pass

    def _tick(self, kind: str) -> None:
        sec = int(time.time())
        row = self.series.setdefault(sec, {"t": sec, "ok": 0, "retried": 0, "failed": 0})
        row[kind] += 1
        for old in [s for s in self.series if s < sec - 90]:
            del self.series[old]

    async def _run(self, rps, keys, write_ratio, consistency, concurrency) -> None:
        client = CacheClient([self.cluster.addr(n) for n in self.cluster.running()])
        locks: dict[str, asyncio.Lock] = {}
        self.started = time.time()
        interval = concurrency / rps

        async def worker(wid):
            await asyncio.sleep(random.random() * interval)
            while True:
                t_start = time.perf_counter()
                key = f"k{random.randrange(keys)}"
                lock = locks.setdefault(key, asyncio.Lock())
                async with lock:
                    await self.one(client, key, write_ratio, consistency)
                await asyncio.sleep(max(0.0, interval - (time.perf_counter() - t_start)))

        workers = [asyncio.create_task(worker(i)) for i in range(concurrency)]
        try:
            await asyncio.gather(*workers)
        finally:
            for w in workers:
                w.cancel()
            await client.close()

    async def one(self, client: CacheClient, key: str, write_ratio: float, consistency: str) -> None:
        self.counts["requests"] += 1
        t0 = time.perf_counter()
        is_write = key not in self.acked or random.random() < write_ratio
        try:
            if is_write:
                self.counts["writes"] += 1
                n = self.next_val.get(key, 0) + 1
                self.next_val[key] = n
                _, attempts, _ = await client.put(key, f"{key}:{n}", consistency=consistency)
                self.acked[key] = n
            else:
                self.counts["reads"] += 1
                expected = self.acked.get(key, 0)
                res, attempts, _ = await client.get(key, consistency=consistency)
                if not res["found"]:
                    self.counts["misses"] += 1
                    if expected:
                        self.counts["stale_reads"] += 1
                elif int(res["value"].split(":")[1]) < expected:
                    self.counts["stale_reads"] += 1
            self.latencies.append((time.perf_counter() - t0) * 1000)
            self.counts["ok"] += 1
            if attempts > 1:
                self.counts["ok_after_retry"] += 1
                self._tick("retried")
            else:
                self._tick("ok")
        except RequestFailed as e:
            self.counts["failed"] += 1
            self.failures.append({"ts": time.time(), "key": key, "error": str(e)[:300]})
            self._tick("failed")

    def report(self) -> dict:
        lat = sorted(self.latencies)
        pct = (lambda p: round(lat[min(len(lat) - 1, int(p * len(lat)))], 1)) if lat else (lambda p: None)
        now = int(time.time())
        return {
            "running": self.running, "config": self.cfg, "elapsed": time.time() - self.started if self.started else 0,
            **self.counts, "p50_ms": pct(0.5), "p99_ms": pct(0.99), "keys_written": len(self.acked),
            "series": [self.series.get(s, {"t": s, "ok": 0, "retried": 0, "failed": 0}) for s in range(now - 59, now + 1)],
            "recent_failures": list(self.failures)[-5:],
        }

    async def verify_all(self, consistency: str = "quorum") -> dict:
        """After traffic: is every acknowledged write still readable at its latest value?"""
        client = CacheClient([self.cluster.addr(n) for n in self.cluster.running()])
        lost, stale, checked = [], [], 0
        sem = asyncio.Semaphore(20)

        async def check(key, n):
            nonlocal checked
            async with sem:
                try:
                    res, _, _ = await client.get(key, consistency=consistency)
                except RequestFailed:
                    lost.append(key)
                    return
                checked += 1
                if not res["found"]:
                    lost.append(key)
                elif int(res["value"].split(":")[1]) < n:
                    stale.append(key)

        await asyncio.gather(*(check(k, n) for k, n in self.acked.items()))
        await client.close()
        return {"checked": checked, "lost": len(lost), "stale": len(stale), "lost_keys": lost[:10]}


@asynccontextmanager
async def lifespan(app: FastAPI):
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.winmm.timeBeginPeriod(1)
    cluster = Cluster()
    for _ in range(INITIAL_NODES):
        cluster.start()
    app.state.cluster = cluster
    app.state.traffic = Traffic(cluster)
    app.state.chaos = {"state": "idle"}
    app.state.kv = None
    yield
    await app.state.traffic.stop()
    for info in cluster.nodes.values():
        if info["proc"] and info["proc"].poll() is None:
            info["proc"].kill()


app = FastAPI(title="05 Distributed Cache - controller", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def C(request: Request) -> Cluster:
    return request.app.state.cluster


@app.get("/cluster")
async def cluster_view(request: Request):
    cl = C(request)
    nodes = await cl.status()
    ring_nodes = sorted({n for s in nodes if s.get("ring_nodes") for n in s["ring_nodes"]} & set(cl.running()))
    ring = Ring(ring_nodes)
    events = sorted([e for s in nodes for e in s.get("events", [])] + list(cl.log), key=lambda e: e["ts"])[-40:]
    return {"nodes": nodes, "ring": {"nodes": ring_nodes, "points": ring.points()}, "events": events,
            "traffic": request.app.state.traffic.report(), "chaos": request.app.state.chaos}


@app.post("/nodes")
async def add_node(request: Request):
    return {"started": C(request).start()}


@app.post("/nodes/{nid}/kill")
async def kill_node(nid: str, request: Request):
    C(request).kill(nid)
    return {"killed": nid}


@app.post("/nodes/{nid}/restart")
async def restart_node(nid: str, request: Request):
    return {"started": C(request).start(nid)}


@app.post("/nodes/kill-random")
async def kill_random(request: Request):
    running = C(request).running()
    if len(running) <= 1:
        raise HTTPException(400, "need at least 2 running nodes")
    victim = random.choice(running)
    C(request).kill(victim)
    return {"killed": victim}


class TrafficIn(BaseModel):
    rps: int = Field(150, ge=1, le=1000)
    keys: int = Field(300, ge=1, le=20000)
    write_ratio: float = Field(0.3, ge=0, le=1)
    consistency: str = "eventual"


@app.post("/traffic/start")
async def traffic_start(body: TrafficIn, request: Request):
    request.app.state.traffic.start(body.rps, body.keys, body.write_ratio, body.consistency)
    return {"ok": True}


@app.post("/traffic/stop")
async def traffic_stop(request: Request):
    await request.app.state.traffic.stop()
    return request.app.state.traffic.report()


@app.get("/traffic")
async def traffic(request: Request):
    return request.app.state.traffic.report()


class ChaosIn(BaseModel):
    duration: float = Field(14, ge=4, le=60)
    kill_at: float = Field(4, ge=1)
    rps: int = 150
    keys: int = 300
    consistency: str = "eventual"


@app.post("/chaos")
async def chaos(body: ChaosIn, request: Request):
    """R7: live traffic, kill a random node mid-flight, then verify."""
    st = request.app.state
    if st.chaos.get("state") == "running":
        raise HTTPException(409, "chaos test already running")
    st.chaos = {"state": "running", "started": time.time(), "config": body.model_dump()}

    async def run():
        cl, tr = st.cluster, st.traffic
        try:
            tr.start(body.rps, body.keys, 0.4, body.consistency)
            await asyncio.sleep(body.kill_at)
            running = cl.running()
            victim = random.choice(running)
            cl.kill(victim)
            st.chaos["victim"] = victim
            st.chaos["killed_at"] = time.time()
            await asyncio.sleep(max(0.0, body.duration - body.kill_at))
            await tr.stop()
            await asyncio.sleep(2.5)  # let gossip + re-replication settle
            verify = await tr.verify_all("quorum")
            rep = tr.report()
            st.chaos.update(state="done", traffic={k: rep[k] for k in (
                "requests", "ok", "ok_after_retry", "failed", "stale_reads", "p50_ms", "p99_ms", "keys_written")},
                verify=verify, passed=rep["failed"] == 0 and verify["lost"] == 0)
        except Exception as e:
            st.chaos.update(state="error", error=repr(e))

    asyncio.create_task(run())
    return st.chaos


@app.get("/chaos")
async def chaos_status(request: Request):
    return request.app.state.chaos


# ---- manual key operations for the UI's "try it" panel ----------------------------------
async def _client(request: Request) -> CacheClient:
    if request.app.state.kv is None:
        request.app.state.kv = CacheClient([C(request).addr(n) for n in C(request).running()])
    return request.app.state.kv


class KVIn(BaseModel):
    value: str
    ttl: float | None = None
    consistency: str = "eventual"


@app.get("/kv/{key}")
async def kv_get(key: str, request: Request, consistency: str = "eventual"):
    try:
        res, attempts, tried = await (await _client(request)).get(key, consistency)
    except RequestFailed as e:
        raise HTTPException(503, str(e))
    return {**res, "attempts": attempts, "entry_nodes": tried}


@app.put("/kv/{key}")
async def kv_put(key: str, body: KVIn, request: Request):
    try:
        res, attempts, tried = await (await _client(request)).put(key, body.value, body.ttl, body.consistency)
    except RequestFailed as e:
        raise HTTPException(503, str(e))
    return {**res, "attempts": attempts, "entry_nodes": tried}


@app.post("/seed")
async def seed(request: Request, count: int = 500):
    client = await _client(request)
    sem = asyncio.Semaphore(20)

    async def one(i):
        async with sem:
            await client.put(f"user:{i}", f"profile-{i}")

    await asyncio.gather(*(one(i) for i in range(count)))
    return {"written": count}


@app.get("/ring/locate/{key}")
async def locate(key: str, request: Request):
    """Where a key sits on the ring and which nodes own it (for the UI)."""
    from .node import N_REPLICAS
    from .ring import h
    cl = C(request)
    nodes = await cl.status()
    ring_nodes = sorted({n for s in nodes if s.get("ring_nodes") for n in s["ring_nodes"]} & set(cl.running()))
    return {"key": key, "pos": h(key) / 2 ** 64, "preference": Ring(ring_nodes).preference(key, N_REPLICAS)}


@app.get("/ring/compare")
async def compare(before: int = 4, after: int = 5):
    return moved_fraction([f"n{i + 1}" for i in range(before)], [f"n{i + 1}" for i in range(after)])


@app.get("/health")
async def health():
    return {"ok": True}
