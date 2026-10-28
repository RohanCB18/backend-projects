"""One cache node. Run several (the controller does this for you):

    python -m app.node --id n1 --port 9201 --seeds 127.0.0.1:9202,127.0.0.1:9203

Every node can serve any client request: it acts as the *coordinator* for that
request and talks to the nodes that own the key. Pieces, all built here:

  storage      in-memory dict with LRU eviction, absolute-time TTLs and a
               version per value (last-write-wins by version)
  membership   gossip: every 300 ms each node bumps its own heartbeat and swaps
               membership tables with 3 random peers. A peer whose heartbeat
               stops increasing is suspect after 1 s and dead after 2 s (R4).
  routing      consistent-hash ring over the nodes currently alive; each key is
               stored on N=3 nodes (its preference list) (R2, R3)
  self-healing whenever the alive set changes, each node re-checks which keys
               it should push where: new owners get copies, keys this node no
               longer owns are handed off and then dropped (R5). A periodic full
               anti-entropy pass repairs anything a push missed.
  joining      a new or restarted node starts as JOINING (not in the ring),
               streams the key range it is about to own from the others, then
               flips to ALIVE. Clients keep being served throughout (R5).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys
import time
from collections import OrderedDict, deque
from contextlib import asynccontextmanager

import aiohttp
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from .ring import Ring

N_REPLICAS = int(os.getenv("CACHE_REPLICAS", "3"))   # copies of each key
GOSSIP_EVERY = float(os.getenv("CACHE_GOSSIP_EVERY", "0.3"))
SUSPECT_AFTER = float(os.getenv("CACHE_SUSPECT_AFTER", "1.0"))
DEAD_AFTER = float(os.getenv("CACHE_DEAD_AFTER", "2.0"))
FULL_SYNC_EVERY = 8.0
TOMBSTONE_TTL_MS = 60_000


def now_ms() -> int:
    return int(time.time() * 1000)


class LocalStore:
    """key -> {"v": value, "ver": [ms, counter, node], "exp": abs ms | None, "del": bool}"""

    def __init__(self, max_keys: int):
        self.data: OrderedDict[str, dict] = OrderedDict()
        self.max_keys = max_keys
        self.evictions = 0
        self.expired = 0

    def get(self, key: str) -> dict | None:
        item = self.data.get(key)
        if item is None:
            return None
        if item["exp"] is not None and item["exp"] <= now_ms():
            del self.data[key]
            self.expired += 1
            return None
        self.data.move_to_end(key)  # LRU: most recently used goes to the end
        return item

    def merge(self, key: str, item: dict) -> bool:
        """Keep whichever version is newer. Same rule on every replica, so all
        replicas agree no matter in which order writes reach them."""
        if item["exp"] is not None and item["exp"] <= now_ms():
            return False  # already expired: absolute expiry means it's expired everywhere
        cur = self.data.get(key)
        if cur is not None and list(cur["ver"]) >= list(item["ver"]):
            return False
        self.data[key] = item
        self.data.move_to_end(key)
        while len(self.data) > self.max_keys:  # stretch goal: LRU eviction under memory pressure
            self.data.popitem(last=False)
            self.evictions += 1
        return True

    def sweep(self) -> None:
        t = now_ms()
        for k in [k for k, v in self.data.items() if v["exp"] is not None and v["exp"] <= t]:
            del self.data[k]
            self.expired += 1

    def live_count(self) -> int:
        return sum(1 for v in self.data.values() if not v["del"])


class Node:
    def __init__(self, node_id: str, addr: str, seeds: list[str], max_keys: int, parent_pid: int | None):
        self.id = node_id
        self.addr = addr
        self.seeds = [s for s in seeds if s and s != addr]
        self.store = LocalStore(max_keys)
        self.parent_pid = parent_pid
        self.incarnation = now_ms()       # a restarted node is a new incarnation
        self.heartbeat = 0
        self.status = "joining"
        self.members: dict[str, dict] = {}  # id -> {id, addr, inc, hb, status, seen}
        self._ring_nodes: list[str] = []
        self._ring = Ring([])
        self.epoch = 0
        self.synced_epoch = -1
        self.last_full_sync = 0.0
        self.owner_cache: dict[str, tuple] = {}
        self.counter = 0
        self.events: deque = deque(maxlen=60)
        self.stats = {k: 0 for k in ("gets", "puts", "deletes", "hits", "misses", "forwards", "forward_errors",
                                     "replica_writes", "handoff_keys_out", "handoff_keys_in", "rebalance_pushed",
                                     "read_repairs")}
        self.http: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------ helpers
    def log(self, kind: str, msg: str) -> None:
        self.events.append({"ts": time.time(), "node": self.id, "kind": kind, "msg": msg})

    def me(self) -> dict:
        return {"id": self.id, "addr": self.addr, "inc": self.incarnation, "hb": self.heartbeat, "status": self.status}

    def addr_of(self, node_id: str) -> str | None:
        return self.addr if node_id == self.id else self.members.get(node_id, {}).get("addr")

    def liveness(self, m: dict) -> str:
        age = time.monotonic() - m["seen"]
        if age > DEAD_AFTER:
            return "dead"
        if age > SUSPECT_AFTER:
            return "suspect"
        return m["status"]  # alive | joining

    def alive_ids(self) -> list[str]:
        ids = [m["id"] for m in self.members.values() if self.liveness(m) in ("alive", "suspect")
               and m["status"] == "alive"]
        if self.status == "alive":
            ids.append(self.id)
        return sorted(ids)

    def ring(self) -> Ring:
        ids = self.alive_ids()
        if ids != self._ring_nodes:
            gone = set(self._ring_nodes) - set(ids)
            came = set(ids) - set(self._ring_nodes)
            if gone:
                self.log("membership", f"removed from ring: {', '.join(sorted(gone))}")
            if came and self._ring_nodes:
                self.log("membership", f"added to ring: {', '.join(sorted(came))}")
            self._ring_nodes = ids
            self._ring = Ring(ids)
            self.epoch += 1
        return self._ring

    def next_version(self) -> list:
        self.counter += 1
        return [now_ms(), self.counter, self.id]

    async def rpc(self, node_id: str, path: str, body: dict) -> dict:
        addr = self.addr_of(node_id)
        if addr is None:
            raise ConnectionError(f"unknown node {node_id}")
        async with self.http.post(f"http://{addr}{path}", json=body) as r:
            r.raise_for_status()
            return await r.json()

    # ------------------------------------------------------------------ gossip (R4)
    def merge_members(self, incoming: list[dict]) -> None:
        for m in incoming:
            if m["id"] == self.id:
                continue
            cur = self.members.get(m["id"])
            if cur is None or (m["inc"], m["hb"]) > (cur["inc"], cur["hb"]):
                was = self.liveness(cur) if cur else None
                self.members[m["id"]] = {**{k: m[k] for k in ("id", "addr", "inc", "hb", "status")},
                                         "seen": time.monotonic()}
                if cur is None or (was == "dead") or cur["inc"] != m["inc"]:
                    self.log("gossip", f"{m['id']} is {m['status']}"
                             + (" (new incarnation)" if cur and cur["inc"] != m["inc"] else ""))

    def member_list(self) -> list[dict]:
        out = [self.me()]
        for m in self.members.values():
            if self.liveness(m) != "dead":
                out.append({k: m[k] for k in ("id", "addr", "inc", "hb", "status")})
        return out

    async def gossip_once(self) -> None:
        self.heartbeat += 1
        peers = [m for m in self.members.values() if self.liveness(m) != "dead"]
        targets = [p["addr"] for p in random.sample(peers, min(3, len(peers)))]
        # also poke a seed or a dead node now and then so partitions heal and returns are noticed
        extra = self.seeds + [m["addr"] for m in self.members.values() if self.liveness(m) == "dead"]
        if extra and (not targets or random.random() < 0.3):
            targets.append(random.choice(extra))

        async def one(addr):
            try:
                async with self.http.post(f"http://{addr}/internal/gossip", json={"members": self.member_list()},
                                          timeout=aiohttp.ClientTimeout(total=0.6, sock_connect=0.3)) as r:
                    self.merge_members((await r.json())["members"])
            except Exception:
                pass

        await asyncio.gather(*(one(a) for a in set(targets)))
        was_dead = {m["id"] for m in self.members.values() if m.get("_dead_logged")}
        for m in self.members.values():
            dead = self.liveness(m) == "dead"
            if dead and m["id"] not in was_dead:
                m["_dead_logged"] = True
                self.log("failure", f"{m['id']} missed heartbeats for {DEAD_AFTER:g}s: marked DEAD")
            elif not dead and m.get("_dead_logged"):
                m["_dead_logged"] = False
        self.ring()

    async def gossip_loop(self) -> None:
        while True:
            await self.gossip_once()
            await asyncio.sleep(GOSSIP_EVERY)

    # ------------------------------------------------------------------ joining (R5)
    async def bootstrap(self) -> None:
        for _ in range(8):  # learn the cluster from the seeds
            await self.gossip_once()
            if any(m["status"] == "alive" and self.liveness(m) == "alive" for m in self.members.values()):
                break
            await asyncio.sleep(0.2)
        peers = [m["id"] for m in self.members.values() if m["status"] == "alive" and self.liveness(m) == "alive"]
        got = 0
        for p in peers:
            try:
                items = (await self.rpc(p, "/internal/handoff", {"joiner": self.id}))["items"]
                for k, item in items.items():
                    got += self.store.merge(k, item)
            except Exception:
                pass
        self.stats["handoff_keys_in"] += got
        self.status = "alive"
        self.heartbeat += 1
        self.log("join", f"streamed {got} keys from {len(peers)} peers, now ALIVE" if peers
                 else "first node(s) up, ALIVE")
        await self.gossip_once()

    def handoff_for(self, joiner: str) -> dict:
        future = Ring(self.alive_ids() + [joiner])
        out = {k: v for k, v in list(self.store.data.items())
               if joiner in future.preference(k, N_REPLICAS) and (v["exp"] is None or v["exp"] > now_ms())}
        self.stats["handoff_keys_out"] += len(out)
        if out:
            self.log("handoff", f"streamed {len(out)} keys to joining node {joiner}")
        return out

    # ------------------------------------------------------------------ rebalancing / anti-entropy (R3, R5)
    async def rebalance_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            if self.status != "alive":
                continue
            try:
                await self.rebalance()
            except Exception as e:
                self.log("error", f"rebalance failed: {e!r}")

    async def rebalance(self) -> None:
        ring = self.ring()
        full = time.time() - self.last_full_sync > FULL_SYNC_EVERY
        if self.epoch == self.synced_epoch and not full:
            return
        epoch = self.epoch
        targets: dict[str, dict] = {}
        drop: list[str] = []
        new_cache = {}
        for key, item in list(self.store.data.items()):
            owners = ring.preference(key, N_REPLICAS)
            prev = self.owner_cache.get(key)
            for o in owners:
                if o != self.id and (full or prev is None or o not in prev):
                    targets.setdefault(o, {})[key] = item
            if self.id not in owners:
                drop.append(key)
            new_cache[key] = tuple(owners)

        failed: set[str] = set()

        async def push(node_id, items):
            try:
                await self.rpc(node_id, "/internal/replicate", {"items": items})
                return len(items)
            except Exception:
                failed.add(node_id)
                return 0

        pushed = sum(await asyncio.gather(*(push(n, it) for n, it in targets.items())))
        self.stats["rebalance_pushed"] += pushed
        dropped = 0
        for key in drop:
            owners = new_cache[key]
            if not failed.intersection(owners) and key in self.store.data:
                del self.store.data[key]  # handed off to its real owners; no longer ours
                new_cache.pop(key, None)
                dropped += 1
        if (pushed or dropped) and not full:
            self.log("rebalance", f"ring changed: pushed {pushed} copies, handed off {dropped} keys")
        elif pushed and full:
            self.log("anti-entropy", f"full sync pushed {pushed} copies")
        self.owner_cache = new_cache
        if not failed:
            self.synced_epoch = epoch
            if full:
                self.last_full_sync = time.time()

    async def sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            self.store.sweep()
            if self.parent_pid and not parent_alive(self.parent_pid):
                os._exit(0)  # controller is gone: don't linger holding the port

    # ------------------------------------------------------------------ coordinating client requests
    async def fetch(self, node_id: str, key: str) -> dict | None:
        if node_id == self.id:
            return self.store.get(key)
        self.stats["forwards"] += 1
        return (await self.rpc(node_id, "/internal/get", {"key": key}))["item"]

    async def store_on(self, node_id: str, key: str, item: dict) -> bool:
        try:
            if node_id == self.id:
                self.store.merge(key, item)
            else:
                self.stats["forwards"] += 1
                await self.rpc(node_id, "/internal/put", {"key": key, "item": item})
            return True
        except Exception:
            self.stats["forward_errors"] += 1
            return False

    async def coord_get(self, key: str, consistency: str) -> dict:
        self.stats["gets"] += 1
        ring = self.ring()
        pref = ring.preference(key, N_REPLICAS + 2)
        trace = []
        if consistency == "quorum":
            owners = pref[:N_REPLICAS]
            need = min(2, len(owners))  # R = 2

            async def ask(o):
                try:
                    return o, await self.fetch(o, key), None
                except Exception as e:
                    self.stats["forward_errors"] += 1
                    return o, None, e

            replies = await asyncio.gather(*(ask(o) for o in owners))
            ok = [(o, it) for o, it, err in replies if err is None]
            trace = [{"node": o, "result": "error" if err else ("hit" if it else "miss")} for o, it, err in replies]
            if len(ok) < need:
                raise HTTPException(503, f"quorum not reached: {len(ok)}/{need} replicas answered")
            items = [it for _, it in ok if it]
            newest = max(items, key=lambda it: list(it["ver"])) if items else None
            if newest:
                stale = [o for o, it in ok if not it or list(it["ver"]) < list(newest["ver"])]
                if stale:
                    self.stats["read_repairs"] += len(stale)
                    asyncio.create_task(self._repair(stale, key, newest))
            return self._answer(key, newest, trace)
        for o in pref:  # eventual: first copy found, owners first then the next nodes on the ring
            try:
                it = await self.fetch(o, key)
            except Exception:
                self.stats["forward_errors"] += 1
                trace.append({"node": o, "result": "error"})
                continue
            trace.append({"node": o, "result": "hit" if it else "miss"})
            if it:
                return self._answer(key, it, trace)
        return self._answer(key, None, trace)

    async def _repair(self, nodes, key, item):
        await asyncio.gather(*(self.store_on(n, key, item) for n in nodes))

    def _answer(self, key, item, trace) -> dict:
        found = item is not None and not item["del"]
        self.stats["hits" if found else "misses"] += 1
        return {"key": key, "found": found, "value": item["v"] if found else None,
                "version": item["ver"] if item else None, "expires_at": item["exp"] if found else None,
                "coordinator": self.id, "trace": trace}

    async def coord_put(self, key: str, value, ttl: float | None, consistency: str, delete: bool = False) -> dict:
        self.stats["deletes" if delete else "puts"] += 1
        exp = now_ms() + (TOMBSTONE_TTL_MS if delete else int(ttl * 1000)) if (ttl or delete) else None
        # TTL is turned into an ABSOLUTE expiry time once, here. Replicas that
        # receive the write later still expire it at the same instant (R6).
        item = {"v": None if delete else value, "ver": self.next_version(), "exp": exp, "del": delete}
        ring = self.ring()
        pref = ring.preference(key, N_REPLICAS + 2)
        owners = pref[:N_REPLICAS]
        need = min(2 if consistency == "quorum" else 1, len(owners))  # W
        results = await asyncio.gather(*(self.store_on(o, key, item) for o in owners))
        acked = [o for o, ok in zip(owners, results) if ok]
        for extra in pref[N_REPLICAS:]:  # sloppy quorum: borrow the next node on the ring
            if len(acked) >= need:
                break
            if await self.store_on(extra, key, item):
                acked.append(extra)
        if len(acked) < need:
            raise HTTPException(503, f"only {len(acked)}/{need} replicas acknowledged")
        return {"key": key, "ok": True, "version": item["ver"], "expires_at": exp, "owners": owners,
                "acked": acked, "coordinator": self.id}

    def status_report(self) -> dict:
        ring = self.ring()
        return {
            "id": self.id, "addr": self.addr, "status": self.status, "incarnation": self.incarnation,
            "heartbeat": self.heartbeat, "keys": self.store.live_count(), "stored": len(self.store.data),
            "max_keys": self.store.max_keys, "evictions": self.store.evictions, "expired": self.store.expired,
            "epoch": self.epoch, "ring_nodes": ring.nodes, "replicas": N_REPLICAS,
            "members": [{"id": m["id"], "addr": m["addr"], "status": self.liveness(m),
                         "seen_ago": round(time.monotonic() - m["seen"], 2)} for m in self.members.values()],
            "stats": self.stats, "events": list(self.events)[-25:],
            "primary_keys": sum(1 for k in self.store.data if ring.primary(k) == self.id),
        }


def parent_alive(pid: int) -> bool:
    if sys.platform == "win32":
        import ctypes
        k = ctypes.windll.kernel32
        handle = k.OpenProcess(0x00100000, False, pid)  # SYNCHRONIZE
        if not handle:
            return False
        alive = k.WaitForSingleObject(handle, 0) == 0x102  # WAIT_TIMEOUT: still running
        k.CloseHandle(handle)
        return alive
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


# ---------------------------------------------------------------------- HTTP surface
class PutIn(BaseModel):
    value: str
    ttl: float | None = None
    consistency: str = "eventual"


def build_app(node: Node) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if sys.platform == "win32":
            import ctypes
            ctypes.windll.winmm.timeBeginPeriod(1)
        # short connect timeout: on Windows a connection to a dead local port takes ~2 s to be refused
        node.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1.5, sock_connect=0.4),
                                          connector=aiohttp.TCPConnector(limit=200, limit_per_host=50))
        tasks = [asyncio.create_task(node.gossip_loop()), asyncio.create_task(node.rebalance_loop()),
                 asyncio.create_task(node.sweep_loop())]
        asyncio.create_task(node.bootstrap())
        yield
        for t in tasks:
            t.cancel()
        await node.http.close()

    app = FastAPI(title=f"cache node {node.id}", lifespan=lifespan)

    @app.get("/kv/{key}")
    async def get(key: str, consistency: str = "eventual"):
        return await node.coord_get(key, consistency)

    @app.put("/kv/{key}")
    async def put(key: str, body: PutIn):
        return await node.coord_put(key, body.value, body.ttl, body.consistency)

    @app.delete("/kv/{key}")
    async def delete(key: str, consistency: str = "eventual"):
        return await node.coord_put(key, None, None, consistency, delete=True)

    @app.post("/internal/gossip")
    async def gossip(request: Request):
        node.merge_members((await request.json())["members"])
        return {"members": node.member_list()}

    @app.post("/internal/get")
    async def iget(request: Request):
        return {"item": node.store.get((await request.json())["key"])}

    @app.post("/internal/put")
    async def iput(request: Request):
        b = await request.json()
        node.stats["replica_writes"] += 1
        node.store.merge(b["key"], b["item"])
        node.owner_cache[b["key"]] = tuple(node.ring().preference(b["key"], N_REPLICAS))
        return {"ok": True}

    @app.post("/internal/replicate")
    async def replicate(request: Request):
        items = (await request.json())["items"]
        n = sum(node.store.merge(k, v) for k, v in items.items())
        return {"merged": n}

    @app.post("/internal/handoff")
    async def handoff(request: Request):
        return {"items": node.handoff_for((await request.json())["joiner"])}

    @app.get("/internal/status")
    async def status():
        return node.status_report()

    @app.get("/internal/members")
    async def members():
        return {"alive": [{"id": i, "addr": node.addr_of(i)} for i in node.alive_ids()]}

    @app.get("/internal/keys")
    async def keys(limit: int = 200):
        ring = node.ring()
        out = []
        for k, v in list(node.store.data.items())[-limit:]:
            out.append({"key": k, "deleted": v["del"], "expires_at": v["exp"],
                        "owners": ring.preference(k, N_REPLICAS)})
        return out

    @app.get("/health")
    async def health():
        return {"ok": True, "id": node.id, "status": node.status}

    return app


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--id", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--seeds", default="")
    p.add_argument("--max-keys", type=int, default=int(os.getenv("CACHE_MAX_KEYS", "20000")))
    p.add_argument("--parent-pid", type=int, default=None)
    a = p.parse_args()
    addr = f"127.0.0.1:{a.port}"
    node = Node(a.id, addr, a.seeds.split(","), a.max_keys, a.parent_pid)
    uvicorn.run(build_app(node), host="127.0.0.1", port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
