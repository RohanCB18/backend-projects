"""Real WebSocket tests against a running server (R5, R7, presence)."""
import asyncio
import json
import os
import socket
import subprocess
import sys
import time
import uuid

import httpx
import psycopg
import pytest
import websockets

from app.rga import RGA, op_key
from app.store import DEFAULT_DSN

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="module")
def server():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    schema = f"test_{uuid.uuid4().hex[:10]}"
    env = {**os.environ, "COLLAB_SCHEMA": schema}
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port),
                             "--log-level", "warning"], cwd=ROOT, env=env)
    for _ in range(100):
        try:
            if httpx.get(f"http://127.0.0.1:{port}/health").status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(0.1)
    yield f"127.0.0.1:{port}"
    proc.terminate()
    proc.wait()
    with psycopg.connect(os.getenv("COLLAB_DSN", DEFAULT_DSN), autocommit=True) as con:
        con.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


class Client:
    """A minimal client: local replica + outbox + last seen seq."""

    def __init__(self, host, doc, site):
        self.host, self.doc, self.site = host, doc, site
        self.rga = RGA(site)
        self.outbox: dict[str, dict] = {}
        self.seq = 0
        self.ws = None
        self.received_at: dict[str, float] = {}

    async def connect(self):
        self.ws = await websockets.connect(f"ws://{self.host}/ws/{self.doc}?site={self.site}")
        await self.ws.send(json.dumps({"type": "hello", "since": self.seq}))
        await self.pump(until="sync")
        if self.outbox:
            await self.ws.send(json.dumps({"type": "ops", "ops": list(self.outbox.values())}))

    async def pump(self, until=None, timeout=3.0):
        end = time.time() + timeout
        while time.time() < end:
            try:
                msg = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=end - time.time()))
            except asyncio.TimeoutError:
                return
            if msg["type"] in ("sync", "ops"):
                for op in msg["ops"]:
                    self.rga.apply(op)
                    self.seq = max(self.seq, op["seq"])
                    self.received_at[op_key(op)] = time.perf_counter()
                if msg["type"] == "sync":
                    self.seq = max(self.seq, msg["seq"])
            elif msg["type"] == "ack":
                for k in msg["keys"]:
                    self.outbox.pop(k, None)
                self.seq = max(self.seq, msg["seq"])
            if msg["type"] == until:
                return

    async def type(self, pos, text, send=True):
        ops = self.rga.insert(pos, text)
        for op in ops:
            self.outbox[op_key(op)] = op
        if send:
            await self.ws.send(json.dumps({"type": "ops", "ops": ops}))
        return ops


def run(coro):
    return asyncio.run(coro)


def test_r5_edits_reach_other_clients_in_well_under_a_second(server):
    async def go():
        a, b, c = Client(server, "latency", "a"), Client(server, "latency", "b"), Client(server, "latency", "c")
        for cl in (a, b, c):
            await cl.connect()
        sent_at = time.perf_counter()
        ops = await a.type(0, "hello!")
        await asyncio.gather(b.pump(timeout=1.0), c.pump(timeout=1.0), a.pump(timeout=0.5))
        delays = [cl.received_at[op_key(ops[-1])] - sent_at for cl in (b, c)]
        assert b.rga.text() == c.rga.text() == "hello!"
        assert max(delays) < 1.0, delays
        print(f"\npropagation: {[f'{d * 1000:.1f} ms' for d in delays]}")
        for cl in (a, b, c):
            await cl.ws.close()
    run(go())


def test_r3_r7_disconnect_mid_edit_then_reconnect_no_loss_no_duplicates(server):
    async def go():
        doc = "flaky-net"
        a, b = Client(server, doc, "a"), Client(server, doc, "b")
        await a.connect()
        await b.connect()
        await a.type(0, "shared ")
        await asyncio.gather(a.pump(timeout=0.5), b.pump(timeout=0.5))

        # a sends an edit and the connection dies before the ack arrives
        await a.type(len(a.rga), "ABC")
        await a.ws.close()
        assert a.outbox  # unacked from a's point of view

        # while a is offline: a types more, b types too
        await a.type(0, ">> ", send=False)
        await b.type(len(b.rga), " from-b")
        await b.pump(timeout=0.5)

        # reconnect: catch up, resend whole outbox (including ops the server already has)
        await a.connect()
        await asyncio.gather(a.pump(timeout=1.0), b.pump(timeout=1.0))
        assert not a.outbox
        server_doc = httpx.get(f"http://{server}/docs/{doc}").json()
        assert a.rga.text() == b.rga.text() == server_doc["text"]
        assert server_doc["text"].count("ABC") == 1 and server_doc["text"].count(">> ") == 1
        assert "from-b" in server_doc["text"]
        # every op stored exactly once: seq == number of characters ever inserted
        assert server_doc["seq"] == len("shared ") + len("ABC") + len(">> ") + len(" from-b")
        await a.ws.close()
        await b.ws.close()
    run(go())


def test_r4_revert_over_http_is_broadcast_to_clients(server):
    async def go():
        doc = "revert-me"
        a, b = Client(server, doc, "a"), Client(server, doc, "b")
        await a.connect()
        await b.connect()
        await a.type(0, "version one")
        await asyncio.gather(a.pump(timeout=0.4), b.pump(timeout=0.4))
        v1 = httpx.get(f"http://{server}/docs/{doc}").json()["seq"]
        await a.type(len(a.rga), " + more edits")
        await asyncio.gather(a.pump(timeout=0.4), b.pump(timeout=0.4))
        r = httpx.post(f"http://{server}/docs/{doc}/revert", json={"seq": v1}).json()
        assert r["text"] == "version one"
        await asyncio.gather(a.pump(timeout=0.5), b.pump(timeout=0.5))
        assert a.rga.text() == b.rga.text() == "version one"
        await a.ws.close()
        await b.ws.close()
    run(go())


def test_stretch_presence_channel_reports_peers_and_cursors(server):
    async def go():
        p1 = await websockets.connect(f"ws://{server}/presence/p?site=a&name=Alice&color=red")
        p2 = await websockets.connect(f"ws://{server}/presence/p?site=b&name=Bob&color=blue")
        await p2.send(json.dumps({"cursor": [3, "a"]}))
        seen = None
        end = time.time() + 2
        while time.time() < end:
            msg = json.loads(await asyncio.wait_for(p1.recv(), timeout=2))
            bob = [p for p in msg["peers"] if p["site"] == "b"]
            if bob and bob[0]["cursor"] == [3, "a"]:
                seen = msg
                break
        assert seen and {p["name"] for p in seen["peers"]} == {"Alice", "Bob"}
        await p1.close()
        await p2.close()
    run(go())
