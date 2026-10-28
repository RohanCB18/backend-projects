"""04 - Real-Time Collaborative Sync Engine.

Transport: one WebSocket per client for edits, plus a second WebSocket channel
for presence (who's online, where their cursor is), so chatty cursor updates
never delay real edits.

Edit protocol (JSON messages):
  client -> server  {"type": "hello", "since": <last seq I have>}
                    {"type": "ops", "ops": [op, ...]}           my new edits
  server -> client  {"type": "sync", "ops": [...], "seq": n}   everything I missed
                    {"type": "ops", "ops": [...]}              someone else's edits
                    {"type": "ack", "keys": [...], "seq": n}   your ops are durable
                    {"type": "reset"}                          document was wiped (demo)

Why this survives disconnects (R3, R7): a client keeps every op it made in an
outbox until the server acks it. On reconnect it says "hello, I have up to seq
N", receives what it missed, and resends its whole outbox. The server stores
ops under a unique key, so anything it already had is silently dropped:
nothing is lost, nothing is duplicated.
"""
from __future__ import annotations

import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .rga import op_key
from .store import DEFAULT_DSN, DocStore

DSN = os.getenv("COLLAB_DSN", DEFAULT_DSN)
SCHEMA = os.getenv("COLLAB_SCHEMA", "public")

# Every op-log access runs on this ONE thread, in arrival order: database calls
# never block the event loop, appends never interleave (so sequence numbers stay
# gap-free), and the in-memory documents are only ever touched by one thread.
_db = ThreadPoolExecutor(max_workers=1, thread_name_prefix="oplog")


async def db(fn, *args):
    return await asyncio.get_running_loop().run_in_executor(_db, fn, *args)


class Hub:
    """Who is connected to which document."""

    def __init__(self):
        self.edit: dict[str, dict[WebSocket, dict]] = {}
        self.presence: dict[str, dict[WebSocket, dict]] = {}

    async def send(self, ws: WebSocket, msg: dict) -> None:
        try:
            await ws.send_json(msg)
        except Exception:
            pass  # a dead socket is cleaned up by its own handler

    async def broadcast(self, doc: str, msg: dict, exclude: WebSocket | None = None) -> None:
        await asyncio.gather(*(self.send(ws, msg) for ws in list(self.edit.get(doc, {})) if ws is not exclude))

    def peers(self, doc: str) -> list[dict]:
        return list(self.edit.get(doc, {}).values())


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.store = await db(DocStore, DSN, SCHEMA)
    app.state.hub = Hub()
    yield
    await db(app.state.store.close)


app = FastAPI(title="04 Collaborative Sync Engine", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.websocket("/ws/{doc}")
async def edit_socket(ws: WebSocket, doc: str, site: str, name: str = ""):
    store: DocStore = ws.app.state.store
    hub: Hub = ws.app.state.hub
    await ws.accept()
    hub.edit.setdefault(doc, {})[ws] = {"site": site, "name": name or site, "since": time.time()}
    await hub.broadcast(doc, {"type": "peers", "peers": hub.peers(doc)})
    try:
        while True:
            msg = await ws.receive_json()
            if msg["type"] == "hello":
                missed, seq = await db(lambda: (store.since(doc, int(msg.get("since", 0))), store.seq[doc]))
                await hub.send(ws, {"type": "sync", "ops": missed, "seq": seq})
            elif msg["type"] == "ops":
                fresh, seq = await db(lambda: (store.append(doc, msg["ops"]), store.seq[doc]))
                # ack everything, including duplicates, so the client can empty its outbox
                await hub.send(ws, {"type": "ack", "keys": [op_key(o) for o in msg["ops"]], "seq": seq})
                if fresh:
                    await hub.broadcast(doc, {"type": "ops", "ops": fresh}, exclude=ws)
    except WebSocketDisconnect:
        pass
    finally:
        hub.edit.get(doc, {}).pop(ws, None)
        await hub.broadcast(doc, {"type": "peers", "peers": hub.peers(doc)})


@app.websocket("/presence/{doc}")
async def presence_socket(ws: WebSocket, doc: str, site: str, name: str = "", color: str = "#888"):
    """Stretch goal: presence on its own channel. Messages: {"cursor": [ctr, site] | null}."""
    hub: Hub = ws.app.state.hub
    await ws.accept()
    room = hub.presence.setdefault(doc, {})
    room[ws] = {"site": site, "name": name or site, "color": color, "cursor": None, "ts": time.time()}

    async def announce():
        snapshot = {"type": "presence", "peers": list(room.values())}
        await asyncio.gather(*(hub.send(w, snapshot) for w in list(room)))

    await announce()
    try:
        while True:
            msg = await ws.receive_json()
            room[ws].update(cursor=msg.get("cursor"), typing=bool(msg.get("typing")), ts=time.time())
            await announce()
    except WebSocketDisconnect:
        pass
    finally:
        room.pop(ws, None)
        await announce()


# ---- REST: document state, history, time travel -------------------------------------
@app.get("/docs/{doc}")
async def get_doc(doc: str, request: Request):
    store: DocStore = request.app.state.store

    def read():
        d = store.doc(doc)
        return {"doc": doc, "text": d.text(), "seq": store.seq[doc], "chars": len(d.elems),
                "tombstones": sum(e.deleted for e in d.elems)}

    return {**await db(read), "clients": request.app.state.hub.peers(doc)}


@app.get("/docs/{doc}/history")
async def history(doc: str, request: Request):
    store: DocStore = request.app.state.store

    def read():
        store.doc(doc)
        return {"seq": store.seq[doc], "edits": store.history(doc)}

    return await db(read)


@app.get("/docs/{doc}/at/{seq}")
async def at(doc: str, seq: int, request: Request):
    """R4: the document as it was after operation number `seq`."""
    store: DocStore = request.app.state.store
    return {"seq": seq, "text": await db(lambda: store.at(doc, seq).text())}


class RevertIn(BaseModel):
    seq: int


@app.post("/docs/{doc}/revert")
async def revert(doc: str, body: RevertIn, request: Request):
    """R4: revert = compute the edits that turn 'now' into 'then' and apply them
    as NEW ops. Nothing is erased from history, so a revert can be reverted."""
    store: DocStore = request.app.state.store

    def do_revert():
        store.doc(doc)
        current = store.at(doc, store.seq[doc])
        current.site = "server"
        fresh = store.append(doc, current.ops_to_become(store.at(doc, body.seq), site="revert"))
        return fresh, store.doc(doc).text(), store.seq[doc]

    fresh, text, seq = await db(do_revert)
    if fresh:
        await request.app.state.hub.broadcast(doc, {"type": "ops", "ops": fresh})
    return {"applied": len(fresh), "text": text, "seq": seq}


@app.post("/docs/{doc}/reset")
async def reset(doc: str, request: Request):
    await db(request.app.state.store.reset, doc)
    await request.app.state.hub.broadcast(doc, {"type": "reset"})
    return {"ok": True}


@app.get("/health")
async def health():
    return {"ok": True}
