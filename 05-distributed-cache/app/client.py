"""Cluster-aware client.

It knows a few node addresses, refreshes the list of live nodes from the
cluster every second, sends each request to a random live node (which then
coordinates it), and if that node fails it retries ONCE on a different node.
Requirement 7 says "zero failed requests beyond one configurable retry": this
is that retry.

(aiohttp rather than httpx here: the traffic generator sends hundreds of
requests per second from one process, and aiohttp costs far less CPU per request.)
"""
from __future__ import annotations

import random
import time

import aiohttp


class RequestFailed(Exception):
    pass


class CacheClient:
    def __init__(self, seeds: list[str], retries: int = 1, timeout: float = 3.0):
        self.known = list(seeds)
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout, sock_connect=0.4)
        self._session: aiohttp.ClientSession | None = None
        self._refreshed = 0.0

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout,
                                                  connector=aiohttp.TCPConnector(limit=200, limit_per_host=60))
        return self._session

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()

    async def refresh(self, force: bool = False) -> None:
        if not force and time.time() - self._refreshed < 1.0:
            return
        self._refreshed = time.time()
        for addr in random.sample(self.known, len(self.known)):
            try:
                async with self.session.get(f"http://{addr}/internal/members",
                                            timeout=aiohttp.ClientTimeout(total=0.8, sock_connect=0.3)) as r:
                    alive = [m["addr"] for m in (await r.json())["alive"]]
                if alive:
                    self.known = alive
                    return
            except Exception:
                continue

    async def request(self, method: str, key: str, **kw) -> tuple[dict, int, list[str]]:
        """Returns (response json, attempts used, nodes tried)."""
        await self.refresh()
        tried: list[str] = []
        last_err = None
        for attempt in range(1 + self.retries):
            choices = [a for a in self.known if a not in tried] or self.known
            addr = random.choice(choices)
            tried.append(addr)
            try:
                async with self.session.request(method, f"http://{addr}/kv/{key}", **kw) as r:
                    if r.status >= 500:
                        raise RequestFailed(f"{addr} -> {r.status} {(await r.text())[:120]}")
                    return await r.json(), attempt + 1, tried
            except Exception as e:
                last_err = e
                self.known = [a for a in self.known if a != addr] or self.known  # don't pick it again soon
        raise RequestFailed(f"failed after {1 + self.retries} attempts via {tried}: {last_err!r}")

    async def get(self, key: str, consistency: str = "eventual"):
        return await self.request("GET", key, params={"consistency": consistency})

    async def put(self, key: str, value: str, ttl: float | None = None, consistency: str = "eventual"):
        return await self.request("PUT", key, json={"value": value, "ttl": ttl, "consistency": consistency})

    async def delete(self, key: str, consistency: str = "eventual"):
        return await self.request("DELETE", key, params={"consistency": consistency})
