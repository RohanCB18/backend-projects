"""Consistent hashing with virtual nodes.

Why not hash(key) % N: going from 4 to 5 nodes changes the answer for ~80% of
keys, so almost the whole cache moves (or goes cold) at once.

Consistent hashing puts nodes and keys on the same circle (here: 64-bit hash
space). A key belongs to the first node clockwise from it. Adding a node only
takes over the arc right before it, so roughly 1/N of the keys move.

Each physical node gets many points on the circle (virtual nodes). With one
point per node the arcs would be very uneven; with ~64 they even out.

Replication: the key's "preference list" is the first N *distinct* physical
nodes clockwise. Index 0 is the primary, the rest are replicas.
"""
from __future__ import annotations

import bisect
import hashlib

VNODES = 64


def h(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode()).digest()[:8], "big")


class Ring:
    def __init__(self, nodes: list[str], vnodes: int = VNODES):
        self.nodes = sorted(set(nodes))
        self.vnodes = vnodes
        points = [(h(f"{n}#{i}"), n) for n in self.nodes for i in range(vnodes)]
        points.sort()
        self.hashes = [p[0] for p in points]
        self.owners = [p[1] for p in points]

    def preference(self, key: str, n: int) -> list[str]:
        """First n distinct nodes clockwise from the key."""
        if not self.nodes:
            return []
        n = min(n, len(self.nodes))
        i = bisect.bisect_right(self.hashes, h(key)) % len(self.hashes)
        out: list[str] = []
        while len(out) < n:
            node = self.owners[i]
            if node not in out:
                out.append(node)
            i = (i + 1) % len(self.hashes)
        return out

    def primary(self, key: str) -> str | None:
        p = self.preference(key, 1)
        return p[0] if p else None

    def points(self) -> list[dict]:
        """For drawing the ring: angle in [0, 1) + node."""
        top = 2 ** 64
        return [{"pos": hv / top, "node": n} for hv, n in zip(self.hashes, self.owners)]


def moved_fraction(before: list[str], after: list[str], sample: int = 20000) -> dict:
    """Share of keys whose primary node changes: consistent hashing vs modulo."""
    rb, ra = Ring(before), Ring(after)
    keys = [f"key:{i}" for i in range(sample)]
    consistent = sum(rb.primary(k) != ra.primary(k) for k in keys) / sample
    sb, sa = sorted(before), sorted(after)
    modulo = sum(sb[h(k) % len(sb)] != sa[h(k) % len(sa)] for k in keys) / sample
    return {"consistent": consistent, "modulo": modulo, "ideal": abs(len(after) - len(before)) / max(len(after), len(before))}
