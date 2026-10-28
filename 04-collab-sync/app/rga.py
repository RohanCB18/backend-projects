"""RGA (Replicated Growable Array): a sequence CRDT for plain text.

The problem: the document is "cat". Alice appends "s" at index 3 ("cats");
at the same time Bob inserts "h" at index 1 ("chat"). If Bob applies Alice's
"insert s at 3" literally he gets "chast". Indexes shift under concurrent edits.

The RGA idea: never address characters by index. Give every character a
unique, never-reused id and describe an insert as "put c right AFTER the
character with id X". Deleting only marks a character as a tombstone, so X
keeps existing and later inserts can still refer to it.

    id = (lamport_counter, site_id)   compared as a tuple: higher = "newer"

Integration rule for "insert N after X": go to X, then skip every element whose
id is greater than N's id, and place N there. Concurrent inserts after the same
X therefore end up sorted by id (newest first) on every replica, regardless of
the order they arrive in. That is the whole convergence argument, and the
property test in tests/ checks it with thousands of random histories.

Ops are plain dicts so they serialise to JSON unchanged:
    {"t": "i", "id": [7, "alice"], "after": [3, "bob"] | None, "ch": "x"}
    {"t": "d", "id": [8, "alice"], "target": [7, "alice"]}

The browser has the same algorithm in frontend/src/lib/rga.js; a test runs
both on identical op streams and checks they agree character for character.
"""
from __future__ import annotations

Id = tuple  # (int counter, str site)


def op_key(op: dict) -> str:
    return f"{op['id'][0]}@{op['id'][1]}"


class Elem:
    __slots__ = ("id", "ch", "deleted")

    def __init__(self, id: Id, ch: str):
        self.id = id
        self.ch = ch
        self.deleted = False


class RGA:
    def __init__(self, site: str):
        self.site = site
        self.clock = 0                      # Lamport clock
        self.elems: list[Elem] = []         # document order, tombstones included
        self.index: dict[Id, Elem] = {}
        self.deletes_seen: set[Id] = set()  # delete-op ids, for idempotency
        self.pending: list[dict] = []       # ops waiting for something they reference

    # ------------------------------------------------------------------ reading
    def text(self) -> str:
        return "".join(e.ch for e in self.elems if not e.deleted)

    def visible(self) -> list[Elem]:
        return [e for e in self.elems if not e.deleted]

    def __len__(self) -> int:
        return sum(1 for e in self.elems if not e.deleted)

    # ------------------------------------------------------------------ local edits -> ops
    def _next_id(self) -> Id:
        self.clock += 1
        return (self.clock, self.site)

    def insert(self, pos: int, text: str) -> list[dict]:
        """Insert `text` at visible index `pos`. Returns the ops (already applied)."""
        vis = self.visible()
        if not 0 <= pos <= len(vis):
            raise IndexError(pos)
        after = list(vis[pos - 1].id) if pos > 0 else None
        ops = []
        for ch in text:
            op = {"t": "i", "id": list(self._next_id()), "after": after, "ch": ch}
            self.apply(op)
            ops.append(op)
            after = op["id"]  # the next char goes right after the one we just typed
        return ops

    def delete(self, pos: int, length: int = 1) -> list[dict]:
        vis = self.visible()
        if not (0 <= pos and pos + length <= len(vis)):
            raise IndexError(pos)
        ops = []
        for e in vis[pos:pos + length]:
            op = {"t": "d", "id": list(self._next_id()), "target": list(e.id)}
            self.apply(op)
            ops.append(op)
        return ops

    # ------------------------------------------------------------------ integrating any op
    def apply(self, op: dict) -> bool:
        """Apply a local or remote op. Idempotent: applying it twice is a no-op.
        If the op references something we haven't seen yet (possible when ops
        arrive out of order, e.g. after being offline) it waits in `pending`.
        Returns True if the op changed the document now."""
        changed = self._try(op)
        if changed is None:
            if all(op_key(p) != op_key(op) for p in self.pending):
                self.pending.append(op)
            return False
        if changed:
            self._drain_pending()
        return changed

    def _try(self, op: dict) -> bool | None:
        oid = tuple(op["id"])
        self.clock = max(self.clock, oid[0])
        if op["t"] == "i":
            if oid in self.index:
                return False  # duplicate delivery
            if op["after"] is None:
                i = 0
            else:
                ref = self.index.get(tuple(op["after"]))
                if ref is None:
                    return None  # not ready
                i = self._pos(ref) + 1
            # skip concurrent siblings with a higher id (and, as a consequence,
            # all of their descendants, which have even higher Lamport counters)
            while i < len(self.elems) and self.elems[i].id > oid:
                i += 1
            e = Elem(oid, op["ch"])
            self.elems.insert(i, e)
            self.index[oid] = e
            return True
        if op["t"] == "d":
            if oid in self.deletes_seen:
                return False
            target = self.index.get(tuple(op["target"]))
            if target is None:
                return None
            self.deletes_seen.add(oid)
            already = target.deleted
            target.deleted = True
            return not already
        raise ValueError(f"unknown op type {op['t']!r}")

    def _pos(self, elem: Elem) -> int:
        for i, e in enumerate(self.elems):
            if e is elem:
                return i
        raise KeyError(elem.id)

    def _drain_pending(self) -> None:
        progress = True
        while progress and self.pending:
            progress = False
            for op in list(self.pending):
                if self._try(op) is not None:
                    self.pending.remove(op)
                    progress = True

    # ------------------------------------------------------------------ revert support
    def ops_to_become(self, target: "RGA", site: str) -> list[dict]:
        """New ops that turn this document's visible text into `target`'s
        visible text (target = this same document replayed to an earlier point).
        Reverting is itself a new edit: history stays append-only."""
        self.site, old_site = site, self.site
        ops: list[dict] = []
        want = [e.id for e in target.elems if not e.deleted]
        want_set = set(want)
        # 1. delete everything visible now that wasn't visible then
        for e in self.visible():
            if e.id not in want_set:
                op = {"t": "d", "id": list(self._next_id()), "target": list(e.id)}
                self.apply(op)
                ops.append(op)
        # 2. bring back characters deleted since then, as fresh copies placed
        #    after the previous surviving character
        anchor = None
        for wid in want:
            cur = self.index.get(wid)
            if cur is not None and not cur.deleted:
                anchor = list(cur.id)
                continue
            op = {"t": "i", "id": list(self._next_id()), "after": anchor, "ch": target.index[wid].ch}
            self.apply(op)
            ops.append(op)
            anchor = op["id"]
        self.site = old_site
        return ops
