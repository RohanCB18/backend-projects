"""Convergence, offline merge, history and cross-implementation tests (R1-R4, R6)."""
import json
import os
import random
import shutil
import subprocess

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.rga import RGA, op_key
from app.store import DocStore

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JS_RGA = os.path.join(ROOT, "..", "frontend", "src", "lib", "rga.js")


def sync_all(*replicas, ops):
    for r in replicas:
        for op in ops:
            r.apply(op)


# ---- R1 / R2: the spec's own example -------------------------------------------------
def test_r1_cat_becomes_chats_on_both_sides():
    alice, bob = RGA("alice"), RGA("bob")
    base = alice.insert(0, "cat")
    sync_all(bob, ops=base)

    a_ops = alice.insert(3, "s")   # "cats"
    b_ops = bob.insert(1, "h")     # "chat"  (concurrently, neither has seen the other)
    assert alice.text() == "cats" and bob.text() == "chat"

    sync_all(alice, ops=b_ops)
    sync_all(bob, ops=a_ops)
    assert alice.text() == bob.text() == "chats"   # not "chast"


def test_r2_concurrent_inserts_at_the_same_spot_are_ordered_identically():
    a, b, c = RGA("a"), RGA("b"), RGA("c")
    base = a.insert(0, "[]")
    sync_all(b, c, ops=base)
    ops = a.insert(1, "AAA") + b.insert(1, "BBB") + c.insert(1, "CCC")
    for r in (a, b, c):
        for op in ops:
            r.apply(op)
    assert a.text() == b.text() == c.text()
    # each run stays together (no interleaving of characters from different users)
    assert sorted(a.text()[1:-1].replace("AAA", "A").replace("BBB", "B").replace("CCC", "C")) == ["A", "B", "C"]


def test_applying_an_op_twice_changes_nothing():
    a = RGA("a")
    ops = a.insert(0, "hello") + a.delete(1, 2)
    b = RGA("b")
    for op in ops + ops + list(reversed(ops)):
        b.apply(op)
    assert b.text() == a.text() == "hlo"


# ---- R6: property-based convergence ----------------------------------------------------
edit = st.tuples(st.integers(0, 2), st.booleans(), st.integers(0, 1000), st.text("abcxyz", min_size=1, max_size=3))


@settings(max_examples=300, deadline=None)
@given(rounds=st.lists(st.lists(edit, max_size=6), min_size=1, max_size=5), seed=st.integers(0, 10_000))
def test_r6_any_delivery_order_converges(rounds, seed):
    """3 replicas edit concurrently in rounds. Between rounds each replica hears
    about a random subset of other replicas' ops (partial sync, so later edits
    depend on remote characters). At the end every replica receives everything
    in its own random order (out-of-order delivery exercises the pending buffer).
    All replicas must end up with exactly the same text."""
    rnd = random.Random(seed)
    reps = [RGA(s) for s in ("a", "b", "c")]
    log: list[dict] = []
    for round_ in rounds:
        for who, is_insert, pos, text in round_:
            r = reps[who]
            n = len(r)
            if is_insert or n == 0:
                log += r.insert(pos % (n + 1), text)
            else:
                p = pos % n
                log += r.delete(p, min(len(text), n - p))
        for r in reps:  # partial gossip
            for op in rnd.sample(log, k=len(log) // 2):
                r.apply(op)
    for r in reps:
        shuffled = log[:]
        rnd.shuffle(shuffled)
        for op in shuffled:
            r.apply(op)
        assert not r.pending
    assert reps[0].text() == reps[1].text() == reps[2].text()


@settings(max_examples=100, deadline=None)
@given(script=st.lists(st.tuples(st.booleans(), st.integers(0, 50), st.text("ab", min_size=1, max_size=2)),
                       min_size=1, max_size=30), seed=st.integers(0, 10_000))
def test_r6_same_ops_in_every_permutation_give_same_text(script, seed):
    src = RGA("src")
    other = RGA("other")
    ops = []
    for i, (ins, pos, text) in enumerate(script):
        r = src if i % 2 else other
        n = len(r)
        ops += r.insert(pos % (n + 1), text) if ins or n == 0 else r.delete(pos % n, 1)
        if i % 3 == 0:
            for op in ops:
                src.apply(op)
                other.apply(op)
    reference = RGA("ref")
    for op in ops:
        reference.apply(op)
    rnd = random.Random(seed)
    for _ in range(5):
        r = RGA("x")
        perm = ops[:]
        rnd.shuffle(perm)
        for op in perm:
            r.apply(op)
        assert r.text() == reference.text()


# ---- R3: offline edits merge without overwriting anyone --------------------------------------
def test_r3_offline_client_merges_without_losing_anyones_work():
    online, offline = RGA("online"), RGA("offline")
    base = online.insert(0, "Meeting notes:\n")
    sync_all(offline, ops=base)

    # offline user writes a paragraph; online user keeps editing meanwhile
    off_ops = offline.insert(len(offline), "- budget approved\n") + offline.insert(0, "[DRAFT] ")
    on_ops = online.insert(len(online), "- launch on Friday\n") + online.delete(0, 7) + online.insert(0, "Sprint")

    sync_all(offline, ops=on_ops)   # reconnect: receive what was missed...
    sync_all(online, ops=off_ops)   # ...and upload the outbox
    assert online.text() == offline.text()
    for fragment in ("[DRAFT] ", "- budget approved", "- launch on Friday", "Sprint"):
        assert fragment in online.text()


# ---- R4: history and revert ------------------------------------------------------------------------
def test_r4_revert_to_any_point_and_revert_the_revert(tmp_path):
    store = DocStore(str(tmp_path / "c.db"))
    a = RGA("a")
    store.append("d", a.insert(0, "hello world"))
    seq_hello = store.seq["d"]
    store.append("d", a.delete(0, 5) + a.insert(0, "goodbye"))
    store.append("d", a.insert(len(a), "!!!"))
    assert store.doc("d").text() == "goodbye world!!!"
    assert store.at("d", seq_hello).text() == "hello world"

    cur = store.at("d", store.seq["d"])
    store.append("d", cur.ops_to_become(store.at("d", seq_hello), site="revert"))
    assert store.doc("d").text() == "hello world"
    seq_after_revert = store.seq["d"]

    # undo the revert: go back to the state just before it
    seq_before_revert = max(s for s in range(seq_after_revert) if store.at("d", s).text() == "goodbye world!!!")
    cur = store.at("d", store.seq["d"])
    store.append("d", cur.ops_to_become(store.at("d", seq_before_revert), site="revert2"))
    assert store.doc("d").text() == "goodbye world!!!"
    assert store.seq["d"] > seq_after_revert  # history only grows
    # the full log is still there: replaying it reproduces the present
    assert store.at("d", store.seq["d"]).text() == store.doc("d").text()


def test_r4_history_survives_server_restart(tmp_path):
    path = str(tmp_path / "c.db")
    s1 = DocStore(path)
    a = RGA("a")
    s1.append("d", a.insert(0, "persist me"))
    s2 = DocStore(path)
    assert s2.doc("d").text() == "persist me" and s2.seq["d"] == 10


def test_r7_resent_ops_are_stored_once(tmp_path):
    store = DocStore(str(tmp_path / "c.db"))
    a = RGA("a")
    ops = a.insert(0, "abc")
    assert len(store.append("d", ops)) == 3
    assert store.append("d", ops) == []  # client resent after a lost ack
    assert store.doc("d").text() == "abc" and store.seq["d"] == 3


# ---- the browser and the server run the same algorithm ------------------------------------------------
@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_python_and_javascript_crdts_agree(tmp_path):
    rnd = random.Random(42)
    cases = []
    for _ in range(40):
        reps = [RGA(s) for s in ("a", "b", "c")]
        log = []
        for _ in range(rnd.randint(1, 6)):
            for r in reps:
                for _ in range(rnd.randint(0, 3)):
                    n = len(r)
                    if n and rnd.random() < 0.3:
                        log += r.delete(rnd.randrange(n), 1)
                    else:
                        log += r.insert(rnd.randint(0, n), rnd.choice(["x", "yz", "Q", "hey"]))
            for r in reps:
                for op in rnd.sample(log, k=len(log) // 2):
                    r.apply(op)
        rnd.shuffle(log)
        py = RGA("py")
        for op in log:
            py.apply(op)
        cases.append({"ops": log, "expected": py.text()})
    data = tmp_path / "cases.json"
    data.write_text(json.dumps(cases))
    script = tmp_path / "check.mjs"
    script.write_text(
        f"import {{ RGA }} from {json.dumps('file:///' + os.path.abspath(JS_RGA).replace(os.sep, '/'))};\n"
        "import { readFileSync } from 'node:fs';\n"
        "const cases = JSON.parse(readFileSync(process.argv[2], 'utf8'));\n"
        "const out = cases.map(c => { const r = new RGA('js'); for (const op of c.ops) r.apply(op); return r.text(); });\n"
        "console.log(JSON.stringify(out));\n")
    res = subprocess.run(["node", str(script), str(data)], capture_output=True, text=True, timeout=60)
    assert res.returncode == 0, res.stderr
    assert json.loads(res.stdout) == [c["expected"] for c in cases]
    assert op_key(cases[0]["ops"][0])
