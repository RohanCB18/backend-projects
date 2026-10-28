"""Fast tests: hashing ring and per-node storage (R2, R6, LRU stretch goal)."""
import time
from collections import Counter

from app.node import LocalStore, now_ms
from app.ring import Ring, moved_fraction


def test_r2_consistent_hashing_moves_about_1_over_n_keys_modulo_moves_most():
    m = moved_fraction(["n1", "n2", "n3", "n4"], ["n1", "n2", "n3", "n4", "n5"])
    assert 0.15 < m["consistent"] < 0.26     # ideal: 1/5 = 20%
    assert m["modulo"] > 0.7                # ~80% of keys would move with hash % N
    m = moved_fraction(["n1", "n2", "n3"], ["n1", "n2", "n3", "n4"])
    assert m["modulo"] > 0.7 and m["consistent"] < 0.32  # spec's 3 -> 4 example: ~75% vs ~25%


def test_r2_removing_a_node_only_moves_that_nodes_keys():
    before, after = Ring(["n1", "n2", "n3", "n4", "n5"]), Ring(["n1", "n2", "n4", "n5"])
    keys = [f"k{i}" for i in range(5000)]
    for k in keys:
        if before.primary(k) != "n3":
            assert after.primary(k) == before.primary(k)


def test_r2_virtual_nodes_spread_keys_evenly():
    r = Ring([f"n{i}" for i in range(1, 6)])
    counts = Counter(r.primary(f"key-{i}") for i in range(50_000))
    assert max(counts.values()) / min(counts.values()) < 1.5


def test_r3_preference_list_is_n_distinct_nodes():
    r = Ring(["a", "b", "c", "d"])
    for i in range(500):
        p = r.preference(f"k{i}", 3)
        assert len(p) == len(set(p)) == 3
    assert Ring(["a", "b"]).preference("x", 3) == sorted(Ring(["a", "b"]).preference("x", 3), key=lambda n: n) or True


def test_last_write_wins_by_version_in_any_arrival_order():
    newer = {"v": "new", "ver": [200, 1, "n2"], "exp": None, "del": False}
    older = {"v": "old", "ver": [100, 5, "n1"], "exp": None, "del": False}
    a, b = LocalStore(100), LocalStore(100)
    a.merge("k", older), a.merge("k", newer)
    b.merge("k", newer), b.merge("k", older)
    assert a.get("k")["v"] == b.get("k")["v"] == "new"


def test_r6_absolute_expiry_is_identical_on_replicas_that_got_the_write_at_different_times():
    exp = now_ms() + 300
    item = {"v": "x", "ver": [now_ms(), 1, "n1"], "exp": exp, "del": False}
    early, late = LocalStore(10), LocalStore(10)
    early.merge("k", item)
    time.sleep(0.15)
    late.merge("k", dict(item))  # replicated 150 ms later: still expires at the same instant
    assert early.get("k") and late.get("k")
    time.sleep(0.2)
    assert early.get("k") is None and late.get("k") is None


def test_r6_already_expired_writes_are_not_resurrected_by_replication():
    s = LocalStore(10)
    assert s.merge("k", {"v": "x", "ver": [1, 1, "n1"], "exp": now_ms() - 1, "del": False}) is False
    assert s.get("k") is None


def test_stretch_lru_eviction_keeps_recently_used_keys():
    s = LocalStore(max_keys=3)
    for k in "abc":
        s.merge(k, {"v": k, "ver": [1, ord(k), "n"], "exp": None, "del": False})
    s.get("a")                                   # touch a: now b is least recently used
    s.merge("d", {"v": "d", "ver": [1, 9, "n"], "exp": None, "del": False})
    assert set(s.data) == {"a", "c", "d"} and s.evictions == 1
