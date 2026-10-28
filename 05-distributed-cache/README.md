# 05 · Self-Healing Distributed Cache

A Redis-cluster-like cache built from scratch: independent node processes,
consistent hashing, 3 copies of every key, gossip failure detection, automatic
re-replication and rebalancing, and a chaos test.

```bash
..\.venv\Scripts\python -m uvicorn app.main:app --port 9105
```
```bash
..\.venv\Scripts\python -m pytest -q
```

The first command starts the controller, which starts 5 nodes on 9201–9205.
One node by hand: `python -m app.node --id n1 --port 9201 --seeds 127.0.0.1:9202,...`

## Pieces

| Piece | Where | How |
|---|---|---|
| Ring | `app/ring.py` | MD5 → 64-bit circle, 64 virtual nodes per server; a key's preference list = first 3 distinct nodes clockwise |
| Storage | `LocalStore` | dict with LRU eviction, absolute-time TTLs, version `[ms, counter, node]`, last-write-wins merge |
| Coordinator | `Node.coord_get/put` | any node serves any request. Writes go to all 3 owners (ack after W=1, or W=2 in quorum mode; borrows the next ring node if needed). Reads take the first copy found (or R=2 newest + read-repair) |
| Gossip | `gossip_once` | every 300 ms: bump own heartbeat, swap membership with 3 random peers. Silent for 1 s = suspect, 2 s = dead, removed from the ring |
| Self-healing | `rebalance` | when the ring changes, push copies to new owners, then drop keys this node no longer owns. Full anti-entropy every 8 s |
| Joining | `bootstrap` | new/restarted node starts JOINING (not in the ring), streams the key range it will own, then flips to ALIVE |
| Client | `app/client.py` | refreshes live nodes every second, sends to a random one, **retries once** on another |
| Controller | `app/main.py` | starts/kills nodes (`TerminateProcess`/SIGKILL), traffic generator with per-key verification, chaos test |

Design notes:
* **TTL as an absolute time.** The coordinator turns `ttl=30` into
  "expires at 13:05:00.123" once; a replica that receives the write later
  still expires it at the same instant.
* **Windows detail.** Connecting to a dead local port takes ~2 s to be
  refused, so internal calls use a 0.4 s connect timeout, and nodes skip peers
  gossip already knows are dead.
* **CAP.** Eventual mode (W=1/R=1) stays available and may briefly serve a
  stale copy while a node is dying. Quorum mode (W=2, R=2, N=3, so R+W>N)
  always overlaps the latest write.

## How each requirement is proven

| Req | Proof |
|---|---|
| R1 one logical cache | `test_r1_five_independent_processes_form_one_logical_cache` (any node answers for any key) |
| R2 consistent hashing | `test_r2_*`: 4→5 nodes moves ~20% of keys vs ~80% with modulo; removing a node only moves its own keys; vnodes keep load even |
| R3 replication | `test_r3_every_key_lives_on_three_nodes_its_preference_list`; reads succeed right after the primary is killed |
| R4 failure detection | `test_r3_r4_primary_dies_…`: dead node leaves every ring in < 5 s with no manual step, and the key is back to 3 copies |
| R5 rebalance / join | `test_r5_new_node_joins_takes_its_share_and_clients_never_notice`: a 6th node joins under traffic, takes over keys, 0 failed requests |
| R6 TTL consistency | `test_r6_ttl_is_the_same_absolute_instant_on_every_replica` + unit tests |
| R7 chaos | `test_r7_chaos_kill_random_node_mid_traffic_zero_failed_requests` (eventual and quorum): about 1,150 requests, a random node killed mid-traffic, **0 failed** (≤ 1 retry), **0 lost** writes |
| Stretch | LRU eviction per node; optional quorum mode |
