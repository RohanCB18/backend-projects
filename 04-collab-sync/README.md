# 04 · Real-Time Collaborative Sync Engine

Google-Docs-style live editing on a hand-written **RGA sequence CRDT**, with
the same algorithm in Python (`app/rga.py`, server) and JavaScript
(`../frontend/src/lib/rga.js`, every browser client).

```bash
..\.venv\Scripts\python -m uvicorn app.main:app --port 9104
```
```bash
..\.venv\Scripts\python -m pytest -q
```

## Why a CRDT and how RGA works

The document is "cat". Alice appends "s" at index 3 → "cats". At the same time
Bob inserts "h" at index 1 → "chat". If Bob applies "insert s at 3" literally
he gets "chast". Indexes shift under concurrent edits.

RGA never uses indexes. Every character gets a unique id `(lamport, site)`, and
an insert means "put c right **after** character X". Deletes only mark a
tombstone, so X keeps existing. To integrate "insert N after X": go to X, skip
every element whose id is greater than N's, and place N there. Concurrent
inserts after the same character end up ordered by id on every replica,
whatever order they arrive in. Ops whose reference hasn't arrived yet wait in a
pending buffer, which is what makes offline merging work.

## Protocol (WebSocket `/ws/{doc}`)

* client → `hello {since: N}`: server replies `sync` with every op after N
* client → `ops [...]`: server stores, acks, broadcasts to the others
* Each client keeps unacked ops in an **outbox**; on reconnect it resends the
  whole outbox. The op log has `UNIQUE(doc, op_key)`, so resends are dropped:
  no loss, no duplicates.
* Presence (who's online, cursor positions) uses a **separate** socket,
  `/presence/{doc}`, so cursor chatter never delays edits.
* History: the op log is the document. `GET /docs/{doc}/at/{seq}` replays it;
  `POST /docs/{doc}/revert {seq}` computes the edits that turn "now" into
  "then" and applies them as **new** ops, so a revert can itself be reverted.

## How each requirement is proven

| Req | Proof |
|---|---|
| R1 converge | `test_r1_cat_becomes_chats_on_both_sides`; 3 live editors in the console |
| R2 own CRDT | `app/rga.py` + `rga.js`, no library; `test_r2_concurrent_inserts_at_the_same_spot_are_ordered_identically` |
| R3 offline merge | `test_r3_offline_client_merges_without_losing_anyones_work`; "Offline merge" button |
| R4 history + revert | `test_r4_revert_to_any_point_and_revert_the_revert`, `test_r4_history_survives_server_restart`, HTTP revert broadcast test |
| R5 sub-second broadcast | `test_r5_edits_reach_other_clients_in_well_under_a_second` (real sockets, ~2–3 ms) |
| R6 convergence proof | Hypothesis property tests: 300 random 3-replica histories with partial gossip and shuffled delivery, plus 100 permutation tests. `test_python_and_javascript_crdts_agree` runs the JS CRDT in Node on 40 random histories and checks it matches Python character for character |
| R7 disconnect mid-edit | `test_r3_r7_disconnect_mid_edit_then_reconnect_no_loss_no_duplicates`, `test_r7_resent_ops_are_stored_once`; "drop mid-edit" button |
| Stretch | presence channel with live cursors (`test_stretch_presence_channel_reports_peers_and_cursors`) |
