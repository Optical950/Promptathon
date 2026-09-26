# Vault — Distributed Object Storage System
### End-to-end design: architecture, math, algorithms, reference implementation

---

## 1. Architecture Overview

Vault is split into two independently scalable planes that communicate only through
well-defined RPC/gRPC contracts. This decoupling is what lets the data plane hit
high throughput while the control plane stays small, consistent, and cheap to run.

```
                         ┌───────────────────────────────┐
                         │   METADATA CONTROL PLANE       │
                         │  (low QPS, strong consistency) │
                         │                                │
                         │  ┌──────────┐   ┌────────────┐ │
                         │  │ Raft RSM │   │ Volume Map │ │
                         │  │ (leader) │◄─►│ (VNode ring│ │
                         │  └──────────┘   │  + layout) │ │
                         │        ▲        └────────────┘ │
                         │        │ AppendEntries          │
                         │  ┌──────────┐   ┌────────────┐ │
                         │  │ Follower │   │  Gossip +  │ │
                         │  │  Raft x2 │   │ Phi-Accrual│ │
                         │  └──────────┘   │  Detector  │ │
                         │                 └────────────┘ │
                         └───────────────────────────────┘
                                       │  placement decisions,
                                       │  fencing tokens, node health
                                       ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │                    HIGH-THROUGHPUT DATA PLANE                    │
   │                                                                   │
   │  Client ──stream──► Chunker ──► {Replicator | Erasure Encoder}   │
   │                                        │                          │
   │              ┌─────────────────────────┼─────────────────────┐   │
   │              ▼                         ▼                     ▼   │
   │        StorageNode 1             StorageNode 2   ...   StorageNode N
   │        [chunk + checksum]        [chunk + checksum]   [chunk+chk] │
   │              │                                                   │
   │              ▼                                                   │
   │        Scrubber daemon → Merkle diff → Async Repair Queue        │
   └─────────────────────────────────────────────────────────────────┘
```

**Why this separation matters:** the control plane makes a placement/consensus
decision maybe thousands of times/sec (metadata ops), while the data plane moves
GB/s of bytes. Putting both in one consensus group (like naively running everything
through Raft) caps your write throughput at single-leader disk fsync speed. Vault
only puts the *volume map* and *node health* through consensus — chunk bytes never
touch the Raft log.

---

## 2. Data Model

```
Object  (bucket, key, version) → ObjectMeta
ObjectMeta {
    object_id       : uuid
    size             : int64
    checksum         : blake3(full object)
    vector_clock     : VectorClock
    durability_mode  : REPLICATED(n) | ERASURE(k, m)
    chunk_plan       : [ChunkLocation]
    created_at, etag
}
ChunkLocation {
    chunk_id       : uuid            # for replication: 1 id, N locations
    shard_index    : int             # for EC: 0..k+m-1
    node_ids       : [NodeID]        # placement result
    checksum       : sha256/blake3 of this specific chunk/shard
}
```

Large objects are split by the **Chunker** into fixed-size blocks (default 4–64MB,
tunable). Each block is independently placed, checksummed, and — depending on the
bucket's durability policy — either replicated N-way or erasure coded k+m.

---

## 3. Placement: Consistent Hashing with Virtual Nodes

Physical nodes are unreliable and join/leave constantly. Naive `hash(key) % N`
placement reshuffles ~100% of keys on any topology change. Consistent hashing with
virtual nodes bounds churn to `O(1/N)` of the keyspace per node event.

**Construction.** Each physical node `p` is assigned `V` virtual nodes (typically
100–256), each placed on a 2^64 ring at position `hash(p || vnode_index)`. To place
a chunk with id `c`, walk the ring clockwise from `hash(c)` and collect the first
`N_distinct_physical_nodes` encountered (skipping vnodes belonging to a physical
node already selected, so a chunk is never "replicated" onto the same box twice).

**Rebalancing bound.** Adding one physical node with `V` vnodes into a ring of
`P` existing physical nodes (with `V` vnodes each) moves only the keys that fall
between the new vnodes and their successors:

```
E[fraction of keyspace moved] ≈ 1 / (P + 1)
```

independent of total key count — this is the whole point of consistent hashing
over modulo hashing (which moves ~`(P-1)/P` of all keys on a resize).

**Placement diversity (failure-domain awareness).** A pure ring can place two
replicas on the same rack/AZ. Vault augments this with a CRUSH-like constraint:
when walking the ring, a candidate vnode is rejected if its physical node shares a
`failure_domain` (rack/AZ/power-shelf) with an already-selected node, guaranteeing
replicas/shards are spread across independent domains whenever topology allows it.

See `consistent_hashing.py`.

---

## 4. Durability Schemes

### 4.1 N-Way Replication with Quorums

For replication factor `N`, define write quorum `W` and read quorum `R`. Vault
enforces:

```
R + W > N        (guarantees read/write sets intersect → read sees latest write)
W > N/2          (guarantees at most one active write quorum at a time → no split brain on writes)
```

Typical config: `N=3, W=2, R=2` → tolerates 1 node down with zero downtime, and
`R+W=4 > 3`.

**Write path:** client sends chunk to all N replica nodes in parallel; the write is
ACKed to the caller as soon as `W` nodes durably persist + checksum-verify the
chunk. The remaining `N-W` are best-effort/async (hinted handoff if down).

**Read path:** client fans out to `R` (or more, for freshness racing) replicas,
compares checksums + vector clocks, returns newest; if replicas disagree, triggers
**read repair** (see §6).

**Availability math.** With per-node independent availability `a`, replicated
`N`-way with quorum `W` is available for reads/writes as long as at least `W`
(or `R`) of `N` nodes are up:

```
P(available) = Σ_{i=W}^{N} C(N,i) a^i (1-a)^(N-i)
```

### 4.2 Erasure Coding (Reed–Solomon, k+m)

Split each block into `k` equal-size data shards `D_1..D_k`, compute `m` parity
shards via a Reed–Solomon code over `GF(2^8)` using a Cauchy or Vandermonde
generator matrix `G` of shape `(k+m) × k`:

```
[ D_1  ]     [ I_k  ]
[ ...  ]  =  [------] × [D_1 .. D_k]^T     (encode)
[ D_k  ]     [ G_par]
[ P_1  ]
[ ...  ]
[ P_m  ]
```

Any `k` of the `k+m` shards (data or parity, mixed) suffice to reconstruct the
original data by inverting the corresponding `k×k` submatrix of `G` over
`GF(2^8)` and matrix-multiplying.

**Storage efficiency vs. replication:**

```
Efficiency_replication = 1/N                    e.g. N=3  → 33.3%
Efficiency_erasure     = k/(k+m)                 e.g. 10+4 → 71.4%
```

**Fault tolerance:** EC(k,m) tolerates the simultaneous loss of any `m` shards,
same as N=m+1 replication in *loss tolerance count*, but at far higher storage
efficiency and higher repair cost (repair requires reading `k` shards to
reconstruct 1, vs. reading 1 replica).

**When to use which:** replication for hot/small/latency-critical objects
(cheap, fast single-shard read); erasure coding for cold/large objects where
storage cost dominates. Vault makes this a per-bucket policy.

See `erasure_coding.py` for a from-scratch `GF(2^8)` Reed–Solomon implementation
(no external deps — pure math, matrix inversion via Gaussian elimination in the
field).

---

## 5. Consistency: Vector Clocks + MVCC

Every object version carries a `VectorClock: {node_id → counter}`. On write, the
coordinator increments its own entry and merges with the clock it read. Two
versions are:

- **A happens-before B** if `∀i: A[i] ≤ B[i]` and `∃i: A[i] < B[i])` → B is strictly newer, discard A.
- **Concurrent** if neither dominates → both are kept as sibling versions
  (MVCC: object key maps to a *set* of versions) and either resolved by
  last-writer-wins (timestamp tiebreak) or surfaced to the application
  (Dynamo-style siblings), depending on bucket policy.

This avoids relying on wall-clock time for correctness while still giving a
default LWW resolution policy for simplicity. See `vector_clock.py`.

**Why not put every write through Raft?** Raft gives linearizability but caps
throughput to one leader's disk fsync latency. Vector clocks + quorums give
tunable (R,W) consistency at N-times-parallel throughput. Vault uses Raft **only**
for metadata (volume map, node membership, bucket policy) — a low-QPS, small-state
workload for which strict consistency is cheap. Data writes use quorum/vector-clock
consistency for scale.

---

## 6. Integrity: Checksums + Read Repair

- Every chunk/shard is checksummed at write time (BLAKE3 preferred for speed;
  SHA-256 fallback; CRC32C optionally used for extremely cheap inline streaming
  verification during transfer, with BLAKE3/SHA-256 as the durable digest of
  record).
- On every read, the storage node recomputes the checksum before returning bytes.
  Mismatch → node returns `CorruptChunk`, coordinator fetches from another
  replica/shard, serves the client from the good copy, and **enqueues a repair
  job** to overwrite the corrupt copy (read repair).
- For replicated data, if the R replicas queried disagree (checksum or vector
  clock), the coordinator picks the causally-latest, serves it, and pushes it to
  the stale replicas asynchronously.

---

## 7. Failure Detection: Gossip + Phi Accrual

Binary up/down failure detectors (e.g. "3 missed heartbeats = dead") produce false
positives under GC pauses / transient network blips, causing needless repair
storms. Vault uses the **Phi Accrual Failure Detector** (Hayashibara et al.):

For each monitored node, maintain a sliding window of recent heartbeat
inter-arrival times. Fit them to a distribution (Vault uses the empirical
distribution / normal approximation) and compute:

```
φ(t) = -log10( P_later(t − t_last) )
```

where `P_later(Δ)` is the probability, under the fitted inter-arrival
distribution, that the gap since the last heartbeat could be at least `Δ` by
chance. A larger `φ` means "very unlikely this gap is normal" → more confidently
dead. Consumers pick a threshold (e.g. `φ > 8` ⇒ mark SUSPECT, `φ > 12` ⇒ mark
DOWN) instead of a hardcoded timeout, so the detector adapts automatically to each
node's own jitter characteristics and to overall cluster load.

Membership state (`ALIVE / SUSPECT / DOWN`) plus each node's latest phi value are
disseminated cluster-wide via **gossip** (each node, every gossip tick, picks a
few random peers and exchanges its membership table — O(log N) rounds to
converge), so no single node is a bottleneck for failure detection, unlike a
centralized heartbeat collector. See `phi_accrual.py`, `gossip.py`.

---

## 8. Anti-Entropy: Merkle Tree Sync

To detect replica divergence without transferring every chunk's bytes, each
storage node maintains a Merkle tree over the checksums of the chunks it holds
(leaves = per-chunk checksum, sorted by chunk id; internal nodes = hash of
children). To compare replica A vs B:

1. Exchange root hashes. Equal → done, no divergence, O(1) network cost.
2. Unequal → recursively exchange child-node hashes, descending only into
   subtrees whose hash differs. This isolates the divergent chunk ranges in
   `O(log(num_chunks))` comparisons instead of `O(num_chunks)`.
3. Only the specific divergent chunks are then shipped/repaired.

This runs continuously in the background (anti-entropy) independent of read
repair, catching silent divergence (e.g. a missed write during a partition) that
no client ever happened to read. See `merkle_tree.py`.

---

## 9. Self-Healing: Scrubber + Rate-Limited Repair Queue

A background **Scrubber** daemon per node walks local chunks on a duty cycle,
recomputing checksums to catch bitrot (silent disk corruption unrelated to node
crashes), and periodically runs Merkle-tree diffs against peer replicas/shard-mates.

Detected problems (missing chunk, checksum mismatch, missing shard) are pushed
onto an **Async Repair Queue**, processed by a bounded pool of repair workers with
a **token-bucket rate limiter** so background repair I/O never starves foreground
client read/write SLA:

```
tokens(t) = min(bucket_capacity, tokens(t-1) + refill_rate * Δt)
repair job runs only if tokens ≥ job_cost; else requeued with backoff
```

For replicated data, repair = copy chunk from a healthy replica. For erasure-coded
data, repair = read any `k` healthy shards, reconstruct via matrix inversion
(§4.2), and rewrite only the missing/corrupt shard(s) — deliberately *not*
re-encoding all `k+m` shards, to minimize repair I/O and repair network traffic
(this matters: EC repair bandwidth amplification is a well-known real-world cost,
so Vault always reconstructs the minimum shard set needed). See `scrubber.py`.

---

## 10. Split-Brain Prevention: Quorum + Fencing Tokens

Two independent mechanisms:

1. **Raft quorum** for the control plane: the volume map can only be mutated by
   a Raft leader elected by a strict majority (`⌊P/2⌋+1`) of metadata nodes, so a
   network partition can produce at most one side with a leader.
2. **Fencing tokens** for the data plane: every time a client (or repair worker)
   is granted write access to a chunk/lease, the metadata leader hands out a
   monotonically increasing **fencing token** (derived from the Raft log index).
   Storage nodes reject any write carrying a token lower than the highest token
   they've already seen for that chunk. This defeats the classic "paused/stale
   coordinator wakes up after a partition and overwrites newer data" race — even
   if a zombie coordinator thinks it still holds a lease, its stale token gets
   rejected by storage nodes that have since seen a newer one.

See `raft_lite.py` (leader election + log replication skeleton that issues
monotonic tokens) and `quorum.py` (token enforcement at the storage-node write
path).

---

## 11. Repository Map

| File | What it implements |
|---|---|
| `consistent_hashing.py` | VNode hash ring, placement with failure-domain diversity |
| `vector_clock.py` | Vector clocks, causal compare, MVCC sibling resolution |
| `merkle_tree.py` | Merkle tree build + recursive diff for anti-entropy |
| `phi_accrual.py` | Phi Accrual failure detector |
| `gossip.py` | Gossip membership dissemination using the phi detector |
| `erasure_coding.py` | GF(2^8) Reed–Solomon encode/decode/reconstruct |
| `quorum.py` | Quorum read/write coordinator, read repair, fencing enforcement |
| `raft_lite.py` | Minimal Raft (leader election, log replication, fencing tokens) |
| `storage_node.py` | Data-plane node: chunk put/get with checksum verify |
| `scrubber.py` | Background scrubber + rate-limited async repair queue |
| `metadata_service.py` | Ties control plane together: volume map + placement + Raft |
| `demo.py` | End-to-end runnable demo exercising all of the above |

Run `python demo.py` for a live walkthrough: cluster bootstrap, replicated write
+ quorum read, erasure-coded write + simulated shard loss + reconstruction,
node failure detection via phi accrual, and a Merkle-driven repair cycle.
