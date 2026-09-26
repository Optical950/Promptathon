"""
Background Scrubber + rate-limited Async Repair Queue (§9 of DESIGN.md).

Scrubber: walks each node's local chunks, re-verifies checksums (bitrot), and
runs Merkle-tree diffs against replica/shard peers to find missing/divergent
chunks that no client read happened to catch.

RepairQueue: a token-bucket rate limiter bounds background repair I/O so it
never starves foreground client SLA. Repair jobs for erasure-coded data
reconstruct only the missing shard(s) from any k healthy peers (minimizing
repair bandwidth), not a full re-encode.
"""

from __future__ import annotations
import itertools
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Optional

from storage_node import StorageNode, CorruptChunkError
from merkle_tree import MerkleTree
from erasure_coding import ReedSolomon


@dataclass
class RepairJob:
    chunk_id: str
    cost: float                       # e.g. proportional to shard size in MB
    kind: str                         # "replica_copy" | "ec_reconstruct"
    target_node: StorageNode
    source_nodes: list[StorageNode]
    shard_index: Optional[int] = None
    rs_codec: Optional[ReedSolomon] = None
    full_shard_set: Optional[list[Optional[StorageNode]]] = None  # index -> node or None


class TokenBucketLimiter:
    def __init__(self, capacity: float, refill_rate_per_sec: float):
        self.capacity = capacity
        self.refill_rate = refill_rate_per_sec
        self.tokens = capacity
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def try_consume(self, cost: float) -> bool:
        self._refill()
        if self.tokens >= cost:
            self.tokens -= cost
            return True
        return False


class AsyncRepairQueue:
    def __init__(self, rate_limiter: TokenBucketLimiter, fencing_token_source: Optional[Callable[[], int]] = None):
        """
        fencing_token_source: zero-arg callable returning a fresh, strictly
        increasing fencing token per repair write (in production: obtained
        from the Raft leader, same as client writes in quorum.py, so a
        repair write can never be shadowed by an older watermark). Defaults
        to a local monotonic counter for standalone/demo use.
        """
        self.rate_limiter = rate_limiter
        self._next_token = fencing_token_source or iter(itertools.count(1)).__next__
        self.queue: deque[RepairJob] = deque()
        self.repaired_count = 0
        self.deferred_count = 0

    def enqueue(self, job: RepairJob) -> None:
        self.queue.append(job)

    def process_one(self) -> bool:
        """Attempt to process the head of queue; requeue at the back (with
        implicit backoff) if rate-limited. Returns True if a job ran."""
        if not self.queue:
            return False
        job = self.queue.popleft()
        if not self.rate_limiter.try_consume(job.cost):
            self.queue.append(job)  # defer, try again next tick
            self.deferred_count += 1
            return False

        if job.kind == "replica_copy":
            self._repair_replica(job)
        elif job.kind == "ec_reconstruct":
            self._repair_ec_shard(job)
        self.repaired_count += 1
        return True

    def _repair_replica(self, job: RepairJob) -> None:
        for src in job.source_nodes:
            try:
                data = src.get_chunk(job.chunk_id)
                job.target_node.put_chunk(job.chunk_id, data, fencing_token=self._next_token())
                return
            except (CorruptChunkError, KeyError, ConnectionError):
                continue
        # all sources failed; drop for now (would alert operator in production)

    def _repair_ec_shard(self, job: RepairJob) -> None:
        assert job.rs_codec is not None and job.full_shard_set is not None and job.shard_index is not None
        available: list[Optional[bytes]] = []
        for node in job.full_shard_set:
            if node is None:
                available.append(None)
                continue
            try:
                available.append(node.get_chunk(job.chunk_id))
            except (CorruptChunkError, KeyError, ConnectionError):
                available.append(None)

        recovered_data_shards = job.rs_codec.reconstruct(available)
        # Re-derive just the missing shard by re-encoding (cheap: one matrix row)
        full = job.rs_codec.encode(recovered_data_shards)
        missing_bytes = full[job.shard_index]
        job.target_node.put_chunk(job.chunk_id, missing_bytes, fencing_token=self._next_token())


class Scrubber:
    def __init__(self, repair_queue: AsyncRepairQueue):
        self.repair_queue = repair_queue

    def scrub_replica_set(self, chunk_id: str, replicas: list[StorageNode]) -> None:
        """Bitrot check + presence check across a replica set; enqueue repairs."""
        healthy = []
        missing_or_corrupt = []
        for node in replicas:
            try:
                node.get_chunk(chunk_id)  # verify=True by default
                healthy.append(node)
            except (KeyError, CorruptChunkError, ConnectionError):
                missing_or_corrupt.append(node)

        if healthy and missing_or_corrupt:
            for target in missing_or_corrupt:
                self.repair_queue.enqueue(
                    RepairJob(chunk_id=chunk_id, cost=1.0, kind="replica_copy",
                              target_node=target, source_nodes=healthy)
                )

    def anti_entropy_pass(self, node_a: StorageNode, node_b: StorageNode) -> set[str]:
        """Merkle-diff two replicas' full chunk sets; return divergent chunk ids
        (caller enqueues appropriate repair jobs per divergent id)."""
        tree_a = MerkleTree(node_a.local_checksums())
        tree_b = MerkleTree(node_b.local_checksums())
        return tree_a.diff(tree_b)


if __name__ == "__main__":
    # Replica-set healing demo
    nodes = [StorageNode(f"node-{i}") for i in range(3)]
    for n in nodes:
        n.put_chunk("chunk-1", b"important-bytes", fencing_token=1)

    nodes[1].corrupt_for_testing("chunk-1")   # simulate bitrot
    del nodes[2]._store["chunk-1"]            # simulate a lost chunk

    limiter = TokenBucketLimiter(capacity=5, refill_rate_per_sec=100)  # generous for demo
    repair_tokens = iter(itertools.count(100))  # must exceed tokens already seen by nodes
    queue = AsyncRepairQueue(limiter, fencing_token_source=lambda: next(repair_tokens))
    scrubber = Scrubber(queue)

    scrubber.scrub_replica_set("chunk-1", nodes)
    print(f"Repair jobs queued: {len(queue.queue)}")
    while queue.queue:
        queue.process_one()
    print(f"Repairs completed: {queue.repaired_count}")
    print("node-1 now reads:", nodes[1].get_chunk("chunk-1"))
    print("node-2 now reads:", nodes[2].get_chunk("chunk-1"))

    # EC shard reconstruction demo
    rs = ReedSolomon(k=4, m=2)
    ec_nodes = [StorageNode(f"shard-{i}") for i in range(6)]
    from erasure_coding import pad_shards
    data_shards = pad_shards(b"erasure-coded-object-payload", k=4)
    all_shards = rs.encode(data_shards)
    for i, n in enumerate(ec_nodes):
        n.put_chunk("obj-1", all_shards[i], fencing_token=1)

    lost_index = 2
    del ec_nodes[lost_index]._store["obj-1"]  # simulate losing shard 2

    ec_limiter = TokenBucketLimiter(capacity=5, refill_rate_per_sec=100)
    ec_repair_tokens = iter(itertools.count(100))
    ec_queue = AsyncRepairQueue(ec_limiter, fencing_token_source=lambda: next(ec_repair_tokens))
    shard_set = [n for n in ec_nodes]
    ec_queue.enqueue(RepairJob(
        chunk_id="obj-1", cost=1.0, kind="ec_reconstruct",
        target_node=ec_nodes[lost_index],
        source_nodes=[],
        shard_index=lost_index,
        rs_codec=rs,
        full_shard_set=shard_set,
    ))
    ec_queue.process_one()
    print("Reconstructed shard matches original:",
          ec_nodes[lost_index].get_chunk("obj-1") == all_shards[lost_index])
