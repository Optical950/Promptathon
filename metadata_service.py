"""
MetadataService: the control-plane facade a client/gateway talks to before
touching the data plane. Owns:
  - the consistent hash ring (placement decisions)
  - the Raft-lite cluster (durably replicated volume map + fencing tokens)
  - per-bucket durability policy (replication vs erasure coding)

Strictly does NOT touch chunk bytes -- it only ever returns *where* a chunk
should live and *what token* to write it with. All byte movement happens in
the data plane (storage_node.py, quorum.py, erasure_coding.py).
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Literal

from consistent_hashing import ConsistentHashRing, PhysicalNode
from raft_lite import RaftNode, Role


@dataclass
class DurabilityPolicy:
    mode: Literal["replication", "erasure"]
    n: int = 3          # replication factor, if mode == replication
    w: int = 2
    r: int = 2
    k: int = 4          # data shards, if mode == erasure
    m: int = 2           # parity shards, if mode == erasure

    def shard_count(self) -> int:
        return self.n if self.mode == "replication" else (self.k + self.m)


class MetadataService:
    def __init__(self, ring: ConsistentHashRing, raft_cluster: dict[str, RaftNode]):
        self.ring = ring
        self.raft_cluster = raft_cluster
        self.bucket_policies: dict[str, DurabilityPolicy] = {}

    def set_bucket_policy(self, bucket: str, policy: DurabilityPolicy) -> None:
        self.bucket_policies[bucket] = policy

    def _leader(self) -> RaftNode:
        leaders = [n for n in self.raft_cluster.values() if n.role == Role.LEADER]
        if not leaders:
            raise RuntimeError("no Raft leader currently elected; metadata plane unavailable")
        return leaders[0]

    def plan_write(self, bucket: str, chunk_id: str) -> tuple[list[str], DurabilityPolicy, int]:
        """
        Returns (placement_node_ids, durability_policy, fencing_token).
        Commits the placement decision through Raft so it's durable before
        any bytes are written (a crashed leader mid-write can't produce an
        inconsistent view of where a chunk is supposed to live).
        """
        policy = self.bucket_policies.get(bucket, DurabilityPolicy(mode="replication", n=3, w=2, r=2))
        placement = self.ring.get_placement(chunk_id, n=policy.shard_count())
        if len(placement) < policy.shard_count():
            raise RuntimeError(
                f"insufficient healthy nodes for policy {policy}: "
                f"need {policy.shard_count()}, ring has {len(placement)}"
            )

        leader = self._leader()
        token = leader.propose(
            {"op": "update_volume_map", "chunk_id": f"{bucket}/{chunk_id}", "placement": placement},
            self.raft_cluster,
        )
        if token is None:
            raise RuntimeError("failed to commit placement through Raft majority")

        return placement, policy, token

    def lookup(self, bucket: str, chunk_id: str) -> list[str] | None:
        leader = self._leader()
        return leader.applied_state.get(f"{bucket}/{chunk_id}")

    def node_join(self, node: PhysicalNode) -> float:
        """Add a node to the ring; returns expected fraction of keyspace moved."""
        expected_move = self.ring.preview_rebalance_fraction()
        self.ring.add_node(node)
        return expected_move

    def node_leave(self, node_id: str) -> None:
        self.ring.remove_node(node_id)


if __name__ == "__main__":
    ring = ConsistentHashRing(vnodes_per_weight=64)
    for i in range(6):
        ring.add_node(PhysicalNode(node_id=f"node-{i}", failure_domain=f"rack-{i % 3}"))

    ids = [f"meta-{i}" for i in range(3)]
    raft_cluster = {i: RaftNode(i, [p for p in ids if p != i]) for i in ids}
    for _ in range(20):
        for n in raft_cluster.values():
            n.tick(raft_cluster)
        if any(n.role == Role.LEADER for n in raft_cluster.values()):
            break

    svc = MetadataService(ring, raft_cluster)
    svc.set_bucket_policy("hot-bucket", DurabilityPolicy(mode="replication", n=3, w=2, r=2))
    svc.set_bucket_policy("cold-bucket", DurabilityPolicy(mode="erasure", k=4, m=2))

    placement, policy, token = svc.plan_write("hot-bucket", "chunk-abc")
    print(f"hot-bucket/chunk-abc -> placement={placement}, mode={policy.mode}, token={token}")

    placement2, policy2, token2 = svc.plan_write("cold-bucket", "chunk-xyz")
    print(f"cold-bucket/chunk-xyz -> placement={placement2}, mode={policy2.mode}, token={token2}")

    print("Lookup hot-bucket/chunk-abc:", svc.lookup("hot-bucket", "chunk-abc"))
