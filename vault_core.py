"""
VaultCluster: an in-process orchestration layer that wires every existing
Vault module (consistent_hashing, raft_lite, metadata_service, storage_node,
quorum, erasure_coding, scrubber) into one addressable service with a plain
Python API. This file does not change any existing module -- it only calls
the public methods they already expose, the same way demo.py and
test_vault.py do.

Deliberately framework-agnostic (no FastAPI/pydantic imports) so it can be
unit-tested on its own and reused by any transport (server.py wraps it as a
REST API).

Known simplification carried over from the original code: there is no
chunker.py in this codebase, so every object is stored as a single chunk
(no multi-MB block splitting), matching how demo.py / test_vault.py exercise
the system.
"""

from __future__ import annotations

import itertools
import time
from dataclasses import dataclass, field
from typing import Literal, Optional

from consistent_hashing import ConsistentHashRing, PhysicalNode
from raft_lite import RaftNode, Role
from metadata_service import MetadataService, DurabilityPolicy
from storage_node import StorageNode, CorruptChunkError, StaleFencingTokenError
from quorum import QuorumCoordinator, QuorumConfig, QuorumUnavailableError
from erasure_coding import ReedSolomon, pad_shards
from merkle_tree import MerkleTree
from scrubber import Scrubber, AsyncRepairQueue, TokenBucketLimiter


class VaultError(Exception):
    """Base class for all errors this layer raises (mapped to HTTP by server.py)."""


class NodeNotFoundError(VaultError):
    pass


class BucketNotFoundError(VaultError):
    pass


class ObjectNotFoundError(VaultError):
    pass


class WriteFailedError(VaultError):
    pass


class ReadFailedError(VaultError):
    pass


@dataclass
class ObjectRecord:
    bucket: str
    key: str
    chunk_id: str
    size: int
    mode: Literal["replication", "erasure"]
    placement: list[str]
    content_type: str = "application/octet-stream"
    filename: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    # replication-only
    n: int = 0
    w: int = 0
    r: int = 0
    # erasure-only
    k: int = 0
    m: int = 0


class VaultCluster:
    """
    One process-wide instance of this class stands in for the whole Vault
    deployment: the metadata control plane (Raft + ring) and the data plane
    (StorageNodes) that server.py's REST endpoints operate on.
    """

    def __init__(self, num_nodes: int = 6, vnodes_per_weight: int = 64):
        self.ring = ConsistentHashRing(vnodes_per_weight=vnodes_per_weight)
        self.storage_nodes: dict[str, StorageNode] = {}

        meta_ids = [f"meta-{i}" for i in range(3)]
        self.raft_cluster: dict[str, RaftNode] = {
            mid: RaftNode(mid, [p for p in meta_ids if p != mid]) for mid in meta_ids
        }
        self._elect_leader()

        self.metadata = MetadataService(self.ring, self.raft_cluster)
        self.metadata.set_bucket_policy("hot", DurabilityPolicy(mode="replication", n=3, w=2, r=2))
        self.metadata.set_bucket_policy("cold", DurabilityPolicy(mode="erasure", k=4, m=2))

        self.objects: dict[tuple[str, str], ObjectRecord] = {}
        self._repair_token_counter = itertools.count(10_000)
        self._total_repairs = 0

        for i in range(num_nodes):
            self.add_node(f"node-{i}", failure_domain=f"rack-{i % 3}")

    # ------------------------------------------------------------- bootstrap

    def _elect_leader(self, max_rounds: int = 50) -> None:
        for _ in range(max_rounds):
            for node in self.raft_cluster.values():
                node.tick(self.raft_cluster)
            if any(n.role == Role.LEADER for n in self.raft_cluster.values()):
                return

    def _leader(self) -> RaftNode:
        leaders = [n for n in self.raft_cluster.values() if n.role == Role.LEADER]
        if not leaders:
            # Leadership can be re-triggered by ticking again (simulates a
            # re-election after the previous leader was considered down).
            self._elect_leader()
            leaders = [n for n in self.raft_cluster.values() if n.role == Role.LEADER]
        if not leaders:
            raise VaultError("no Raft leader currently elected; metadata plane unavailable")
        return leaders[0]

    # ------------------------------------------------------------- node ops

    def list_nodes(self) -> list[dict]:
        out = []
        for node_id, pn in self.ring.nodes.items():
            sn = self.storage_nodes.get(node_id)
            out.append(
                {
                    "node_id": node_id,
                    "failure_domain": pn.failure_domain,
                    "weight": pn.weight,
                    "alive": pn.alive,
                    "chunk_count": len(sn.local_checksums()) if sn else 0,
                }
            )
        return sorted(out, key=lambda n: n["node_id"])

    def add_node(self, node_id: str, failure_domain: str = "default", weight: int = 1) -> dict:
        pn = PhysicalNode(node_id=node_id, failure_domain=failure_domain, weight=weight)
        expected_move = self.ring.preview_rebalance_fraction()
        self.ring.add_node(pn)
        self.storage_nodes[node_id] = StorageNode(node_id)
        return {"node_id": node_id, "expected_rebalance_fraction": expected_move}

    def remove_node(self, node_id: str) -> None:
        if node_id not in self.storage_nodes:
            raise NodeNotFoundError(node_id)
        self.ring.remove_node(node_id)
        self.storage_nodes.pop(node_id, None)

    def set_node_alive(self, node_id: str, alive: bool) -> None:
        if node_id not in self.storage_nodes:
            raise NodeNotFoundError(node_id)
        sn = self.storage_nodes[node_id]
        sn.come_back_up() if alive else sn.go_down()
        self.ring.mark_alive(node_id, alive)

    # ------------------------------------------------------------- raft/status

    def raft_status(self) -> list[dict]:
        return [
            {
                "node_id": n.node_id,
                "role": n.role.value,
                "term": n.current_term,
                "log_length": len(n.log),
                "commit_index": n.commit_index,
            }
            for n in self.raft_cluster.values()
        ]

    def overview(self) -> dict:
        nodes = self.list_nodes()
        leader = None
        try:
            leader = self._leader().node_id
        except VaultError:
            pass
        return {
            "leader": leader,
            "raft_term": self._leader().current_term if leader else None,
            "node_count": len(nodes),
            "alive_count": sum(1 for n in nodes if n["alive"]),
            "bucket_count": len(self.metadata.bucket_policies),
            "object_count": len(self.objects),
            "total_bytes": sum(o.size for o in self.objects.values()),
            "total_repairs": self._total_repairs,
        }

    # ------------------------------------------------------------- buckets

    def list_buckets(self) -> list[dict]:
        out = []
        for bucket, policy in self.metadata.bucket_policies.items():
            out.append(self._policy_to_dict(bucket, policy))
        return sorted(out, key=lambda b: b["bucket"])

    @staticmethod
    def _policy_to_dict(bucket: str, policy: DurabilityPolicy) -> dict:
        d = {"bucket": bucket, "mode": policy.mode, "shard_count": policy.shard_count()}
        if policy.mode == "replication":
            d.update(n=policy.n, w=policy.w, r=policy.r)
            d["efficiency"] = round(1 / policy.n, 3)
        else:
            d.update(k=policy.k, m=policy.m)
            d["efficiency"] = round(policy.k / (policy.k + policy.m), 3)
        return d

    def set_bucket_policy(
        self,
        bucket: str,
        mode: Literal["replication", "erasure"],
        n: int = 3,
        w: int = 2,
        r: int = 2,
        k: int = 4,
        m: int = 2,
    ) -> dict:
        policy = DurabilityPolicy(mode=mode, n=n, w=w, r=r, k=k, m=m)
        # QuorumConfig.__post_init__ validates R+W>N and W>N/2; surface the
        # same validation here so bad policies are rejected before they're
        # stored (same invariants quorum.py enforces at write time).
        if mode == "replication":
            QuorumConfig(n=n, w=w, r=r)
        self.metadata.set_bucket_policy(bucket, policy)
        return self._policy_to_dict(bucket, policy)

    # ------------------------------------------------------------- objects

    def write_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        filename: Optional[str] = None,
    ) -> ObjectRecord:
        if bucket not in self.metadata.bucket_policies:
            raise BucketNotFoundError(bucket)
        chunk_id = f"{bucket}:{key}"

        try:
            placement, policy, token = self.metadata.plan_write(bucket, chunk_id)
        except RuntimeError as e:
            raise WriteFailedError(str(e)) from e

        if policy.mode == "replication":
            replicas = [self.storage_nodes[nid] for nid in placement if nid in self.storage_nodes]
            qcfg = QuorumConfig(n=policy.n, w=policy.w, r=policy.r)
            token_iter = iter(range(token, token + 1000))
            coordinator = QuorumCoordinator(qcfg, fencing_token_source=lambda: next(token_iter))
            try:
                coordinator.write(chunk_id, data, replicas)
            except QuorumUnavailableError as e:
                raise WriteFailedError(str(e)) from e
            record = ObjectRecord(
                bucket=bucket, key=key, chunk_id=chunk_id, size=len(data), mode="replication",
                placement=placement, content_type=content_type, filename=filename,
                n=policy.n, w=policy.w, r=policy.r,
            )
        else:
            rs = ReedSolomon(k=policy.k, m=policy.m)
            shards = pad_shards(data, policy.k)
            all_shards = rs.encode(shards)
            nodes = [self.storage_nodes[nid] for nid in placement if nid in self.storage_nodes]
            if len(nodes) < policy.k + policy.m:
                raise WriteFailedError(
                    f"insufficient live nodes for erasure write: need {policy.k + policy.m}, have {len(nodes)}"
                )
            for i, node in enumerate(nodes):
                try:
                    node.put_chunk(chunk_id, all_shards[i], fencing_token=token)
                except (ConnectionError, StaleFencingTokenError) as e:
                    raise WriteFailedError(str(e)) from e
            record = ObjectRecord(
                bucket=bucket, key=key, chunk_id=chunk_id, size=len(data), mode="erasure",
                placement=placement, content_type=content_type, filename=filename,
                k=policy.k, m=policy.m,
            )

        self.objects[(bucket, key)] = record
        return record

    def list_objects(self, bucket: Optional[str] = None) -> list[ObjectRecord]:
        records = list(self.objects.values())
        if bucket is not None:
            records = [r for r in records if r.bucket == bucket]
        return sorted(records, key=lambda r: r.created_at, reverse=True)

    def read_object(self, bucket: str, key: str) -> tuple[bytes, ObjectRecord]:
        record = self.objects.get((bucket, key))
        if record is None:
            raise ObjectNotFoundError(f"{bucket}/{key}")

        if record.mode == "replication":
            nodes = [self.storage_nodes[nid] for nid in record.placement if nid in self.storage_nodes]
            if len(nodes) < record.n:
                raise ReadFailedError(
                    f"placement node missing (removed from cluster); need {record.n}, have {len(nodes)}"
                )
            qcfg = QuorumConfig(n=record.n, w=record.w, r=record.r)
            coordinator = QuorumCoordinator(qcfg, fencing_token_source=lambda: next(self._repair_token_counter))
            try:
                data = coordinator.read(record.chunk_id, nodes)
            except QuorumUnavailableError as e:
                raise ReadFailedError(str(e)) from e
            # Opportunistic read-repair: heal any replica that answered stale
            # or corrupt, the same mechanism quorum.py's drain_repairs offers.
            self._total_repairs += coordinator.drain_repairs()
            return data, record

        # erasure mode
        rs = ReedSolomon(k=record.k, m=record.m)
        shards: list[Optional[bytes]] = []
        for nid in record.placement:
            node = self.storage_nodes.get(nid)
            if node is None or not node.alive:
                shards.append(None)
                continue
            try:
                shards.append(node.get_chunk(record.chunk_id))
            except (KeyError, CorruptChunkError):
                shards.append(None)
        try:
            recovered = rs.reconstruct(shards)
        except ValueError as e:
            raise ReadFailedError(str(e)) from e
        data = b"".join(recovered)[: record.size]
        return data, record

    def delete_object(self, bucket: str, key: str) -> None:
        record = self.objects.pop((bucket, key), None)
        if record is None:
            raise ObjectNotFoundError(f"{bucket}/{key}")
        for nid in record.placement:
            node = self.storage_nodes.get(nid)
            if node is not None:
                node._store.pop(record.chunk_id, None)

    # ------------------------------------------------------------- repair

    def run_scrub(self) -> dict:
        """
        Walks every replicated object, Merkle-diffs its replicas, and repairs
        any divergence found -- the same flow demo.py section 5 exercises,
        just applied across every stored object instead of one hardcoded id.
        """
        limiter = TokenBucketLimiter(capacity=50, refill_rate_per_sec=500)
        repair_tokens = itertools.count(next(self._repair_token_counter) + 1)
        repair_queue = AsyncRepairQueue(limiter, fencing_token_source=lambda: next(repair_tokens))
        scrubber = Scrubber(repair_queue)

        objects_scanned = 0
        for record in self.objects.values():
            if record.mode != "replication":
                continue
            nodes = [self.storage_nodes[nid] for nid in record.placement if nid in self.storage_nodes]
            if len(nodes) < 2:
                continue
            objects_scanned += 1
            scrubber.scrub_replica_set(record.chunk_id, nodes)

        repaired = 0
        while repair_queue.queue:
            if repair_queue.process_one():
                repaired += 1
        self._total_repairs += repaired
        return {"objects_scanned": objects_scanned, "chunks_repaired": repaired}
