"""
End-to-end demo: wires together every Vault component into one scenario.

Run: python demo.py
"""

from consistent_hashing import ConsistentHashRing, PhysicalNode
from raft_lite import RaftNode, Role
from metadata_service import MetadataService, DurabilityPolicy
from storage_node import StorageNode
from quorum import QuorumCoordinator, QuorumConfig
from erasure_coding import ReedSolomon, pad_shards
from scrubber import Scrubber, AsyncRepairQueue, TokenBucketLimiter, RepairJob
import itertools
from phi_accrual import PhiAccrualFailureDetector
from merkle_tree import MerkleTree


def section(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def main() -> None:
    # ------------------------------------------------------------------
    section("1. Bootstrap: placement ring + Raft metadata cluster")
    # ------------------------------------------------------------------
    ring = ConsistentHashRing(vnodes_per_weight=64)
    physical_nodes = {}
    for i in range(6):
        pn = PhysicalNode(node_id=f"node-{i}", failure_domain=f"rack-{i % 3}")
        physical_nodes[pn.node_id] = pn
        ring.add_node(pn)
    print(f"Ring bootstrapped with {len(physical_nodes)} physical nodes across 3 failure domains")

    meta_ids = [f"meta-{i}" for i in range(3)]
    raft_cluster = {i: RaftNode(i, [p for p in meta_ids if p != i]) for i in meta_ids}
    for _ in range(20):
        for n in raft_cluster.values():
            n.tick(raft_cluster)
        if any(n.role == Role.LEADER for n in raft_cluster.values()):
            break
    leader = next(n for n in raft_cluster.values() if n.role == Role.LEADER)
    print(f"Raft leader elected: {leader.node_id} (term {leader.current_term})")

    metadata = MetadataService(ring, raft_cluster)
    metadata.set_bucket_policy("hot", DurabilityPolicy(mode="replication", n=3, w=2, r=2))
    metadata.set_bucket_policy("cold", DurabilityPolicy(mode="erasure", k=4, m=2))

    storage_nodes = {nid: StorageNode(nid) for nid in physical_nodes}

    # ------------------------------------------------------------------
    section("2. Replicated write + quorum read (bucket: hot)")
    # ------------------------------------------------------------------
    placement, policy, token = metadata.plan_write("hot", "chunk-A")
    print(f"Placement decision: {placement}  fencing_token={token}")

    replicas = [storage_nodes[nid] for nid in placement]
    qcfg = QuorumConfig(n=policy.n, w=policy.w, r=policy.r)
    token_iter = iter(range(token, token + 1000))
    coordinator = QuorumCoordinator(qcfg, fencing_token_source=lambda: next(token_iter))

    coordinator.write("chunk-A", b"vault demo payload replicated", replicas)
    print("Quorum write acked by >=W replicas")
    print("Quorum read result:", coordinator.read("chunk-A", replicas))

    # ------------------------------------------------------------------
    section("3. Erasure-coded write + shard loss + reconstruction (bucket: cold)")
    # ------------------------------------------------------------------
    placement2, policy2, token2 = metadata.plan_write("cold", "chunk-B")
    print(f"Placement decision (k+m={policy2.k}+{policy2.m}): {placement2}")

    rs = ReedSolomon(k=policy2.k, m=policy2.m)
    payload = b"This object is erasure coded across 6 shards for storage efficiency."
    data_shards = pad_shards(payload, policy2.k)
    all_shards = rs.encode(data_shards)

    ec_nodes = [storage_nodes[nid] for nid in placement2]
    for i, node in enumerate(ec_nodes):
        node.put_chunk("chunk-B", all_shards[i], fencing_token=token2)
    print(f"Wrote {len(all_shards)} shards ({policy2.k} data + {policy2.m} parity), "
          f"storage efficiency {policy2.k/(policy2.k+policy2.m):.1%}")

    # simulate losing m=2 shards simultaneously
    lost = [0, 3]
    for idx in lost:
        del ec_nodes[idx]._store["chunk-B"]
    print(f"Simulated simultaneous loss of shards {lost} (== m, the max tolerated)")

    available = []
    for i, node in enumerate(ec_nodes):
        try:
            available.append(node.get_chunk("chunk-B"))
        except KeyError:
            available.append(None)

    recovered = rs.reconstruct(available)
    recovered_bytes = b"".join(recovered)[: len(payload)]
    print("Object correctly reconstructed from surviving shards:", recovered_bytes == payload)

    # ------------------------------------------------------------------
    section("4. Failure detection: Phi Accrual instead of fixed timeout")
    # ------------------------------------------------------------------
    fd = PhiAccrualFailureDetector()
    t = 0.0
    import random
    random.seed(42)
    for _ in range(20):
        t += 200 + random.uniform(-15, 15)
        fd.heartbeat(t)
    print("node-3 heartbeats stop now...")
    for silence_ms in (200, 800, 2000, 4000):
        print(f"  after {silence_ms:5d}ms silence: phi={fd.phi(t + silence_ms):5.2f}  status={fd.status(t + silence_ms)}")

    # ------------------------------------------------------------------
    section("5. Anti-entropy: Merkle diff finds divergence, scrubber repairs it")
    # ------------------------------------------------------------------
    r0, r1, r2 = replicas
    # simulate r1 missing a write that r0/r2 got (e.g. it was down during write)
    r1._store.pop("chunk-A", None)
    r0._store["chunk-A"].checksum  # no-op, just illustrating access

    tree0 = MerkleTree(r0.local_checksums(), num_buckets=64)
    tree1 = MerkleTree(r1.local_checksums(), num_buckets=64)
    diverging_buckets = tree0.diverging_buckets(tree1)
    print(f"Merkle diff isolated divergence to {len(diverging_buckets)}/{tree0.num_buckets} buckets "
          f"in O(log n) comparisons")
    print("Resolved divergent chunk ids:", tree0.diff(tree1))

    limiter = TokenBucketLimiter(capacity=5, refill_rate_per_sec=50)
    repair_tokens = iter(itertools.count(token + 100))  # must exceed watermark already on nodes
    repair_queue = AsyncRepairQueue(limiter, fencing_token_source=lambda: next(repair_tokens))
    scrubber = Scrubber(repair_queue)
    scrubber.scrub_replica_set("chunk-A", [r0, r1, r2])
    while repair_queue.queue:
        repair_queue.process_one()
    print(f"Repairs applied: {repair_queue.repaired_count}")
    print("r1 chunk-A after repair:", r1.get_chunk("chunk-A"))

    # ------------------------------------------------------------------
    section("6. Split-brain prevention: stale fencing token rejected")
    # ------------------------------------------------------------------
    from storage_node import StaleFencingTokenError
    current_token = token  # the token used for the original successful write
    try:
        r0.put_chunk("chunk-A", b"zombie coordinator trying to overwrite", fencing_token=current_token - 1)
        print("ERROR: stale write was NOT rejected (bug)")
    except StaleFencingTokenError as e:
        print("Correctly rejected zombie coordinator's stale write:", e)

    section("Demo complete.")


if __name__ == "__main__":
    main()
