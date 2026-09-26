import unittest
import itertools

from consistent_hashing import ConsistentHashRing, PhysicalNode
from raft_lite import RaftNode, Role
from metadata_service import MetadataService, DurabilityPolicy
from storage_node import StorageNode, StaleFencingTokenError
from quorum import QuorumCoordinator, QuorumConfig
from erasure_coding import ReedSolomon, pad_shards
from scrubber import (
    Scrubber,
    AsyncRepairQueue,
    TokenBucketLimiter,
)


class TestFullSystemIntegration(unittest.TestCase):

    def setUp(self):
        # ---------------------------------------------------------
        # 1. Create placement ring
        # ---------------------------------------------------------

        self.ring = ConsistentHashRing(
            vnodes_per_weight=32
        )

        self.physical_nodes = {}

        for i in range(6):
            node = PhysicalNode(
                node_id=f"node-{i}",
                failure_domain=f"rack-{i % 3}",
            )

            self.physical_nodes[node.node_id] = node
            self.ring.add_node(node)

        # ---------------------------------------------------------
        # 2. Create Raft metadata cluster
        # ---------------------------------------------------------

        meta_ids = [
            "meta-0",
            "meta-1",
            "meta-2",
        ]

        self.raft_cluster = {
            node_id: RaftNode(
                node_id,
                [
                    peer
                    for peer in meta_ids
                    if peer != node_id
                ],
            )
            for node_id in meta_ids
        }

        # Run election.
        for _ in range(20):
            for node in self.raft_cluster.values():
                node.tick(self.raft_cluster)

            if any(
                node.role == Role.LEADER
                for node in self.raft_cluster.values()
            ):
                break

        self.assertTrue(
            any(
                node.role == Role.LEADER
                for node in self.raft_cluster.values()
            )
        )

        # ---------------------------------------------------------
        # 3. Create Metadata Service
        # ---------------------------------------------------------

        self.metadata = MetadataService(
            self.ring,
            self.raft_cluster,
        )

        self.metadata.set_bucket_policy(
            "hot",
            DurabilityPolicy(
                mode="replication",
                n=3,
                w=2,
                r=2,
            ),
        )

        self.metadata.set_bucket_policy(
            "cold",
            DurabilityPolicy(
                mode="erasure",
                k=4,
                m=2,
            ),
        )

        # ---------------------------------------------------------
        # 4. Create storage nodes
        # ---------------------------------------------------------

        self.storage_nodes = {
            node_id: StorageNode(node_id)
            for node_id in self.physical_nodes
        }

    # =============================================================
    # Test 1: Replicated end-to-end write/read
    # =============================================================

    def test_replicated_end_to_end_flow(self):

        placement, policy, token = self.metadata.plan_write(
            "hot",
            "chunk-A",
        )

        self.assertEqual(
            len(placement),
            3,
        )

        self.assertEqual(
            policy.mode,
            "replication",
        )

        self.assertIsInstance(
            token,
            int,
        )

        replicas = [
            self.storage_nodes[node_id]
            for node_id in placement
        ]

        quorum_config = QuorumConfig(
            n=policy.n,
            w=policy.w,
            r=policy.r,
        )

        token_iter = itertools.count(token)

        coordinator = QuorumCoordinator(
            quorum_config,
            fencing_token_source=lambda: next(token_iter),
        )

        payload = b"Vault integration test payload"

        coordinator.write(
            "chunk-A",
            payload,
            replicas,
        )

        result = coordinator.read(
            "chunk-A",
            replicas,
        )

        self.assertEqual(
            result,
            payload,
        )

    # =============================================================
    # Test 2: Metadata lookup matches actual placement
    # =============================================================

    def test_metadata_lookup_matches_placement(self):

        placement, policy, token = self.metadata.plan_write(
            "hot",
            "chunk-B",
        )

        lookup_result = self.metadata.lookup(
            "hot",
            "chunk-B",
        )

        self.assertEqual(
            lookup_result,
            placement,
        )

    # =============================================================
    # Test 3: Erasure-coded write and reconstruction
    # =============================================================

    def test_erasure_coded_end_to_end_flow(self):

        placement, policy, token = self.metadata.plan_write(
            "cold",
            "chunk-C",
        )

        self.assertEqual(
            policy.mode,
            "erasure",
        )

        self.assertEqual(
            len(placement),
            6,
        )

        rs = ReedSolomon(
            k=policy.k,
            m=policy.m,
        )

        payload = (
            b"Vault erasure coded integration payload"
        )

        data_shards = pad_shards(
            payload,
            policy.k,
        )

        all_shards = rs.encode(
            data_shards
        )

        ec_nodes = [
            self.storage_nodes[node_id]
            for node_id in placement
        ]

        # Write every shard.
        for index, node in enumerate(ec_nodes):
            node.put_chunk(
                "chunk-C",
                all_shards[index],
                fencing_token=token,
            )

        # Simulate loss of two shards.
        lost = [0, 3]

        for index in lost:
            del ec_nodes[index]._store["chunk-C"]

        # Read surviving shards.
        available = []

        for node in ec_nodes:
            try:
                available.append(
                    node.get_chunk("chunk-C")
                )
            except KeyError:
                available.append(None)

        # Reconstruct.
        recovered = rs.reconstruct(
            available
        )

        recovered_payload = (
            b"".join(recovered)[:len(payload)]
        )

        self.assertEqual(
            recovered_payload,
            payload,
        )

    # =============================================================
    # Test 4: Scrubber repairs a missing replica
    # =============================================================

    def test_scrubber_repairs_missing_replica(self):

        placement, policy, token = self.metadata.plan_write(
            "hot",
            "chunk-D",
        )

        replicas = [
            self.storage_nodes[node_id]
            for node_id in placement
        ]

        payload = b"data that must survive"

        for node in replicas:
            node.put_chunk(
                "chunk-D",
                payload,
                fencing_token=token,
            )

        # Simulate node failure/lost chunk.
        damaged_node = replicas[1]

        del damaged_node._store["chunk-D"]

        limiter = TokenBucketLimiter(
            capacity=5,
            refill_rate_per_sec=0,
        )

        repair_tokens = itertools.count(
            token + 100
        )

        queue = AsyncRepairQueue(
            limiter,
            fencing_token_source=lambda: next(
                repair_tokens
            ),
        )

        scrubber = Scrubber(queue)

        scrubber.scrub_replica_set(
            "chunk-D",
            replicas,
        )

        self.assertEqual(
            len(queue.queue),
            1,
        )

        queue.process_one()

        self.assertEqual(
            queue.repaired_count,
            1,
        )

        self.assertEqual(
            damaged_node.get_chunk("chunk-D"),
            payload,
        )

    # =============================================================
    # Test 5: Stale fencing token is rejected
    # =============================================================

    def test_stale_fencing_token_is_rejected(self):

        placement, policy, token = self.metadata.plan_write(
            "hot",
            "chunk-E",
        )

        node = self.storage_nodes[
            placement[0]
        ]

        node.put_chunk(
            "chunk-E",
            b"valid-data",
            fencing_token=token,
        )

        with self.assertRaises(
            StaleFencingTokenError
        ):
            node.put_chunk(
                "chunk-E",
                b"stale-data",
                fencing_token=token - 1,
            )

        # Original data must remain.
        self.assertEqual(
            node.get_chunk("chunk-E"),
            b"valid-data",
        )

    # =============================================================
    # Test 6: Complete system handles multiple objects
    # =============================================================

    def test_multiple_objects(self):

        for i in range(5):

            chunk_id = f"chunk-{i}"

            placement, policy, token = (
                self.metadata.plan_write(
                    "hot",
                    chunk_id,
                )
            )

            replicas = [
                self.storage_nodes[node_id]
                for node_id in placement
            ]

            quorum_config = QuorumConfig(
                n=3,
                w=2,
                r=2,
            )

            token_iter = itertools.count(
                token
            )

            coordinator = QuorumCoordinator(
                quorum_config,
                fencing_token_source=lambda: next(
                    token_iter
                ),
            )

            payload = (
                f"payload-{i}".encode()
            )

            coordinator.write(
                chunk_id,
                payload,
                replicas,
            )

            result = coordinator.read(
                chunk_id,
                replicas,
            )

            self.assertEqual(
                result,
                payload,
            )


if __name__ == "__main__":
    unittest.main()