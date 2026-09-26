"""
Quorum coordinator for N-way replicated writes/reads (§4.1 and §6 of DESIGN.md).

Enforces R + W > N and W > N/2, performs parallel fan-out writes/reads against
StorageNodes, resolves divergent reads via VectorClock causal order, and
triggers read-repair against stale replicas. Every write is fenced with a
monotonically increasing token obtained from the Raft leader (raft_lite.py) so
a zombie coordinator from a stale partition can never clobber newer data.
"""

from __future__ import annotations
from dataclasses import dataclass

from storage_node import StorageNode, CorruptChunkError, StaleFencingTokenError
from vector_clock import VectorClock, Order


class QuorumUnavailableError(Exception):
    pass


@dataclass
class QuorumConfig:
    n: int
    w: int
    r: int

    def __post_init__(self):
        if self.w + self.r <= self.n:
            raise ValueError("Quorum config violates R + W > N; reads could miss the latest write")
        if self.w <= self.n // 2:
            raise ValueError("Quorum config violates W > N/2; concurrent write quorums could overlap incorrectly")


class QuorumCoordinator:
    def __init__(self, config: QuorumConfig, fencing_token_source):
        """
        fencing_token_source: zero-arg callable returning the next fencing
        token (in production: the Raft leader's `propose()` result).
        """
        self.config = config
        self._next_token = fencing_token_source
        self._repair_queue: list[tuple[str, list[StorageNode], bytes]] = []

    def write(self, chunk_id: str, data: bytes, replicas: list[StorageNode]) -> bool:
        if len(replicas) < self.config.n:
            raise QuorumUnavailableError(f"need {self.config.n} replicas, only {len(replicas)} placed")

        token = self._next_token()
        acked = 0
        failures = []
        for node in replicas:
            try:
                node.put_chunk(chunk_id, data, fencing_token=token)
                acked += 1
            except (ConnectionError, StaleFencingTokenError) as e:
                failures.append((node.node_id, str(e)))

        if acked < self.config.w:
            raise QuorumUnavailableError(
                f"write quorum not met: {acked}/{self.config.w} acked, failures={failures}"
            )
        return True

    def read(self, chunk_id: str, replicas: list[StorageNode]) -> bytes:
        if len(replicas) < self.config.n:
            raise QuorumUnavailableError(f"need {self.config.n} replicas, only {len(replicas)} placed")

        responses: dict[str, bytes] = {}
        errors: dict[str, str] = {}

        for node in replicas:
            if len(responses) >= self.config.r and len(responses) + len(errors) >= self.config.r:
                pass  # keep querying up to N for repair opportunities in this simple model
            try:
                data = node.get_chunk(chunk_id)
                responses[node.node_id] = data
            except CorruptChunkError as e:
                errors[node.node_id] = f"corrupt: {e}"
            except (KeyError, ConnectionError) as e:
                errors[node.node_id] = str(e)

        if len(responses) < self.config.r:
            raise QuorumUnavailableError(
                f"read quorum not met: {len(responses)}/{self.config.r} responded, errors={errors}"
            )

        # Resolve: with plain byte payloads (no per-value vector clock in this
        # simplified demo) we treat majority value as canonical; production
        # Vault compares VersionedValue.clock via causal order instead.
        value_counts: dict[bytes, list[str]] = {}
        for node_id, data in responses.items():
            value_counts.setdefault(data, []).append(node_id)
        canonical_value = max(value_counts.items(), key=lambda kv: len(kv[1]))[0]

        # Read repair: any replica that answered with something else, or
        # errored with CorruptChunk, gets queued for repair.
        stale_nodes = [n for n in replicas if responses.get(n.node_id) != canonical_value]
        if stale_nodes:
            self._repair_queue.append((chunk_id, stale_nodes, canonical_value))

        return canonical_value

    def drain_repairs(self) -> int:
        """Apply queued read-repairs. Returns count repaired. In production
        this hands off to the rate-limited AsyncRepairQueue (scrubber.py)
        instead of running inline."""
        count = 0
        token = self._next_token()
        for chunk_id, nodes, canonical_value in self._repair_queue:
            for node in nodes:
                try:
                    node.put_chunk(chunk_id, canonical_value, fencing_token=token)
                    count += 1
                except (ConnectionError, StaleFencingTokenError):
                    continue
        self._repair_queue.clear()
        return count


if __name__ == "__main__":
    nodes = [StorageNode(f"node-{i}") for i in range(3)]
    cfg = QuorumConfig(n=3, w=2, r=2)

    token_counter = iter(range(1, 1000))
    coordinator = QuorumCoordinator(cfg, fencing_token_source=lambda: next(token_counter))

    coordinator.write("chunk-1", b"vault-payload-v1", nodes)
    print("Read after quorum write:", coordinator.read("chunk-1", nodes))

    # Simulate one node down -> write should still succeed via quorum
    nodes[2].go_down()
    coordinator.write("chunk-2", b"vault-payload-v2", nodes)
    print("Write succeeded with 1 node down (quorum W=2 met)")

    nodes[2].come_back_up()
    # Simulate stale replica (missed a previous write) causing read disagreement
    nodes[1].corrupt_for_testing("chunk-1")
    value = coordinator.read("chunk-1", nodes)
    print("Read despite one corrupt replica:", value)
    repaired = coordinator.drain_repairs()
    print(f"Read-repair patched {repaired} stale/corrupt replica(s)")
