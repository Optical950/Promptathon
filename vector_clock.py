"""
Vector clocks for causal ordering of concurrent object writes (MVCC-style).

Enables Vault to detect whether one version strictly supersedes another
(happens-before) or whether they are concurrent (siblings), without relying on
wall-clock time for correctness.
"""

from __future__ import annotations
import time
from dataclasses import dataclass, field
from enum import Enum


class Order(Enum):
    BEFORE = "before"          # self happened-before other
    AFTER = "after"            # self happened-after other
    EQUAL = "equal"
    CONCURRENT = "concurrent"


@dataclass
class VectorClock:
    counters: dict[str, int] = field(default_factory=dict)

    def increment(self, node_id: str) -> "VectorClock":
        new_counters = dict(self.counters)
        new_counters[node_id] = new_counters.get(node_id, 0) + 1
        return VectorClock(new_counters)

    def merge(self, other: "VectorClock") -> "VectorClock":
        keys = set(self.counters) | set(other.counters)
        merged = {k: max(self.counters.get(k, 0), other.counters.get(k, 0)) for k in keys}
        return VectorClock(merged)

    def compare(self, other: "VectorClock") -> Order:
        keys = set(self.counters) | set(other.counters)
        self_leq_other = all(self.counters.get(k, 0) <= other.counters.get(k, 0) for k in keys)
        other_leq_self = all(other.counters.get(k, 0) <= self.counters.get(k, 0) for k in keys)
        if self_leq_other and other_leq_self:
            return Order.EQUAL
        if self_leq_other:
            return Order.BEFORE
        if other_leq_self:
            return Order.AFTER
        return Order.CONCURRENT


@dataclass
class VersionedValue:
    value: bytes
    clock: VectorClock
    coordinator_node: str
    wall_clock_ts: float = field(default_factory=time.time)   # LWW tiebreak only


class MVCCStore:
    """
    Maps a key -> set of sibling VersionedValues. On write, prunes any sibling
    that the new version causally dominates. If the new version is concurrent
    with existing siblings, all are kept (Dynamo-style) unless the store is
    configured for last-writer-wins.
    """

    def __init__(self, last_writer_wins: bool = True):
        self.last_writer_wins = last_writer_wins
        self._store: dict[str, list[VersionedValue]] = {}

    def put(self, key: str, value: bytes, clock: VectorClock, node_id: str) -> list[VersionedValue]:
        existing = self._store.get(key, [])
        new_version = VersionedValue(value=value, clock=clock, coordinator_node=node_id)

        survivors: list[VersionedValue] = []
        dominated_by_new = False
        new_is_dominated = False

        for v in existing:
            order = clock.compare(v.clock)
            if order == Order.AFTER:
                continue  # new version supersedes this sibling -> drop it
            elif order == Order.BEFORE:
                new_is_dominated = True
                survivors.append(v)
            else:
                survivors.append(v)  # concurrent or equal -> keep as sibling

        if new_is_dominated:
            # A newer causal version already exists among siblings; still
            # record it as a sibling (could be concurrent with others).
            survivors.append(new_version)
        else:
            survivors.append(new_version)

        if self.last_writer_wins and len(survivors) > 1:
            # Resolve concurrent siblings by wall-clock tiebreak (documented
            # trade-off: can lose a concurrent write, acceptable per bucket policy)
            survivors = [max(survivors, key=lambda v: v.wall_clock_ts)]

        self._store[key] = survivors
        return survivors

    def get(self, key: str) -> list[VersionedValue]:
        return self._store.get(key, [])


if __name__ == "__main__":
    store = MVCCStore(last_writer_wins=False)

    vc_a = VectorClock().increment("coordinator-1")
    store.put("obj/foo", b"v1-from-node1", vc_a, "coordinator-1")

    # Concurrent write from a different coordinator that never saw vc_a
    vc_b = VectorClock().increment("coordinator-2")
    siblings = store.put("obj/foo", b"v1-from-node2", vc_b, "coordinator-2")
    print(f"After concurrent writes, siblings: {len(siblings)}")  # expect 2

    # A write that causally follows both (merged clock, then incremented)
    merged = vc_a.merge(vc_b).increment("coordinator-1")
    siblings = store.put("obj/foo", b"v2-merged", merged, "coordinator-1")
    print(f"After causal merge write, siblings: {len(siblings)}")  # expect 1
