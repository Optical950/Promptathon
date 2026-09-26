"""
Consistent Hashing ring with Virtual Nodes (VNodes) for Vault's placement layer.

Bounds keyspace churn to ~1/(P+1) of the ring per node join/leave (P = physical
node count), unlike modulo hashing which reshuffles ~(P-1)/P of all keys.

Also supports failure-domain aware placement (CRUSH-like constraint): when
selecting N distinct physical nodes for a chunk, nodes sharing a failure domain
(rack/AZ) with an already-chosen node are skipped when possible.
"""

from __future__ import annotations
import bisect
import hashlib
from dataclasses import dataclass, field
from typing import Optional


def _hash(key: str) -> int:
    """64-bit hash position on the ring."""
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


@dataclass
class PhysicalNode:
    node_id: str
    failure_domain: str = "default"   # e.g. rack id / AZ id
    weight: int = 1                    # capacity-proportional vnode multiplier
    alive: bool = True


class ConsistentHashRing:
    def __init__(self, vnodes_per_weight: int = 128):
        self.vnodes_per_weight = vnodes_per_weight
        self._ring: dict[int, str] = {}        # ring position -> physical node_id
        self._sorted_positions: list[int] = []
        self.nodes: dict[str, PhysicalNode] = {}

    # ---------------------------------------------------------------- admin

    def add_node(self, node: PhysicalNode) -> None:
        self.nodes[node.node_id] = node
        vcount = self.vnodes_per_weight * node.weight
        for i in range(vcount):
            pos = _hash(f"{node.node_id}#vn{i}")
            self._ring[pos] = node.node_id
        self._rebuild_sorted()

    def remove_node(self, node_id: str) -> None:
        if node_id not in self.nodes:
            return
        node = self.nodes.pop(node_id)
        vcount = self.vnodes_per_weight * node.weight
        for i in range(vcount):
            pos = _hash(f"{node_id}#vn{i}")
            self._ring.pop(pos, None)
        self._rebuild_sorted()

    def mark_alive(self, node_id: str, alive: bool) -> None:
        if node_id in self.nodes:
            self.nodes[node_id].alive = alive

    def _rebuild_sorted(self) -> None:
        self._sorted_positions = sorted(self._ring.keys())

    # --------------------------------------------------------------- lookup

    def get_placement(
        self,
        key: str,
        n: int,
        respect_failure_domains: bool = True,
        only_alive: bool = True,
    ) -> list[str]:
        """
        Walk the ring clockwise from hash(key), returning up to `n` distinct
        physical node ids. Prefers spreading across distinct failure domains;
        falls back to reusing a domain only if there aren't enough distinct
        domains available (never returns the same physical node twice).
        """
        if not self._sorted_positions:
            return []

        start = _hash(key)
        idx = bisect.bisect_right(self._sorted_positions, start) % len(self._sorted_positions)

        chosen: list[str] = []
        chosen_domains: set[str] = set()
        fallback: list[str] = []
        seen_nodes: set[str] = set()

        total = len(self._sorted_positions)
        for step in range(total):
            pos = self._sorted_positions[(idx + step) % total]
            node_id = self._ring[pos]
            if node_id in seen_nodes:
                continue
            node = self.nodes.get(node_id)
            if node is None:
                continue
            if only_alive and not node.alive:
                continue
            seen_nodes.add(node_id)

            if respect_failure_domains and node.failure_domain in chosen_domains:
                fallback.append(node_id)
                continue

            chosen.append(node_id)
            chosen_domains.add(node.failure_domain)
            if len(chosen) == n:
                return chosen

        # not enough distinct-domain nodes; top up from fallback list
        for node_id in fallback:
            if len(chosen) == n:
                break
            chosen.append(node_id)

        return chosen

    def preview_rebalance_fraction(self) -> float:
        """Expected fraction of keyspace that moves on the next node add,
        given current physical node count P: ~ 1/(P+1)."""
        p = len(self.nodes)
        return 1.0 / (p + 1) if p >= 0 else 1.0


if __name__ == "__main__":
    ring = ConsistentHashRing(vnodes_per_weight=64)
    for i in range(6):
        ring.add_node(PhysicalNode(node_id=f"node-{i}", failure_domain=f"rack-{i % 3}"))

    placement = ring.get_placement("chunk-abc123", n=3)
    print("Placement for chunk-abc123:", placement)
    print("Domains used:", [ring.nodes[n].failure_domain for n in placement])
    print("Expected rebalance fraction on next join:", ring.preview_rebalance_fraction())
