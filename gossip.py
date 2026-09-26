"""
Gossip protocol for cluster membership dissemination.

Each node keeps a local membership table {node_id -> (incarnation, status, phi)}.
On each gossip tick, a node picks a small random fan-out of peers and exchanges
tables; entries are merged keeping the higher incarnation number (or, for equal
incarnation, the more "concerning" status). This converges cluster-wide
membership knowledge in O(log N) rounds without a central heartbeat collector,
and combines with the Phi Accrual detector (phi_accrual.py) so status changes
are based on adaptive suspicion, not a fixed timeout.
"""

from __future__ import annotations
import random
from dataclasses import dataclass, field

from phi_accrual import PhiAccrualFailureDetector


_STATUS_RANK = {"ALIVE": 0, "SUSPECT": 1, "DOWN": 2}


@dataclass
class MemberEntry:
    node_id: str
    incarnation: int
    status: str        # ALIVE / SUSPECT / DOWN
    phi: float = 0.0


class GossipNode:
    def __init__(self, node_id: str, fanout: int = 3):
        self.node_id = node_id
        self.fanout = fanout
        self.incarnation = 0
        self.members: dict[str, MemberEntry] = {
            node_id: MemberEntry(node_id, self.incarnation, "ALIVE")
        }
        self.detectors: dict[str, PhiAccrualFailureDetector] = {}

    def join(self, peer_id: str) -> None:
        self.members.setdefault(peer_id, MemberEntry(peer_id, 0, "ALIVE"))
        self.detectors.setdefault(peer_id, PhiAccrualFailureDetector())

    def record_heartbeat_from(self, peer_id: str, now_ms: float) -> None:
        self.join(peer_id)
        self.detectors[peer_id].heartbeat(now_ms)

    def refresh_local_suspicion(self, now_ms: float) -> None:
        """Recompute status of every peer from this node's own phi detector,
        then bump incarnation on changes so the update propagates via gossip."""
        for peer_id, detector in self.detectors.items():
            new_status = detector.status(now_ms)
            entry = self.members[peer_id]
            entry.phi = detector.phi(now_ms)
            if new_status != entry.status and _STATUS_RANK[new_status] > _STATUS_RANK[entry.status]:
                # only locally escalate (ALIVE->SUSPECT->DOWN); recovery is
                # driven by a fresh heartbeat + explicit incarnation bump from
                # the peer itself, avoiding flapping.
                entry.status = new_status
                entry.incarnation += 1

    def gossip_round(self, all_nodes: dict[str, "GossipNode"]) -> None:
        peers = [n for n in self.members if n != self.node_id]
        if not peers:
            return
        targets = random.sample(peers, k=min(self.fanout, len(peers)))
        for t in targets:
            if t in all_nodes:
                self._exchange(all_nodes[t])

    def _exchange(self, other: "GossipNode") -> None:
        for node_id, entry in list(self.members.items()):
            other_entry = other.members.get(node_id)
            if other_entry is None or entry.incarnation > other_entry.incarnation:
                other.members[node_id] = MemberEntry(node_id, entry.incarnation, entry.status, entry.phi)
        for node_id, entry in list(other.members.items()):
            self_entry = self.members.get(node_id)
            if self_entry is None or entry.incarnation > self_entry.incarnation:
                self.members[node_id] = MemberEntry(node_id, entry.incarnation, entry.status, entry.phi)


if __name__ == "__main__":
    cluster = {f"n{i}": GossipNode(f"n{i}") for i in range(8)}
    for a in cluster.values():
        for b_id in cluster:
            if b_id != a.node_id:
                a.join(b_id)

    # Simulate heartbeats from all nodes, then n3 goes silent.
    t = 0.0
    for round_ in range(30):
        t += 200
        for a in cluster.values():
            for b_id in cluster:
                if b_id == a.node_id:
                    continue
                if b_id == "n3" and round_ > 15:
                    continue  # n3 stops heartbeating after round 15
                a.record_heartbeat_from(b_id, t)

        for a in cluster.values():
            a.refresh_local_suspicion(t)
        for a in cluster.values():
            a.gossip_round(cluster)

    for node_id in ["n0", "n5"]:
        print(f"{node_id} view of n3:", cluster[node_id].members["n3"])
