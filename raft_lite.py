"""
Minimal single-process Raft implementation for Vault's metadata control plane.

This is a teaching/hackathon-scale implementation of the core Raft protocol
(Ongaro & Ousterhout) sufficient to demonstrate: leader election by strict
majority, log replication with commit-index advancement, and issuing
monotonically increasing fencing tokens (= committed log index) that the data
plane uses to reject stale writes from zombie coordinators (split-brain
prevention, see quorum.py).

In production this would be swapped for etcd/raft, hashicorp/raft, or a real
network-transported implementation; the state machine and safety properties
below are the same shape.
"""

from __future__ import annotations
import random
from dataclasses import dataclass, field
from enum import Enum


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: dict   # e.g. {"op": "update_volume_map", "chunk_id": ..., "placement": [...]}


class RaftNode:
    def __init__(self, node_id: str, peer_ids: list[str]):
        self.node_id = node_id
        self.peer_ids = peer_ids  # other node ids in the cluster
        self.role = Role.FOLLOWER

        # persistent state
        self.current_term = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []

        # volatile state
        self.commit_index = -1
        self.last_applied = -1

        # leader-only volatile state
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self.election_timeout_ticks = random.randint(5, 10)
        self._ticks_since_heartbeat = 0

        self.applied_state: dict = {}   # the replicated volume map, once committed

    # ------------------------------------------------------------ elections

    def tick(self, cluster: dict[str, "RaftNode"]) -> None:
        self._ticks_since_heartbeat += 1
        if self.role != Role.LEADER and self._ticks_since_heartbeat >= self.election_timeout_ticks:
            self._start_election(cluster)

    def _start_election(self, cluster: dict[str, "RaftNode"]) -> None:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self._ticks_since_heartbeat = 0
        votes = 1  # vote for self

        last_log_index = len(self.log) - 1
        last_log_term = self.log[-1].term if self.log else 0

        for peer_id in self.peer_ids:
            peer = cluster.get(peer_id)
            if peer is None:
                continue
            granted = peer._handle_request_vote(
                candidate_term=self.current_term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
            )
            if granted:
                votes += 1

        majority = (len(self.peer_ids) + 1) // 2 + 1
        if votes >= majority:
            self._become_leader(cluster)
        else:
            self.role = Role.FOLLOWER

    def _handle_request_vote(
        self, candidate_term: int, candidate_id: str, last_log_index: int, last_log_term: int
    ) -> bool:
        if candidate_term < self.current_term:
            return False
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.voted_for = None
            self.role = Role.FOLLOWER

        my_last_term = self.log[-1].term if self.log else 0
        my_last_index = len(self.log) - 1
        candidate_log_ok = (last_log_term > my_last_term) or (
            last_log_term == my_last_term and last_log_index >= my_last_index
        )

        if (self.voted_for in (None, candidate_id)) and candidate_log_ok:
            self.voted_for = candidate_id
            self._ticks_since_heartbeat = 0
            return True
        return False

    def _become_leader(self, cluster: dict[str, "RaftNode"]) -> None:
        self.role = Role.LEADER
        next_idx = len(self.log)
        for peer_id in self.peer_ids:
            self.next_index[peer_id] = next_idx
            self.match_index[peer_id] = -1
        self._replicate_heartbeat(cluster)

    # ------------------------------------------------------------ log repl

    def propose(self, command: dict, cluster: dict[str, "RaftNode"]) -> int | None:
        """
        Leader-only: append a command to the log and replicate to a majority.
        Returns the fencing token (= committed log index) on success, else None.
        """
        if self.role != Role.LEADER:
            return None

        entry = LogEntry(term=self.current_term, index=len(self.log), command=command)
        self.log.append(entry)

        acked = 1  # self
        for peer_id in self.peer_ids:
            peer = cluster.get(peer_id)
            if peer is None:
                continue
            ok = peer._handle_append_entries(
                leader_term=self.current_term,
                leader_id=self.node_id,
                entries=[entry],
                leader_commit=self.commit_index,
                prev_log_index=entry.index - 1,
                prev_log_term=self.log[entry.index - 1].term if entry.index > 0 else 0,
            )
            if ok:
                acked += 1
                self.match_index[peer_id] = entry.index

        majority = (len(self.peer_ids) + 1) // 2 + 1
        if acked >= majority:
            self.commit_index = entry.index
            self._apply_committed()
            # propagate commit index to followers on next heartbeat
            self._replicate_heartbeat(cluster)
            return entry.index  # fencing token = committed Raft log index
        return None

    def _replicate_heartbeat(self, cluster: dict[str, "RaftNode"]) -> None:
        if self.role != Role.LEADER:
            return
        for peer_id in self.peer_ids:
            peer = cluster.get(peer_id)
            if peer is None:
                continue
            peer._handle_append_entries(
                leader_term=self.current_term,
                leader_id=self.node_id,
                entries=[],
                leader_commit=self.commit_index,
                prev_log_index=len(self.log) - 1,
                prev_log_term=self.log[-1].term if self.log else 0,
            )

    def _handle_append_entries(
        self,
        leader_term: int,
        leader_id: str,
        entries: list[LogEntry],
        leader_commit: int,
        prev_log_index: int,
        prev_log_term: int,
    ) -> bool:
        if leader_term < self.current_term:
            return False

        self.current_term = leader_term
        self.role = Role.FOLLOWER
        self.voted_for = leader_id
        self._ticks_since_heartbeat = 0

        for entry in entries:
            if entry.index < len(self.log):
                self.log[entry.index] = entry
            else:
                self.log.append(entry)

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            self._apply_committed()

        return True

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self._apply(entry.command)

    def _apply(self, command: dict) -> None:
        # Toy state machine: applies volume-map mutations into applied_state.
        op = command.get("op")
        if op == "update_volume_map":
            self.applied_state[command["chunk_id"]] = command["placement"]


if __name__ == "__main__":
    ids = [f"m{i}" for i in range(5)]
    cluster = {i: RaftNode(i, [p for p in ids if p != i]) for i in ids}

    # Run ticks until a leader emerges (election timeouts are randomized).
    leader = None
    for _ in range(20):
        for node in cluster.values():
            node.tick(cluster)
        leaders = [n for n in cluster.values() if n.role == Role.LEADER]
        if leaders:
            leader = leaders[0]
            break

    print(f"Elected leader: {leader.node_id if leader else 'none'}, term={leader.current_term if leader else '-'}")

    token1 = leader.propose({"op": "update_volume_map", "chunk_id": "chunk-1", "placement": ["n0", "n1", "n2"]}, cluster)
    token2 = leader.propose({"op": "update_volume_map", "chunk_id": "chunk-2", "placement": ["n3", "n4", "n0"]}, cluster)
    print(f"Fencing token for write 1: {token1}")
    print(f"Fencing token for write 2: {token2}  (must be > token1 -> {token2 > token1})")

    follower = [n for n in cluster.values() if n.role != Role.LEADER][0]
    print(f"Follower '{follower.node_id}' replicated state:", follower.applied_state)
