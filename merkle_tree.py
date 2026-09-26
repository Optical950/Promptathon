"""
Merkle tree over per-chunk checksums, used for anti-entropy synchronization
between replicas / shard-mates (§8 of DESIGN.md).

Design note: a naive Merkle tree built by pairing a *sorted list* of leaves is
unstable under insertion/deletion -- removing one chunk shifts the pairing of
every leaf that sorts after it, so a single missing chunk makes the entire
tree disagree and anti-entropy degrades to O(n) instead of O(log n). Real
systems (Cassandra, Dynamo) avoid this by partitioning the keyspace into a
FIXED number of hash-range buckets up front; a chunk's bucket depends only on
its own id, never on what else exists. Insert/delete only ever changes the one
bucket (and its ancestors) a chunk hashes into, keeping tree shape stable and
diff cost O(log(num_buckets)) regardless of churn.
"""

from __future__ import annotations
import hashlib
from dataclasses import dataclass


def h(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _bucket_of(chunk_id: str, num_buckets: int) -> int:
    digest = hashlib.sha256(chunk_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % num_buckets


@dataclass
class MerkleNode:
    hash_: bytes
    left: "MerkleNode | None" = None
    right: "MerkleNode | None" = None
    bucket_index: int | None = None   # set only on leaves


class MerkleTree:
    """
    Fixed `num_buckets` leaves (must be a power of two). Each chunk hashes
    (via SHA-256 of its id) into exactly one bucket; a leaf's hash is the
    XOR-fold of that bucket's member (chunk_id, checksum) hashes, so members
    can be added/removed independently of every other bucket's contents.
    """

    def __init__(self, chunk_checksums: dict[str, bytes], num_buckets: int = 256):
        assert num_buckets > 0 and (num_buckets & (num_buckets - 1) == 0), "num_buckets must be a power of two"
        self.num_buckets = num_buckets
        self.buckets: dict[int, dict[str, bytes]] = {i: {} for i in range(num_buckets)}
        for cid, cksum in chunk_checksums.items():
            self.buckets[_bucket_of(cid, num_buckets)][cid] = cksum

        leaves = [self._leaf_hash(i) for i in range(num_buckets)]
        self.leaf_nodes = [MerkleNode(hash_=leaves[i], bucket_index=i) for i in range(num_buckets)]
        self.root = self._build(list(self.leaf_nodes))

    def _leaf_hash(self, bucket_index: int) -> bytes:
        members = self.buckets[bucket_index]
        if not members:
            return h(b"empty")
        acc = 0
        for cid, cksum in members.items():
            acc ^= int.from_bytes(h(cid.encode() + cksum), "big")
        return acc.to_bytes(32, "big")

    def _build(self, level: list[MerkleNode]) -> MerkleNode:
        if len(level) == 1:
            return level[0]
        next_level = []
        for i in range(0, len(level), 2):
            l, r = level[i], level[i + 1]
            next_level.append(MerkleNode(hash_=h(l.hash_ + r.hash_), left=l, right=r))
        return self._build(next_level)

    def root_hash(self) -> bytes:
        return self.root.hash_

    def diverging_buckets(self, other: "MerkleTree") -> set[int]:
        """O(log num_buckets) descent; returns the set of bucket indices
        whose contents differ between the two trees."""
        assert self.num_buckets == other.num_buckets
        out: set[int] = set()
        self._diff_recursive(self.root, other.root, out)
        return out

    def _diff_recursive(self, a: MerkleNode, b: MerkleNode, out: set[int]) -> None:
        if a.hash_ == b.hash_:
            return
        if a.bucket_index is not None:
            out.add(a.bucket_index)
            return
        self._diff_recursive(a.left, b.left, out)
        self._diff_recursive(a.right, b.right, out)

    def diff(self, other: "MerkleTree") -> set[str]:
        """Convenience: divergent bucket indices resolved down to the actual
        divergent chunk_ids (union of both sides' membership in those buckets).
        Only the divergent buckets are compared chunk-by-chunk -- the whole
        point being we never had to touch the non-divergent buckets."""
        divergent_chunks: set[str] = set()
        for b in self.diverging_buckets(other):
            a_members = self.buckets[b]
            b_members = other.buckets[b]
            for cid in set(a_members) | set(b_members):
                if a_members.get(cid) != b_members.get(cid):
                    divergent_chunks.add(cid)
        return divergent_chunks


if __name__ == "__main__":
    replica_a = {f"chunk-{i}": h(f"data-{i}".encode()) for i in range(1000)}
    replica_b = dict(replica_a)
    # simulate divergence: one chunk corrupted, one missing, one extra
    replica_b["chunk-42"] = h(b"CORRUPTED")
    del replica_b["chunk-99"]
    replica_b["chunk-1000"] = h(b"extra-on-b")

    tree_a = MerkleTree(replica_a, num_buckets=256)
    tree_b = MerkleTree(replica_b, num_buckets=256)

    print("Root A:", tree_a.root_hash().hex()[:16])
    print("Root B:", tree_b.root_hash().hex()[:16])
    diverging = tree_a.diverging_buckets(tree_b)
    print(f"Diverging buckets: {len(diverging)} / {tree_a.num_buckets}  (isolated via O(log n) descent)")
    print("Resolved divergent chunk ids:", sorted(tree_a.diff(tree_b)))
