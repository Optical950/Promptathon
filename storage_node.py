"""
Data-plane StorageNode: the thing that actually holds bytes on disk (here, an
in-memory dict standing in for a local KV/blob engine, e.g. a WAL + LSM store).

Responsibilities kept intentionally narrow (decoupled from the control plane):
  - persist a chunk with its checksum
  - verify checksum on every read (bitrot detection)
  - enforce fencing tokens on writes (split-brain protection, §10 of DESIGN.md)
  - expose local chunk checksums for Merkle-based anti-entropy
"""

from __future__ import annotations
import hashlib
from dataclasses import dataclass, field


def checksum(data: bytes) -> bytes:
    # BLAKE3 preferred in production; SHA-256 used here for zero extra deps.
    return hashlib.sha256(data).digest()


class CorruptChunkError(Exception):
    pass


class StaleFencingTokenError(Exception):
    pass


@dataclass
class StoredChunk:
    data: bytes
    checksum: bytes
    fencing_token: int


class StorageNode:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.alive = True
        self._store: dict[str, StoredChunk] = {}
        self._highest_token_seen: dict[str, int] = {}   # per-chunk-id fencing watermark

    # ---------------------------------------------------------------- write

    def put_chunk(self, chunk_id: str, data: bytes, fencing_token: int) -> None:
        if not self.alive:
            raise ConnectionError(f"{self.node_id} is down")

        watermark = self._highest_token_seen.get(chunk_id, -1)
        if fencing_token <= watermark and chunk_id in self._store:
            # A stale coordinator (e.g. one that paused during a partition)
            # is trying to write with an old lease -> reject. This is the
            # concrete enforcement point for split-brain prevention.
            raise StaleFencingTokenError(
                f"token {fencing_token} <= watermark {watermark} for {chunk_id} on {self.node_id}"
            )

        self._store[chunk_id] = StoredChunk(data=data, checksum=checksum(data), fencing_token=fencing_token)
        self._highest_token_seen[chunk_id] = max(watermark, fencing_token)

    # ----------------------------------------------------------------- read

    def get_chunk(self, chunk_id: str, verify: bool = True) -> bytes:
        if not self.alive:
            raise ConnectionError(f"{self.node_id} is down")
        if chunk_id not in self._store:
            raise KeyError(f"{chunk_id} not found on {self.node_id}")

        stored = self._store[chunk_id]
        if verify and checksum(stored.data) != stored.checksum:
            raise CorruptChunkError(f"checksum mismatch for {chunk_id} on {self.node_id}")
        return stored.data

    def get_chunk_meta(self, chunk_id: str) -> StoredChunk | None:
        return self._store.get(chunk_id)

    # ------------------------------------------------------- anti-entropy

    def local_checksums(self) -> dict[str, bytes]:
        return {cid: sc.checksum for cid, sc in self._store.items()}

    def has_chunk(self, chunk_id: str) -> bool:
        return chunk_id in self._store

    # -------------------------------------------------------- fault inject

    def corrupt_for_testing(self, chunk_id: str) -> None:
        """Test-only helper: flips bytes to simulate bitrot without updating checksum."""
        if chunk_id in self._store:
            sc = self._store[chunk_id]
            corrupted = bytes([b ^ 0xFF for b in sc.data])
            self._store[chunk_id] = StoredChunk(data=corrupted, checksum=sc.checksum, fencing_token=sc.fencing_token)

    def go_down(self) -> None:
        self.alive = False

    def come_back_up(self) -> None:
        self.alive = True


if __name__ == "__main__":
    node = StorageNode("node-0")
    node.put_chunk("chunk-1", b"hello vault", fencing_token=1)
    print("Read back:", node.get_chunk("chunk-1"))

    try:
        node.put_chunk("chunk-1", b"stale zombie write", fencing_token=0)
    except StaleFencingTokenError as e:
        print("Correctly rejected stale write:", e)

    node.corrupt_for_testing("chunk-1")
    try:
        node.get_chunk("chunk-1")
    except CorruptChunkError as e:
        print("Correctly detected bitrot:", e)
