"""
Reed-Solomon erasure coding over GF(2^8), implemented from scratch (no external
deps) using a Vandermonde-derived encoding matrix.

encode(k, m, data_shards)   -> k data shards + m parity shards
reconstruct(k, m, shards)   -> original k data shards, given ANY k of the k+m
                                 shards (missing ones marked None)

Storage efficiency: k / (k+m), vs 1/N for N-way replication.
Fault tolerance: survives the loss of any m shards.
"""

from __future__ import annotations
from typing import Optional

# --------------------------------------------------------------------------
# GF(2^8) arithmetic, generator polynomial 0x11D (same field as AES/QR codes)
# --------------------------------------------------------------------------

_EXP = [0] * 512
_LOG = [0] * 256


def _init_tables():
    x = 1
    for i in range(255):
        _EXP[i] = x
        _LOG[x] = i
        x <<= 1
        if x & 0x100:
            x ^= 0x11D
    for i in range(255, 512):
        _EXP[i] = _EXP[i - 255]


_init_tables()


def gf_mul(a: int, b: int) -> int:
    if a == 0 or b == 0:
        return 0
    return _EXP[_LOG[a] + _LOG[b]]


def gf_div(a: int, b: int) -> int:
    if a == 0:
        return 0
    if b == 0:
        raise ZeroDivisionError("GF(256) division by zero")
    return _EXP[(_LOG[a] - _LOG[b]) % 255]


def gf_pow(a: int, power: int) -> int:
    if a == 0:
        return 0
    return _EXP[(_LOG[a] * power) % 255]


def gf_inverse(a: int) -> int:
    return _EXP[255 - _LOG[a]]


# --------------------------------------------------------------------------
# Matrix helpers over GF(256)
# --------------------------------------------------------------------------

Matrix = list  # list[list[int]]


def matmul(a: Matrix, b: Matrix) -> Matrix:
    rows, inner, cols = len(a), len(b), len(b[0])
    out = [[0] * cols for _ in range(rows)]
    for i in range(rows):
        for kk in range(inner):
            aik = a[i][kk]
            if aik == 0:
                continue
            for j in range(cols):
                out[i][j] ^= gf_mul(aik, b[kk][j])
    return out


def identity(n: int) -> Matrix:
    return [[1 if i == j else 0 for j in range(n)] for i in range(n)]


def matrix_inverse(m: Matrix) -> Matrix:
    """Gauss-Jordan elimination over GF(2^8)."""
    n = len(m)
    aug = [row[:] + idrow for row, idrow in zip(m, identity(n))]

    for col in range(n):
        pivot_row = next((r for r in range(col, n) if aug[r][col] != 0), None)
        if pivot_row is None:
            raise ValueError("Matrix is singular over GF(256); cannot invert")
        aug[col], aug[pivot_row] = aug[pivot_row], aug[col]

        inv_pivot = gf_inverse(aug[col][col])
        aug[col] = [gf_mul(v, inv_pivot) for v in aug[col]]

        for r in range(n):
            if r != col and aug[r][col] != 0:
                factor = aug[r][col]
                aug[r] = [aug[r][c] ^ gf_mul(factor, aug[col][c]) for c in range(2 * n)]

    return [row[n:] for row in aug]


def vandermonde_matrix(rows: int, cols: int) -> Matrix:
    """Rows use distinct nonzero GF(256) elements 1..rows as x-values."""
    return [[gf_pow(x, j) for j in range(cols)] for x in range(1, rows + 1)]


# --------------------------------------------------------------------------
# Reed-Solomon encode / reconstruct
# --------------------------------------------------------------------------

class ReedSolomon:
    def __init__(self, k: int, m: int):
        self.k = k
        self.m = m
        # Top k rows = identity (so first k output shards == data verbatim,
        # cheap reads when no shard is missing); bottom m rows = Vandermonde
        # parity rows, chosen so every k x k submatrix of the full (k+m) x k
        # matrix is invertible (MDS property of Vandermonde-derived codes).
        parity_rows = vandermonde_matrix(m, k)
        self.generator: Matrix = identity(k) + parity_rows  # (k+m) x k

    def encode(self, data_shards: list[bytes]) -> list[bytes]:
        assert len(data_shards) == self.k
        shard_len = len(data_shards[0])
        assert all(len(s) == shard_len for s in data_shards)

        data_matrix = [[b for b in shard] for shard in data_shards]  # k x shard_len
        # transpose-free byte-wise application: for each output row, XOR-mul
        out_bytes: list[bytearray] = [bytearray(shard_len) for _ in range(self.k + self.m)]
        for out_row in range(self.k + self.m):
            gen_row = self.generator[out_row]
            acc = bytearray(shard_len)
            for kk in range(self.k):
                coeff = gen_row[kk]
                if coeff == 0:
                    continue
                src = data_shards[kk]
                if coeff == 1:
                    for i in range(shard_len):
                        acc[i] ^= src[i]
                else:
                    for i in range(shard_len):
                        acc[i] ^= gf_mul(coeff, src[i])
            out_bytes[out_row] = acc
        return [bytes(row) for row in out_bytes]

    def reconstruct(self, shards: list[Optional[bytes]]) -> list[bytes]:
        """
        `shards` has length k+m; missing entries are None. Requires at least
        k non-None entries. Returns the original k data shards.
        """
        assert len(shards) == self.k + self.m
        available_idx = [i for i, s in enumerate(shards) if s is not None]
        if len(available_idx) < self.k:
            raise ValueError(f"Need at least {self.k} shards, only have {len(available_idx)}")

        chosen = available_idx[: self.k]
        shard_len = len(shards[chosen[0]])

        sub_generator = [self.generator[i] for i in chosen]   # k x k
        inv = matrix_inverse(sub_generator)                    # k x k

        chosen_shards = [shards[i] for i in chosen]            # k shards of shard_len
        recovered = [bytearray(shard_len) for _ in range(self.k)]
        for out_row in range(self.k):
            inv_row = inv[out_row]
            acc = recovered[out_row]
            for kk in range(self.k):
                coeff = inv_row[kk]
                if coeff == 0:
                    continue
                src = chosen_shards[kk]
                if coeff == 1:
                    for i in range(shard_len):
                        acc[i] ^= src[i]
                else:
                    for i in range(shard_len):
                        acc[i] ^= gf_mul(coeff, src[i])
        return [bytes(r) for r in recovered]


def pad_shards(data: bytes, k: int) -> list[bytes]:
    """Split `data` into k equal-length shards, zero-padding the tail."""
    shard_len = (len(data) + k - 1) // k
    shard_len = max(shard_len, 1)
    padded = data + b"\x00" * (shard_len * k - len(data))
    return [padded[i * shard_len:(i + 1) * shard_len] for i in range(k)]


if __name__ == "__main__":
    k, m = 4, 2
    rs = ReedSolomon(k, m)

    original = b"VaultObjectStorageDemoPayload!!!"  # arbitrary bytes
    data_shards = pad_shards(original, k)
    all_shards = rs.encode(data_shards)
    print(f"Encoded into {len(all_shards)} shards ({k} data + {m} parity)")

    # Simulate losing up to m=2 shards (any positions, mixed data/parity)
    lossy = list(all_shards)
    lossy[0] = None
    lossy[3] = None
    print("Lost shard indices: 0, 3")

    recovered = rs.reconstruct(lossy)
    recovered_bytes = b"".join(recovered)[: len(original)]
    print("Reconstruction matches original:", recovered_bytes == original)
    print(f"Storage efficiency: {k}/{k+m} = {k/(k+m):.1%}  (vs replication N=3 -> 33.3%)")
