#!/usr/bin/env python3
"""
Exact brute-force L2 kNN ground truth for MS MARCO v2 (Cohere embeddings), output
schema matches rust/index/benches/datasets/ground_truth.rs:

  query_vector, max_vector_id, neighbors_l2, neighbors_ip, neighbors_cosine

For each boundary B in {1M, 2M, ..., max_boundary}, neighbors are the K=100 smallest
L2^2 distances among corpus vector ids [0, B) (exclusive of B).

Queries: 100 deterministic random unit vectors (seed 42) — not in the database.

Implementation: one streaming pass per boundary (50 passes for 50M), vectorized NumPy
chunk processing. Heavy disk traffic (~sum_B B vector reads); run from fast NVMe.

Usage (full run):
  export HF_HOME=/mnt/data/huggingface
  pip install pyarrow numpy tqdm
  python3 compute_msmarco_ground_truth.py --hf-home "$HF_HOME" \\
    --output ~/.cache/msmarco_v2/ground_truth.parquet --max-boundary 50000000

Last N million-vector checkpoints only (e.g. last 50 boundaries 89M..138M), merge into existing GT:
  python3 compute_msmarco_ground_truth.py --hf-home "$HF_HOME" \\
    --min-boundary 89000000 --max-boundary 138000000 \\
    --extra-boundaries 138364198 \\
    --merge-existing ~/.cache/msmarco_v2/ground_truth.parquet \\
    --output ~/.cache/msmarco_v2/ground_truth.parquet

138364198 is the full MsMarco v2 corpus length (final checkpoint after 138 x 1M + remainder).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterator, Optional

import numpy as np

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from tqdm import tqdm
except ImportError as e:
    print("Install: pip install pyarrow numpy tqdm", file=sys.stderr)
    raise e

REPO = "Cohere/msmarco-v2-embed-multilingual-v3"
DIM = 1024
K = 100
NUM_SHARDS = 139
NUM_QUERIES = 100
COLUMN = "emb"
CHUNK_ROWS = 2048


def find_snapshot_root(hf_home_or_hub: Path) -> Path:
    p = hf_home_or_hub.expanduser().resolve()
    candidates = [
        p / "datasets--Cohere--msmarco-v2-embed-multilingual-v3",
        p / "hub" / "datasets--Cohere--msmarco-v2-embed-multilingual-v3",
    ]
    ds = next((c for c in candidates if c.is_dir()), None)
    if ds is None:
        raise FileNotFoundError(
            f"Could not find msmarco under {p}. "
            "Try: huggingface-cli download Cohere/msmarco-v2-embed-multilingual-v3 "
            "corpus/0000.parquet --repo-type dataset"
        )
    snaps = sorted((ds / "snapshots").glob("*"))
    if not snaps:
        raise FileNotFoundError(f"No snapshots under {ds / 'snapshots'}")
    return snaps[-1]


def iter_shard_rows(path: Path) -> Iterator[np.ndarray]:
    table = pq.read_table(path, columns=[COLUMN])
    col = table[COLUMN]
    # PyArrow 15+: is_null() takes no args and returns a BooleanArray (no is_null(i)).
    arr = col.combine_chunks()
    null_mask = arr.is_null()
    for i in range(len(arr)):
        if null_mask[i].as_py():
            continue
        v = arr[i].as_py()
        if v is None:
            continue
        a = np.asarray(v, dtype=np.float32)
        if a.shape != (DIM,):
            raise ValueError(f"bad dim {a.shape} in {path}")
        yield a


def stream_corpus_prefix(
    snapshot_dir: Path, max_n: int
) -> Iterator[tuple[int, np.ndarray]]:
    """Global id and vector, same order as Rust MsMarco::load_range."""
    gid = 0
    for shard in range(NUM_SHARDS):
        path = snapshot_dir / "corpus" / f"{shard:04d}.parquet"
        if not path.exists():
            raise FileNotFoundError(f"Missing {path}")
        for emb in iter_shard_rows(path):
            if gid >= max_n:
                return
            yield gid, emb
            gid += 1


def merge_topk(
    best_d: np.ndarray,
    best_i: np.ndarray,
    d2_chunk: np.ndarray,
    id_chunk: np.ndarray,
) -> None:
    """best_d (100,K), best_i (100,K); d2_chunk (100,n), id_chunk (n,). In-place merge."""
    for qi in range(NUM_QUERIES):
        cat_d = np.concatenate([best_d[qi], d2_chunk[qi]])
        cat_i = np.concatenate([best_i[qi], id_chunk])
        if len(cat_d) <= K:
            idx = np.argsort(cat_d)[:K]
        else:
            idx = np.argpartition(cat_d, K - 1)[:K]
            idx = idx[np.argsort(cat_d[idx])]
        best_d[qi] = cat_d[idx]
        best_i[qi] = cat_i[idx]


def run_boundary(
    snapshot_dir: Path,
    B: int,
    queries: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Return (neighbor_ids (100,K), dist_sq (100,K)) for corpus [0, B)."""
    q2 = np.sum(queries * queries, axis=1)
    best_d = np.full((NUM_QUERIES, K), np.inf, dtype=np.float64)
    best_i = np.full((NUM_QUERIES, K), -1, dtype=np.int64)

    buf: list[np.ndarray] = []
    buf_ids: list[int] = []
    pbar = tqdm(total=B, desc=f"B={B//1_000_000}M", unit="vec", leave=False)

    def flush() -> None:
        nonlocal buf, buf_ids
        if not buf:
            return
        X = np.stack(buf, axis=0)
        ids = np.array(buf_ids, dtype=np.int64)
        v2 = np.sum(X * X, axis=1)
        dots = queries @ X.T
        d2 = q2[:, None] + v2[None, :] - 2.0 * dots
        merge_topk(best_d, best_i, d2, ids)
        buf = []
        buf_ids = []

    for t, vec in stream_corpus_prefix(snapshot_dir, B):
        buf.append(vec)
        buf_ids.append(t)
        pbar.update(1)
        if len(buf) >= CHUNK_ROWS:
            flush()
    flush()
    pbar.close()

    if not np.all(np.isfinite(best_d[:, -1])):
        raise RuntimeError(f"boundary B={B}: not enough vectors for k={K}")
    return best_i.astype(np.uint32), best_d


def parse_extra_boundaries(s: str) -> list[int]:
    s = s.strip()
    if not s:
        return []
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def load_merge_base(path: Path, drop_max_ids: set[int]) -> Optional[pa.Table]:
    """Load existing parquet and drop all rows whose max_vector_id is in drop_max_ids."""
    if not path.exists():
        raise FileNotFoundError(f"--merge-existing not found: {path}")
    table = pq.read_table(path)
    if "max_vector_id" not in table.column_names:
        raise ValueError("merge file missing max_vector_id column")
    max_ids = table.column("max_vector_id").to_pylist()
    keep = [i for i, mid in enumerate(max_ids) if int(mid) not in drop_max_ids]
    if not keep:
        return None
    return table.take(keep)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--hf-home",
        type=Path,
        default=Path(os.environ.get("HF_HOME", Path.home() / ".cache/huggingface")),
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=Path.home() / ".cache/msmarco_v2/ground_truth.parquet",
    )
    ap.add_argument(
        "--min-boundary",
        type=int,
        default=1_000_000,
        help="First corpus prefix size B (multiple of 1_000_000). Default 1M.",
    )
    ap.add_argument("--max-boundary", type=int, default=50_000_000)
    ap.add_argument(
        "--extra-boundaries",
        type=str,
        default="",
        help="Comma-separated extra B values (e.g. final corpus size 138364198). "
        "Need not be a multiple of 1M.",
    )
    ap.add_argument(
        "--merge-existing",
        type=Path,
        default=None,
        help="Existing ground_truth.parquet: rows for computed boundaries are removed "
        "and replaced with newly computed rows. Read fully before writing --output.",
    )
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    if args.min_boundary % 1_000_000 != 0:
        ap.error("--min-boundary must be a multiple of 1_000_000")
    if args.max_boundary % 1_000_000 != 0:
        ap.error("--max-boundary must be a multiple of 1_000_000")
    if args.min_boundary > args.max_boundary:
        ap.error("--min-boundary must be <= --max-boundary")

    million_steps = list(range(args.min_boundary, args.max_boundary + 1, 1_000_000))
    extra = parse_extra_boundaries(args.extra_boundaries)
    boundaries = sorted(set(million_steps) | set(extra))
    if not boundaries:
        ap.error("no boundaries to compute (empty range and --extra-boundaries)")

    boundaries_set = set(boundaries)
    snapshot_dir = find_snapshot_root(args.hf_home)
    if not (snapshot_dir / "corpus" / "0000.parquet").exists():
        raise FileNotFoundError(f"No corpus/0000.parquet under {snapshot_dir}")

    rng = np.random.default_rng(args.seed)
    queries = rng.standard_normal((NUM_QUERIES, DIM)).astype(np.float32)
    queries /= np.linalg.norm(queries, axis=1, keepdims=True)

    rows_query: list[list[float]] = []
    rows_maxid: list[int] = []
    rows_l2: list[list[int]] = []
    rows_ip: list[list[int]] = []
    rows_cos: list[list[int]] = []

    for B in tqdm(boundaries, desc="boundaries"):
        nbr_ids, _dists = run_boundary(snapshot_dir, B, queries)
        for qi in range(NUM_QUERIES):
            nbrs = [int(x) for x in nbr_ids[qi].tolist()]
            rows_query.append(queries[qi].tolist())
            rows_maxid.append(B)
            rows_l2.append(nbrs)
            rows_ip.append(nbrs)
            rows_cos.append(nbrs)

    new_table = pa.table(
        {
            "query_vector": pa.array(rows_query, type=pa.list_(pa.float32(), DIM)),
            "max_vector_id": pa.array(rows_maxid, type=pa.uint64()),
            "neighbors_l2": pa.array(rows_l2, type=pa.list_(pa.uint32(), K)),
            "neighbors_ip": pa.array(rows_ip, type=pa.list_(pa.uint32(), K)),
            "neighbors_cosine": pa.array(rows_cos, type=pa.list_(pa.uint32(), K)),
        }
    )

    if args.merge_existing is not None:
        base = load_merge_base(args.merge_existing, boundaries_set)
        if base is not None:
            table = pa.concat_tables([base, new_table])
        else:
            table = new_table
    else:
        table = new_table

    args.output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, args.output)
    print(
        f"Wrote {table.num_rows} rows ({len(boundaries)} boundaries x {NUM_QUERIES} queries) to {args.output}"
    )


if __name__ == "__main__":
    main()
