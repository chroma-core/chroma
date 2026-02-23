#!/usr/bin/env python3
"""
Download Cohere Wikipedia embeddings and prepare recall-benchmark data.

Produces compact binary files consumed by the Rust recall benchmark:

    data__nogit/cohere_wiki/
        vectors_{N}.bin     -- N x 1024 f32 (little-endian, row-major)
        queries_{N}.bin     -- Q x 1024 f32
        ground_truth_{N}.bin -- Q x K u32 (brute-force KNN indices into vectors)
        meta_{N}.json       -- {n, dim, n_queries, k, dataset}

Usage:
    # Install deps (first time only):
    pip install datasets numpy tqdm

    # Prepare all three sizes:
    python prepare_dataset.py

    # Just one size:
    python prepare_dataset.py --sizes 10000

    # Custom query count or K:
    python prepare_dataset.py --n-queries 200 --k 100
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np

DATA_DIR = Path(__file__).resolve().parent / "data__nogit" / "cohere_wiki"

DATASET_NAME = "Cohere/wikipedia-2023-11-embed-multilingual-v3"
DATASET_CONFIG = "en"
DIM = 1024
DEFAULT_SIZES = [10_000, 100_000, 1_000_000]
DEFAULT_N_QUERIES = 100
DEFAULT_K = 100


def download_embeddings(n_needed: int) -> np.ndarray[Any, np.dtype[np.float32]]:
    """Stream the first `n_needed` embeddings from HuggingFace."""
    try:
        from datasets import load_dataset
    except ImportError:
        print(
            "ERROR: `datasets` package not installed.  Run:\n"
            "  pip install datasets\n",
            file=sys.stderr,
        )
        sys.exit(1)

    from tqdm import tqdm

    print(f"Loading {n_needed:,} embeddings from {DATASET_NAME} ({DATASET_CONFIG}) ...")
    ds = load_dataset(DATASET_NAME, DATASET_CONFIG, split="train", streaming=True)

    vecs = np.empty((n_needed, DIM), dtype=np.float32)
    for i, row in enumerate(tqdm(ds, total=n_needed, desc="downloading")):
        if i >= n_needed:
            break
        emb = row["emb"]
        if len(emb) != DIM:
            print(f"WARNING: row {i} has dim={len(emb)}, expected {DIM}; skipping")
            continue
        vecs[i] = emb
    return vecs[:i]


def brute_force_knn(
    vectors: np.ndarray[Any, np.dtype[np.float32]],
    queries: np.ndarray[Any, np.dtype[np.float32]],
    k: int,
) -> np.ndarray[Any, np.dtype[np.uint32]]:
    """Exact KNN (squared L2) using batched numpy. Returns (Q, K) indices."""
    from tqdm import tqdm

    n, d = vectors.shape
    q = queries.shape[0]
    gt = np.empty((q, k), dtype=np.uint32)

    # Precompute ||v||^2 once.
    v_norms_sq = np.sum(vectors * vectors, axis=1)  # (n,)

    batch_size = 256
    print(f"Computing brute-force KNN (n={n:,}, q={q}, k={k}) ...")
    for qi in tqdm(range(0, q, batch_size), desc="KNN"):
        q_batch = queries[qi : qi + batch_size]  # (bs, d)
        # ||v - q||^2 = ||v||^2 - 2 v.q + ||q||^2
        q_norms_sq = np.sum(q_batch * q_batch, axis=1)  # (bs,)
        dots = q_batch @ vectors.T  # (bs, n)
        dists = v_norms_sq[None, :] - 2 * dots + q_norms_sq[:, None]  # (bs, n)
        # Partial sort: O(n) per query instead of O(n log n).
        top_k_idx = np.argpartition(dists, k, axis=1)[:, :k]  # (bs, k)
        # Sort those k by distance so gt[i] is sorted nearest-first.
        for j in range(q_batch.shape[0]):
            idx = top_k_idx[j]
            order = np.argsort(dists[j, idx])
            gt[qi + j] = idx[order].astype(np.uint32)

    return gt


def save_binary(path: Path, arr: np.ndarray[Any, np.dtype[Any]]) -> None:
    arr = np.ascontiguousarray(arr)
    with open(path, "wb") as f:
        f.write(arr.tobytes())
    print(f"  wrote {path.name} ({path.stat().st_size / 1e6:.1f} MB)")


def prepare_size(
    all_vecs: np.ndarray[Any, np.dtype[np.float32]], n: int, n_queries: int, k: int
) -> None:
    assert n + n_queries <= all_vecs.shape[0], (
        f"Need {n + n_queries:,} vectors total but only have {all_vecs.shape[0]:,}. "
        f"Download more data."
    )

    # Split: first N are the database, next n_queries are queries.
    # This ensures queries are real embeddings but not in the database.
    vectors = all_vecs[:n]
    queries = all_vecs[n : n + n_queries]

    gt = brute_force_knn(vectors, queries, k)

    prefix = DATA_DIR
    prefix.mkdir(parents=True, exist_ok=True)

    save_binary(prefix / f"vectors_{n}.bin", vectors)
    save_binary(prefix / f"queries_{n}.bin", queries)
    save_binary(prefix / f"ground_truth_{n}.bin", gt)

    meta = {
        "n": n,
        "dim": DIM,
        "n_queries": n_queries,
        "k": k,
        "dataset": DATASET_NAME,
        "config": DATASET_CONFIG,
    }
    meta_path = prefix / f"meta_{n}.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"  wrote {meta_path.name}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--sizes",
        type=int,
        nargs="+",
        default=DEFAULT_SIZES,
        help="Database sizes to prepare (default: 10000 100000 1000000)",
    )
    parser.add_argument(
        "--n-queries",
        type=int,
        default=DEFAULT_N_QUERIES,
        help="Number of query vectors (default: 100)",
    )
    parser.add_argument(
        "--k", type=int, default=DEFAULT_K, help="Ground-truth K (default: 100)"
    )
    args = parser.parse_args()

    max_needed = max(args.sizes) + args.n_queries

    t0 = time.time()
    all_vecs = download_embeddings(max_needed)
    print(f"Downloaded {all_vecs.shape[0]:,} vectors in {time.time() - t0:.1f}s\n")

    for n in sorted(args.sizes):
        print(f"\n=== Preparing N={n:,} ===")
        prepare_size(all_vecs, n, args.n_queries, args.k)

    print(f"\nDone.  Data written to {DATA_DIR}/")
    print(f"Total time: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
