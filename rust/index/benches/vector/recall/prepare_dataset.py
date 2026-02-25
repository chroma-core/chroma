#!/usr/bin/env python3
"""
Download embedding datasets and prepare recall-benchmark data.

Produces compact binary files consumed by the Rust recall benchmark:

    data__nogit/{dataset}/
        vectors_{N}.bin     -- N x dim f32 (little-endian, row-major)
        queries_{N}.bin     -- Q x dim f32
        ground_truth_{N}.bin -- Q x K u32 (brute-force KNN indices into vectors)
        meta_{N}.json       -- {n, dim, n_queries, k, dataset}

Supported datasets:
    cohere_wiki   - Cohere/wikipedia-2023-11-embed-multilingual-v3 (en)
    msmarco       - Cohere/msmarco-v2-embed-multilingual-v3
    beir          - Cohere/beir-embed-english-v3 (msmarco subset)
    sec_filings   - Sicheng-Chroma/sec-filings

Usage:
    # Install deps (first time only):
    pip install datasets numpy tqdm

    # Prepare cohere_wiki (default) for all sizes:
    python prepare_dataset.py

    # Prepare a specific dataset:
    python prepare_dataset.py --dataset msmarco
    python prepare_dataset.py --dataset beir --sizes 10000 100000 1000000
    python prepare_dataset.py --dataset sec_filings --sizes 10000

    # Just one size:
    python prepare_dataset.py --dataset cohere_wiki --sizes 10000

    # Custom query count or K:
    python prepare_dataset.py --n-queries 200 --k 100
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

DATA_ROOT = Path(__file__).resolve().parent / "data__nogit"

DEFAULT_SIZES = [10_000, 100_000, 1_000_000]
DEFAULT_N_QUERIES = 100
DEFAULT_K = 100

# Dataset configs: slug -> {name, config, emb_column, corpus_config?, queries_config?, dim}
# For single-split datasets: load train, use first N as vectors, next n_queries as queries.
# For BEIR (corpus+queries): load corpus for vectors, queries split for query vectors.
DATASETS: Dict[str, Dict[str, Any]] = {
    "cohere_wiki": {
        "name": "Cohere/wikipedia-2023-11-embed-multilingual-v3",
        "config": "en",
        "emb_column": "emb",
        "dim": 1024,
        "streaming": True,
    },
    "msmarco": {
        "name": "Cohere/msmarco-v2-embed-multilingual-v3",
        "config": None,
        "emb_column": "emb",
        "dim": 1024,
        "streaming": True,
    },
    "beir": {
        "name": "Cohere/beir-embed-english-v3",
        "corpus_config": "msmarco-corpus",
        "queries_config": "msmarco-queries",
        "emb_column": "emb",
        "dim": 1024,
        "streaming": True,
    },
    "sec_filings": {
        "name": "Sicheng-Chroma/sec-filings",
        "config": None,
        "emb_column": "embedding",
        "dim": 1024,
        "streaming": True,
    },
}


def download_single_split(
    dataset_name: str,
    config: Optional[str],
    emb_column: str,
    n_needed: int,
    streaming: bool,
) -> np.ndarray[Any, np.dtype[np.float32]]:
    """Stream or load the first n_needed embeddings from a single-split dataset."""
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

    load_kw: Dict[str, Any] = {"split": "train", "streaming": streaming}
    if config:
        load_kw["name"] = config

    print(f"Loading {n_needed:,} embeddings from {dataset_name} ...")
    ds = load_dataset(dataset_name, **load_kw)

    vecs: List[List[float]] = []
    expected_dim: Optional[int] = None
    for i, row in enumerate(
        tqdm(ds, total=n_needed if streaming else None, desc="downloading")
    ):
        if i >= n_needed:
            break
        emb = row[emb_column]
        emb_list = list(emb)
        if expected_dim is None:
            expected_dim = len(emb_list)
        elif len(emb_list) != expected_dim:
            print(
                f"WARNING: row {i} has dim={len(emb_list)}, expected {expected_dim}; skipping"
            )
            continue
        vecs.append(emb_list)

    if not vecs:
        raise RuntimeError("No embeddings loaded from dataset")
    arr = np.array(vecs, dtype=np.float32)
    return arr


def download_beir(
    dataset_name: str,
    corpus_config: str,
    queries_config: str,
    emb_column: str,
    n_vectors: int,
    n_queries: int,
) -> Tuple[
    np.ndarray[Any, np.dtype[np.float32]], np.ndarray[Any, np.dtype[np.float32]]
]:
    """Load corpus and queries from BEIR (separate splits)."""
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

    print(
        f"Loading {n_vectors:,} corpus + {n_queries:,} queries from {dataset_name} ({corpus_config}) ..."
    )
    corpus_ds = load_dataset(dataset_name, corpus_config, split="train", streaming=True)
    queries_ds = load_dataset(dataset_name, queries_config, split="dev", streaming=True)

    vecs: List[List[float]] = []
    for i, row in enumerate(tqdm(corpus_ds, total=n_vectors, desc="corpus")):
        if i >= n_vectors:
            break
        vecs.append(list(row[emb_column]))
    if not vecs:
        raise RuntimeError("No corpus embeddings loaded")
    vectors = np.array(vecs, dtype=np.float32)

    qvecs: List[List[float]] = []
    for i, row in enumerate(tqdm(queries_ds, total=n_queries, desc="queries")):
        if i >= n_queries:
            break
        qvecs.append(list(row[emb_column]))
    if not qvecs:
        raise RuntimeError("No query embeddings loaded")
    queries = np.array(qvecs, dtype=np.float32)

    return vectors, queries


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

    v_norms_sq = np.sum(vectors * vectors, axis=1)
    batch_size = 256
    print(f"Computing brute-force KNN (n={n:,}, q={q}, k={k}) ...")
    for qi in tqdm(range(0, q, batch_size), desc="KNN"):
        q_batch = queries[qi : qi + batch_size]
        q_norms_sq = np.sum(q_batch * q_batch, axis=1)
        dots = q_batch @ vectors.T
        dists = v_norms_sq[None, :] - 2 * dots + q_norms_sq[:, None]
        kth = min(k - 1, n - 1)
        top_k_idx = np.argpartition(dists, kth, axis=1)[:, :k]
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
    vectors: np.ndarray[Any, np.dtype[np.float32]],
    queries: np.ndarray[Any, np.dtype[np.float32]],
    n: int,
    n_queries: int,
    k: int,
    dim: int,
    dataset_slug: str,
    dataset_name: str,
    config_info: Any,
) -> None:
    assert (
        vectors.shape[0] >= n
    ), f"Need {n:,} vectors but only have {vectors.shape[0]:,}."
    assert (
        queries.shape[0] >= n_queries
    ), f"Need {n_queries:,} queries but only have {queries.shape[0]:,}."
    assert vectors.shape[1] == dim and queries.shape[1] == dim

    v = vectors[:n]
    q = queries[:n_queries]
    gt = brute_force_knn(v, q, k)

    prefix = DATA_ROOT / dataset_slug
    prefix.mkdir(parents=True, exist_ok=True)

    save_binary(prefix / f"vectors_{n}.bin", v)
    save_binary(prefix / f"queries_{n}.bin", q)
    save_binary(prefix / f"ground_truth_{n}.bin", gt)

    meta = {
        "n": n,
        "dim": dim,
        "n_queries": n_queries,
        "k": k,
        "dataset": dataset_name,
        "config": config_info,
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
        "--dataset",
        "-d",
        choices=list(DATASETS),
        default="cohere_wiki",
        help="Dataset to prepare (default: cohere_wiki)",
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

    cfg: Dict[str, Any] = DATASETS[args.dataset]
    emb_column = cfg["emb_column"]
    t0 = time.time()
    config_info: Any

    if "corpus_config" in cfg:
        n_vectors = max(args.sizes)
        n_queries = args.n_queries
        vectors, queries = download_beir(
            cfg["name"],
            cfg["corpus_config"],
            cfg["queries_config"],
            emb_column,
            n_vectors,
            n_queries,
        )
        config_info = {
            "corpus": cfg["corpus_config"],
            "queries": cfg["queries_config"],
        }
        dim = int(vectors.shape[1])
    else:
        max_needed = max(args.sizes) + args.n_queries
        all_vecs = download_single_split(
            cfg["name"],
            cfg.get("config"),
            emb_column,
            max_needed,
            cfg.get("streaming", False),
        )
        vectors = all_vecs[: max(args.sizes)]
        queries = all_vecs[max(args.sizes) : max(args.sizes) + args.n_queries]
        config_info = cfg.get("config") or "default"
        dim = int(vectors.shape[1])

    print(
        f"Downloaded vectors {vectors.shape}, queries {queries.shape} in {time.time() - t0:.1f}s\n"
    )

    for n in sorted(args.sizes):
        if n > vectors.shape[0]:
            print(f"\nSkipping N={n:,} (only {vectors.shape[0]:,} vectors available)")
            continue
        print(f"\n=== Preparing {args.dataset} N={n:,} ===")
        prepare_size(
            vectors,
            queries,
            n,
            args.n_queries,
            args.k,
            dim,
            args.dataset,
            cfg["name"],
            config_info,
        )

    print(f"\nDone.  Data written to {DATA_ROOT / args.dataset}/")
    print(f"Total time: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
