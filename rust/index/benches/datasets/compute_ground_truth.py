#!/usr/bin/env python3
"""Compute ground truth nearest neighbors for the quantized_spann benchmark.

Produces a parquet file at ~/.cache/<dataset>/ground_truth.parquet with schema:
  - query_vector: list<f32>
  - max_vector_id: u64
  - neighbors_l2: list<u32>
  - neighbors_ip: list<u32>
  - neighbors_cosine: list<u32>

Usage:
  pip install huggingface_hub numpy pyarrow
  python compute_ground_truth.py --dataset wikipedia --max-vectors 5000000
  python compute_ground_truth.py --dataset dbpedia --num-queries 100 --k 100
"""

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DATASETS = {
    "dbpedia": {
        "hf_repo": "KShivendu/dbpedia-entities-openai-1M",
        "column": "openai",
        "n_vectors": 1_000_000,
        "dim": 1536,
        "cache_dir": "dbpedia",
        "shard_pattern": "data/train-{:05d}-of-00026-*.parquet",
        "n_shards": 26,
    },
    "arxiv": {
        "hf_repo": "bluuebunny/arxiv_abstract_embedding_mxbai_large_v1_milvus",
        "column": "vector",
        "n_vectors": 2_922_184,
        "dim": 1024,
        "cache_dir": "arxiv_mxbai",
        "shard_names": [f"data/{y}.parquet" for y in range(1991, 2026)],
    },
    "sec": {
        "hf_repo": "Sicheng-Chroma/sec-filings",
        "column": "embedding",
        "n_vectors": 1_713_392,
        "dim": 1536,
        "cache_dir": "sec_filings",
    },
    "msmarco": {
        "hf_repo": "Cohere/msmarco-v2-embed-multilingual-v3",
        "column": "emb",
        "n_vectors": 138_364_198,
        "dim": 1024,
        "cache_dir": "msmarco_v2",
        "shard_pattern": "data/train-{:05d}-of-00139-*.parquet",
        "n_shards": 139,
    },
    "wikipedia": {
        "hf_repo": "Cohere/wikipedia-2023-11-embed-multilingual-v3",
        "column": "emb",
        "n_vectors": 41_488_110,
        "dim": 1024,
        "cache_dir": "wikipedia_en",
        "shard_pattern": "en/{:04d}.parquet",
        "n_shards": 415,
    },
}

BATCH_SIZE = 1_000_000


def resolve_shard_names(config):
    """Get the list of shard filenames for a dataset."""
    if "shard_names" in config:
        return config["shard_names"]
    if "shard_pattern" in config:
        pat = config["shard_pattern"]
        if "*" not in pat:
            return [pat.format(i) for i in range(config["n_shards"])]
    from huggingface_hub import HfApi

    api = HfApi()
    files = api.list_repo_files(config["hf_repo"], repo_type="dataset")
    return sorted(f for f in files if f.endswith(".parquet"))


def download_shard(repo_id, filename, cache_dir):
    """Download a single shard file, returning its local path."""
    from huggingface_hub import hf_hub_download

    return hf_hub_download(
        repo_id=repo_id,
        filename=filename,
        repo_type="dataset",
        cache_dir=cache_dir,
    )


def load_vectors_fast(config, max_vectors=None):
    """Load vectors by downloading parquet shards in parallel and reading with pyarrow.

    Much faster than datasets streaming: parallel HTTP downloads + columnar reads.
    """
    hf_repo = config["hf_repo"]
    col = config["column"]
    dim = config["dim"]

    n = config["n_vectors"]
    if max_vectors is not None:
        n = min(n, max_vectors)

    shard_names = resolve_shard_names(config)
    if shard_names is None:
        print("No shard info available, falling back to datasets streaming")
        return load_vectors_streaming(config, max_vectors)

    print(f"Loading up to {n:,} vectors from {hf_repo} via parallel shard download...")
    t0 = time.time()

    hf_cache = os.path.join(os.path.expanduser("~"), ".cache", "huggingface", "hub")

    vectors = np.empty((n, dim), dtype=np.float32)
    count = 0
    shard_idx = 0
    download_workers = 4

    vecs_per_shard = max(1, config["n_vectors"] // len(shard_names))
    shards_needed = min(
        len(shard_names), (n + vecs_per_shard - 1) // vecs_per_shard + 1
    )
    shards_to_download = shard_names[:shards_needed]
    print(
        f"  Downloading {len(shards_to_download)} of {len(shard_names)} shards (~{vecs_per_shard:,} vecs/shard)..."
    )

    downloaded_paths = {}
    with ThreadPoolExecutor(max_workers=download_workers) as executor:
        futures = {}
        for name in shards_to_download:
            f = executor.submit(download_shard, hf_repo, name, hf_cache)
            futures[f] = name

        for future in as_completed(futures):
            name = futures[future]
            try:
                path = future.result()
                downloaded_paths[name] = path
            except Exception as e:
                print(f"  WARNING: Failed to download {name}: {e}")

    for name in shards_to_download:
        if count >= n:
            break
        if name not in downloaded_paths:
            continue

        path = downloaded_paths[name]
        shard_idx += 1

        try:
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=50_000, columns=[col]):
                if count >= n:
                    break

                col_data = batch.column(col)
                flat = col_data.values.to_numpy(zero_copy_only=False).astype(np.float32)
                batch_vecs = flat.reshape(-1, dim)
                take = min(batch_vecs.shape[0], n - count)
                vectors[count : count + take] = batch_vecs[:take]
                count += take
        except Exception as e:
            print(f"  WARNING: Failed to read {name}: {e}")
            continue

        if count % 500_000 < 100_000 or shard_idx <= 2:
            elapsed = time.time() - t0
            rate = count / elapsed if elapsed > 0 else 0
            eta = (n - count) / rate if rate > 0 else 0
            print(
                f"  {count:,}/{n:,} vectors from {shard_idx} shards ({elapsed:.0f}s, ~{eta:.0f}s remaining)"
            )

    vectors = vectors[:count]
    print(f"  Loaded {count:,} vectors in {time.time()-t0:.1f}s")
    return vectors


def load_vectors_streaming(config, max_vectors=None):
    """Fallback: load vectors via HuggingFace datasets streaming."""
    from datasets import load_dataset

    hf_repo = config["hf_repo"]
    hf_config = config.get("hf_config")
    col = config["column"]
    dim = config["dim"]

    n = config["n_vectors"]
    if max_vectors is not None:
        n = min(n, max_vectors)

    load_kwargs = {"split": "train", "streaming": True}
    if hf_config is not None:
        load_kwargs["name"] = hf_config

    print(f"Loading {n:,} vectors from {hf_repo} (streaming)...")
    t0 = time.time()
    ds = load_dataset(hf_repo, **load_kwargs)

    vectors = np.empty((n, dim), dtype=np.float32)
    count = 0
    for row in ds:
        if count >= n:
            break
        vectors[count] = np.asarray(row[col], dtype=np.float32)
        count += 1
        if count % 100_000 == 0:
            print(f"  Loaded {count}/{n} vectors ({time.time()-t0:.1f}s)")

    vectors = vectors[:count]
    print(f"  Loaded {count:,} vectors in {time.time()-t0:.1f}s")
    return vectors


def brute_force_knn_l2(data, queries, k):
    """Batch L2 KNN using matrix multiply: ||a-b||^2 = ||a||^2 + ||b||^2 - 2*a.b"""
    data_norms = np.sum(data * data, axis=1)
    query_norms = np.sum(queries * queries, axis=1)
    dots = queries @ data.T
    dists = data_norms[None, :] - 2 * dots + query_norms[:, None]

    n_queries = queries.shape[0]
    neighbors = np.empty((n_queries, k), dtype=np.uint32)
    kth = min(k - 1, data.shape[0] - 1)
    for i in range(n_queries):
        top_k = np.argpartition(dists[i], kth)[:k]
        neighbors[i] = top_k[np.argsort(dists[i, top_k])].astype(np.uint32)
    return neighbors


def brute_force_knn_ip(data, queries, k):
    """Batch inner product KNN (largest = closest)."""
    scores = queries @ data.T

    n_queries = queries.shape[0]
    neighbors = np.empty((n_queries, k), dtype=np.uint32)
    kth = min(k - 1, data.shape[0] - 1)
    for i in range(n_queries):
        top_k = np.argpartition(-scores[i], kth)[:k]
        neighbors[i] = top_k[np.argsort(-scores[i, top_k])].astype(np.uint32)
    return neighbors


def brute_force_knn_cosine(data, queries, k):
    """Batch cosine KNN."""
    data_norms = np.linalg.norm(data, axis=1, keepdims=True)
    data_normed = data / np.maximum(data_norms, 1e-10)
    query_norms = np.linalg.norm(queries, axis=1, keepdims=True)
    queries_normed = queries / np.maximum(query_norms, 1e-10)
    return brute_force_knn_ip(data_normed, queries_normed, k)


def main():
    parser = argparse.ArgumentParser(
        description="Compute ground truth for quantized_spann benchmark"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="dbpedia",
        choices=list(DATASETS.keys()) + ["all"],
    )
    parser.add_argument("--num-queries", type=int, default=100)
    parser.add_argument("--k", type=int, default=100)
    parser.add_argument(
        "--max-vectors",
        type=int,
        default=None,
        help="Cap the number of vectors loaded (useful for huge datasets)",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--start-checkpoint",
        type=int,
        default=1,
        help="First checkpoint to compute (1-indexed). Earlier checkpoints "
        "are read from the existing ground truth file.",
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Force datasets streaming instead of direct shard download",
    )
    args = parser.parse_args()

    if args.dataset == "all":
        for ds_name in DATASETS:
            print(f"\n{'='*60}")
            print(f"  Dataset: {ds_name}")
            print(f"{'='*60}\n")
            run_dataset(ds_name, args)
        return

    run_dataset(args.dataset, args)


def run_dataset(dataset_name, args):
    config = DATASETS[dataset_name]
    cache_dir = Path.home() / ".cache" / config["cache_dir"]
    cache_dir.mkdir(parents=True, exist_ok=True)
    out_path = cache_dir / "ground_truth.parquet"

    start_cp = args.start_checkpoint  # 1-indexed

    if args.streaming:
        vectors = load_vectors_streaming(config, max_vectors=args.max_vectors)
    else:
        vectors = load_vectors_fast(config, max_vectors=args.max_vectors)
    n = vectors.shape[0]

    rng = np.random.RandomState(args.seed)
    num_checkpoints = (n + BATCH_SIZE - 1) // BATCH_SIZE

    all_rows = []

    # Load existing rows from previous ground truth for skipped checkpoints
    if start_cp > 1 and out_path.exists():
        existing = pq.read_table(out_path)
        max_ids = existing.column("max_vector_id").to_pylist()
        skip_boundary = (start_cp - 1) * BATCH_SIZE
        for i, mid in enumerate(max_ids):
            if mid <= skip_boundary:
                all_rows.append(
                    {
                        "query_vector": existing.column("query_vector")[i].as_py(),
                        "max_vector_id": np.uint64(mid),
                        "neighbors_l2": existing.column("neighbors_l2")[i].as_py(),
                        "neighbors_ip": existing.column("neighbors_ip")[i].as_py(),
                        "neighbors_cosine": existing.column("neighbors_cosine")[
                            i
                        ].as_py(),
                    }
                )
        print(
            f"Loaded {len(all_rows)} existing queries from checkpoints 1-{start_cp - 1}"
        )

    for cp_idx in range(num_checkpoints):
        cp_num = cp_idx + 1  # 1-indexed
        cp_end = min(cp_num * BATCH_SIZE, n)

        # Always advance the RNG to keep deterministic sequence
        query_indices = rng.choice(cp_end, size=args.num_queries, replace=False)

        if cp_num < start_cp:
            continue

        queries = vectors[query_indices].copy()
        data = vectors[:cp_end]

        print(
            f"Checkpoint {cp_num}/{num_checkpoints}: {cp_end:,} vectors, {args.num_queries} queries"
        )

        t0 = time.time()
        neighbors_l2 = brute_force_knn_l2(data, queries, args.k)
        print(f"  L2 neighbors in {time.time()-t0:.1f}s")

        t0 = time.time()
        neighbors_ip = brute_force_knn_ip(data, queries, args.k)
        print(f"  IP neighbors in {time.time()-t0:.1f}s")

        t0 = time.time()
        neighbors_cosine = brute_force_knn_cosine(data, queries, args.k)
        print(f"  Cosine neighbors in {time.time()-t0:.1f}s")

        for i in range(args.num_queries):
            all_rows.append(
                {
                    "query_vector": queries[i].tolist(),
                    "max_vector_id": np.uint64(cp_end),
                    "neighbors_l2": neighbors_l2[i].tolist(),
                    "neighbors_ip": neighbors_ip[i].tolist(),
                    "neighbors_cosine": neighbors_cosine[i].tolist(),
                }
            )

    query_vectors = pa.array(
        [r["query_vector"] for r in all_rows], type=pa.list_(pa.float32())
    )
    max_vector_ids = pa.array([r["max_vector_id"] for r in all_rows], type=pa.uint64())
    neighbors_l2_arr = pa.array(
        [r["neighbors_l2"] for r in all_rows], type=pa.list_(pa.uint32())
    )
    neighbors_ip_arr = pa.array(
        [r["neighbors_ip"] for r in all_rows], type=pa.list_(pa.uint32())
    )
    neighbors_cosine_arr = pa.array(
        [r["neighbors_cosine"] for r in all_rows], type=pa.list_(pa.uint32())
    )

    table = pa.table(
        {
            "query_vector": query_vectors,
            "max_vector_id": max_vector_ids,
            "neighbors_l2": neighbors_l2_arr,
            "neighbors_ip": neighbors_ip_arr,
            "neighbors_cosine": neighbors_cosine_arr,
        }
    )

    pq.write_table(table, out_path)
    print(f"\nWrote {len(all_rows)} queries to {out_path}")
    print(f"  File size: {os.path.getsize(out_path) / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
