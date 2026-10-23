#!/usr/bin/env python3
"""Compute exhaustive top-100 neighbors for the 1M and 10M corpus prefixes.

The same 100 real corpus vectors are used as queries at both boundaries. Their
global IDs are selected by NumPy RandomState(42) from the first million rows,
matching the benchmark's existing Wikipedia 1M query selection. Every corpus
vector is scored using float64 accumulation for L2, inner product, and cosine.
"""

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DIM = 1024
K = 100
QUERIES = 100
BOUNDARIES = tuple(range(1_000_000, 10_000_001, 1_000_000))
BATCH_ROWS = 10_000


def batches(shards, limit):
    count = 0
    for shard in shards:
        path = Path(shard["path"])
        if not path.exists():
            raise FileNotFoundError(path)
        reader = pq.ParquetFile(path)
        if reader.metadata.num_rows != shard["rows"]:
            raise ValueError(f"row count changed: {path}")
        for batch in reader.iter_batches(batch_size=BATCH_ROWS, columns=["emb"]):
            arr = batch.column(0)
            if arr.null_count:
                raise ValueError(f"null embedding in {path}")
            sizes = np.diff(np.asarray(arr.offsets))
            if not np.all(sizes == DIM):
                raise ValueError(f"embedding dimension changed in {path}")
            flat = arr.values.to_numpy(zero_copy_only=False)
            vectors = np.asarray(flat, dtype=np.float32).reshape(-1, DIM)
            take = min(len(vectors), limit - count)
            if take:
                yield count, vectors[:take]
                count += take
            if count == limit:
                return
    raise RuntimeError(f"only {count:,} rows available; expected {limit:,}")


def select_queries(shards):
    ids = np.random.RandomState(42).choice(1_000_000, size=QUERIES, replace=False)
    queries = np.empty((QUERIES, DIM), dtype=np.float32)
    found = np.zeros(QUERIES, dtype=bool)
    for start, block in batches(shards, BOUNDARIES[0]):
        mask = (ids >= start) & (ids < start + len(block))
        positions = np.flatnonzero(mask)
        if len(positions):
            queries[positions] = block[ids[positions] - start]
            found[positions] = True
    if not found.all():
        raise RuntimeError("some selected query vectors were not found")
    return ids, queries


def update_best(best_dist, best_ids, distances, start):
    local_k = min(K, distances.shape[1])
    indices = np.argpartition(distances, local_k - 1, axis=1)[:, :local_k]
    d = np.take_along_axis(distances, indices, axis=1)
    ids = indices.astype(np.uint32) + start
    merged_d = np.concatenate((best_dist, d), axis=1)
    merged_i = np.concatenate((best_ids, ids), axis=1)
    keep = np.argpartition(merged_d, K - 1, axis=1)[:, :K]
    best_dist[:] = np.take_along_axis(merged_d, keep, axis=1)
    best_ids[:] = np.take_along_axis(merged_i, keep, axis=1)


def sorted_ids(best_dist, best_ids):
    # Sort retained neighbors by distance and ID. If a tie crosses rank K,
    # any of the equally distant vectors is a valid exact neighbor.
    order = np.lexsort((best_ids, best_dist), axis=1)
    return np.take_along_axis(best_ids, order, axis=1).copy()


def run(shards, queries):
    q = queries.astype(np.float64)
    qnorm2 = np.einsum("ij,ij->i", q, q)
    qnorm = np.sqrt(qnorm2)
    if np.any(qnorm == 0):
        raise ValueError("zero-norm query")
    best = {
        name: (np.full((QUERIES, K), np.inf), np.full((QUERIES, K), np.iinfo(np.uint32).max, dtype=np.uint32))
        for name in ("l2", "ip", "cosine")
    }
    snapshots = {}
    started = time.time()
    for start, block in batches(shards, BOUNDARIES[-1]):
        x = block.astype(np.float64)
        xnorm2 = np.einsum("ij,ij->i", x, x)
        dots = q @ x.T
        distances = {
            "l2": qnorm2[:, None] + xnorm2[None, :] - 2 * dots,
            "ip": -dots,
            "cosine": -dots / np.maximum(qnorm[:, None] * np.sqrt(xnorm2)[None, :], 1e-300),
        }
        for name, d in distances.items():
            if not np.all(np.isfinite(d)):
                raise ValueError(f"nonfinite {name} distance at row {start}")
            update_best(*best[name], d, start)
        end = start + len(block)
        if end in BOUNDARIES:
            snapshots[end] = {name: sorted_ids(*best[name]) for name in best}
            print(f"Exact top-{K} complete at {end:,} vectors, elapsed {time.time() - started:.1f}s", flush=True)
        elif end % 1_000_000 == 0:
            print(f"Scanned {end:,} vectors, elapsed {time.time() - started:.1f}s", flush=True)
    return snapshots, time.time() - started


def write_results(dataset, shards, query_ids, queries, snapshots, elapsed, outdir):
    outdir.mkdir(parents=True, exist_ok=True)
    rows = []
    for boundary in BOUNDARIES:
        for qi in range(QUERIES):
            rows.append({
                "query_vector": queries[qi].tolist(),
                "max_vector_id": boundary,
                "neighbors_l2": snapshots[boundary]["l2"][qi].tolist(),
                "neighbors_ip": snapshots[boundary]["ip"][qi].tolist(),
                "neighbors_cosine": snapshots[boundary]["cosine"][qi].tolist(),
            })
    table = pa.table({
        "query_vector": pa.array([r["query_vector"] for r in rows], type=pa.list_(pa.float32())),
        "max_vector_id": pa.array([r["max_vector_id"] for r in rows], type=pa.uint64()),
        "neighbors_l2": pa.array([r["neighbors_l2"] for r in rows], type=pa.list_(pa.uint32())),
        "neighbors_ip": pa.array([r["neighbors_ip"] for r in rows], type=pa.list_(pa.uint32())),
        "neighbors_cosine": pa.array([r["neighbors_cosine"] for r in rows], type=pa.list_(pa.uint32())),
    })
    path = outdir / f"{dataset}_1m_10m_ground_truth.parquet"
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {
        "dataset": dataset,
        "source_shards": len(shards),
        "source_rows": sum(s["rows"] for s in shards),
        "source_paths": [s["path"] for s in shards],
        "dimension": DIM,
        "boundaries": list(BOUNDARIES),
        "query_selection": "100 real corpus vectors sampled without replacement from IDs [0, 1000000), NumPy RandomState(42).choice",
        "query_ids": query_ids.tolist(),
        "query_count": QUERIES,
        "neighbors_per_metric": K,
        "metrics": ["squared L2", "negative inner product", "negative cosine similarity"],
        "method": "exhaustive scan, float64 dot products and norms, top-k merge per 10000-row batch",
        "elapsed_seconds": elapsed,
        "parquet_path": str(path),
        "parquet_sha256": digest,
    }
    mpath = outdir / f"{dataset}_1m_10m_ground_truth.json"
    mpath.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Wrote {path}; sha256={digest}; {elapsed:.1f}s", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", choices=["wikipedia", "msmarco"])
    parser.add_argument("--source-manifest", type=Path)
    parser.add_argument("--outdir", type=Path, default=Path("/home/ubuntu/benchmark-data"))
    args = parser.parse_args()
    source = args.source_manifest or args.outdir / f"{args.dataset}_10m_sources.json"
    shards = json.loads(source.read_text())["shards"]
    if sum(s["rows"] for s in shards) < BOUNDARIES[-1]:
        raise ValueError("source prefix contains fewer than 10M vectors")
    ids, queries = select_queries(shards)
    print(f"Selected {len(ids)} real 1024-dimensional corpus queries; first IDs: {ids[:10].tolist()}", flush=True)
    snapshots, elapsed = run(shards, queries)
    write_results(args.dataset, shards, ids, queries, snapshots, elapsed, args.outdir)


if __name__ == "__main__":
    main()
