#!/usr/bin/env python3
"""Compare CPU and GPU two-means on one captured writer split."""

from __future__ import annotations

import argparse
import json
import struct
import time
from pathlib import Path
from typing import Any

import numpy as np


def load_fixture(path: Path) -> tuple[np.ndarray, dict[str, int]]:
    with path.open("rb") as source:
        header = source.read(32)
    if len(header) != 32 or header[:8] != b"HSPNSPL1":
        raise ValueError("invalid split fixture")
    count, dim, seed, leaf_id, depth = struct.unpack("<IIQII", header[8:])
    record = np.dtype([("id", "<u4"), ("version", "<u4"),
                       ("vector", "<f4", (dim,))])
    expected = 32 + count * record.itemsize
    if path.stat().st_size != expected:
        raise ValueError(f"fixture is {path.stat().st_size} bytes; expected {expected}")
    rows = np.memmap(path, dtype=record, mode="r", offset=32, shape=(count,))
    points = np.ascontiguousarray(rows["vector"])
    return points, {"count": count, "dim": dim, "seed": seed,
                    "leaf_id": leaf_id, "depth": depth}


def scalar(value: Any) -> float:
    return float(value.item())


def two_means(x: Any, pairs: np.ndarray, xp: Any, max_iterations: int) -> tuple[Any, Any, Any, int]:
    count, dim = x.shape
    norm = xp.sum(x * x, axis=1)
    total_x = xp.sum(x, axis=0)
    best_score = float("inf")
    first = None
    second = None
    for i, j in pairs:
        c0 = x[int(i)].copy()
        c1 = x[int(j)].copy()
        d0 = norm - 2 * (x @ c0) + xp.sum(c0 * c0)
        d1 = norm - 2 * (x @ c1) + xp.sum(c1 * c1)
        score = scalar(xp.sum(xp.minimum(d0, d1)))
        if score < best_score:
            best_score = score
            first, second = c0, c1
    assert first is not None and second is not None
    c0, c1 = first, second
    previous_score = float("inf")
    no_improvement = 0
    iterations = 0
    labels = None
    d0 = d1 = None
    for _ in range(max_iterations):
        d0 = norm - 2 * (x @ c0) + xp.sum(c0 * c0)
        d1 = norm - 2 * (x @ c1) + xp.sum(c1 * c1)
        labels = d1 < d0
        score = scalar(xp.sum(xp.minimum(d0, d1)))
        n1 = int(scalar(xp.sum(labels)))
        n0 = count - n1
        sum1 = labels.astype(xp.float32) @ x
        next_c1 = sum1 / n1 if n1 else xp.zeros(dim, dtype=xp.float32)
        next_c0 = (total_x - sum1) / n0 if n0 else xp.zeros(dim, dtype=xp.float32)
        separation = scalar(xp.sum((c0 - c1) ** 2))
        movement = scalar(xp.sum((c0 - next_c0) ** 2)
                          + xp.sum((c1 - next_c1) ** 2))
        c0, c1 = next_c0, next_c1
        iterations += 1
        if separation > np.finfo(np.float32).eps and movement / separation < np.finfo(np.float32).eps:
            break
        if score >= previous_score:
            no_improvement += 1
            if no_improvement >= 4:
                break
        else:
            no_improvement = 0
        previous_score = score
    assert labels is not None and d0 is not None and d1 is not None

    n1 = int(scalar(xp.sum(labels)))
    n0 = count - n1
    minimum = count // 4
    if min(n0, n1) < minimum:
        larger_is_zero = n0 > n1
        deficit = minimum - min(n0, n1)
        candidates = xp.where(~labels if larger_is_zero else labels)[0]
        own_distance = d0[candidates] if larger_is_zero else d1[candidates]
        chosen = candidates[xp.argsort(-own_distance)[:deficit]]
        labels[chosen] = ~labels[chosen]
        n1 = int(scalar(xp.sum(labels)))
        n0 = count - n1
        sum1 = labels.astype(xp.float32) @ x
        c1 = sum1 / n1
        c0 = (total_x - sum1) / n0
    return labels, c0, c1, iterations


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--max-points", type=int)
    parser.add_argument("--max-iterations", type=int, default=128)
    args = parser.parse_args()
    import cupy as cp

    points, metadata = load_fixture(args.fixture)
    if args.max_points:
        points = points[:args.max_points].copy()
    if len(points) < 2:
        parser.error("fixture needs at least two points")
    rng = np.random.default_rng(metadata["seed"])
    pairs = np.stack([rng.choice(len(points), size=2, replace=False)
                      for _ in range(4)])

    start = time.perf_counter()
    cpu_labels, cpu_c0, cpu_c1, cpu_iterations = two_means(
        points, pairs, np, args.max_iterations)
    cpu_seconds = time.perf_counter() - start

    start = time.perf_counter()
    gpu_points = cp.asarray(points)
    gpu_labels, gpu_c0, gpu_c1, gpu_iterations = two_means(
        gpu_points, pairs, cp, args.max_iterations)
    gpu_labels = cp.asnumpy(gpu_labels)
    gpu_c0 = cp.asnumpy(gpu_c0)
    gpu_c1 = cp.asnumpy(gpu_c1)
    cp.cuda.Stream.null.synchronize()
    gpu_seconds = time.perf_counter() - start

    same = np.mean(cpu_labels == gpu_labels)
    swapped = np.mean(cpu_labels != gpu_labels)
    if swapped > same:
        gpu_labels = ~gpu_labels
        gpu_c0, gpu_c1 = gpu_c1, gpu_c0
    report = {
        **metadata,
        "points_used": len(points),
        "cpu_seconds": cpu_seconds,
        "gpu_seconds_including_transfers": gpu_seconds,
        "speedup": cpu_seconds / gpu_seconds,
        "cpu_iterations": cpu_iterations,
        "gpu_iterations": gpu_iterations,
        "label_agreement": float(np.mean(cpu_labels == gpu_labels)),
        "cpu_group_sizes": [int(np.sum(~cpu_labels)), int(np.sum(cpu_labels))],
        "gpu_group_sizes": [int(np.sum(~gpu_labels)), int(np.sum(gpu_labels))],
        "center_max_abs_error": max(float(np.max(np.abs(cpu_c0 - gpu_c0))),
                                    float(np.max(np.abs(cpu_c1 - gpu_c1)))),
        "note": "Prototype two-means with shared seed pairs; this does not replay Rust's StdRng or measure the full writer.",
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
