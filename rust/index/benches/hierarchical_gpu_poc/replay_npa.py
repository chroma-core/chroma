#!/usr/bin/env python3
"""Replay the distance decision in a captured writer split on CPU and GPU."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, TypeVar, cast

os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import cupy as cp  # noqa: E402
import numpy as np  # noqa: E402

from replay_split import load_fixture, two_means  # noqa: E402


T = TypeVar("T")


def decide(x: Any, labels: Any, old: Any, left: Any, right: Any, xp: Any) -> Any:
    # The vector norm cancels when comparing squared Euclidean distances.
    delta = xp.stack((left - old, right - old), axis=1)
    scores = -2.0 * (x @ delta)
    norms = xp.stack((xp.dot(left, left), xp.dot(right, right))) - xp.dot(old, old)
    scores += norms
    return xp.where(labels, scores[:, 1], scores[:, 0]) > 0.0


def median_seconds(run: Callable[[], T], repetitions: int) -> tuple[float, T]:
    samples = []
    for _ in range(repetitions):
        start = time.perf_counter()
        result = run()
        samples.append(time.perf_counter() - start)
    return float(np.median(samples)), result


def direct_decide(
    x: np.ndarray, labels: np.ndarray, old: np.ndarray,
    left: np.ndarray, right: np.ndarray,
) -> np.ndarray:
    decisions = np.empty(len(x), dtype=np.bool_)
    for start in range(0, len(x), 4096):
        end = min(start + 4096, len(x))
        vectors = x[start:end]
        chosen = np.where(labels[start:end, None], right, left)
        old_distance = np.sum((vectors - old) ** 2, axis=1)
        new_distance = np.sum((vectors - chosen) ** 2, axis=1)
        decisions[start:end] = new_distance > old_distance
    return decisions


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--sizes", type=int, nargs="+", default=[2048, 4096, 100000])
    parser.add_argument("--repetitions", type=int, default=9)
    args = parser.parse_args()

    points, metadata = load_fixture(args.fixture)
    rng = np.random.default_rng(metadata["seed"])
    pairs = np.stack([rng.choice(len(points), size=2, replace=False) for _ in range(4)])
    labels, left, right, iterations = two_means(points, pairs, np, 128)
    old = points.mean(axis=0)
    reports = []

    for count in args.sizes:
        if count > len(points):
            continue
        x = np.ascontiguousarray(points[:count])
        group = np.ascontiguousarray(labels[:count])
        cpu_time, cpu_decision = median_seconds(
            lambda: decide(x, group, old, left, right, np), args.repetitions
        )
        direct_decision = direct_decide(x, group, old, left, right)

        gx = cp.asarray(x)
        gg = cp.asarray(group)
        go = cp.asarray(old)
        gl = cp.asarray(left)
        gr = cp.asarray(right)
        decide(gx, gg, go, gl, gr, cp)
        cp.cuda.Stream.null.synchronize()

        def gpu_resident() -> Any:
            result = decide(gx, gg, go, gl, gr, cp)
            cp.cuda.Stream.null.synchronize()
            return result

        resident_time, _ = median_seconds(gpu_resident, args.repetitions)

        def gpu_transfer() -> np.ndarray:
            dx = cp.asarray(x)
            dg = cp.asarray(group)
            result = decide(dx, dg, go, gl, gr, cp)
            host = cp.asnumpy(result)
            cp.cuda.Stream.null.synchronize()
            return cast(np.ndarray, host)

        transfer_time, gpu_decision = median_seconds(gpu_transfer, args.repetitions)
        reports.append({
            "vectors": count,
            "cpu_batched_seconds": cpu_time,
            "gpu_resident_seconds": resident_time,
            "gpu_with_transfer_seconds": transfer_time,
            "cpu_over_gpu_with_transfer": cpu_time / transfer_time,
            "decision_agreement": float(np.mean(cpu_decision == gpu_decision)),
            "direct_distance_agreement": float(np.mean(cpu_decision == direct_decision)),
            "cpu_reassign_candidates": int(cpu_decision.sum()),
        })

    print(json.dumps({
        **metadata,
        "two_means_iterations": iterations,
        "repetitions": args.repetitions,
        "results": reports,
        "scope": "Real captured split vectors; centers and labels reconstructed by the shared-seed two-means replay. This measures only the NPA self distance decision, with batched CPU and GPU implementations; it excludes version checks, tree navigation, posting updates, and recursive balancing.",
    }, indent=2))


if __name__ == "__main__":
    main()
