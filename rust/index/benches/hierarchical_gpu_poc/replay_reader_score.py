#!/usr/bin/env python3
"""Replay one real reader query's leaf scores on a GPU."""

from __future__ import annotations

import argparse
import json
import statistics
import struct
import time
from pathlib import Path
from typing import Any

import numpy as np

from score_codes_gpu import KERNEL


def read_fixture(
    path: Path,
) -> tuple[list[tuple[np.ndarray, np.ndarray, tuple[Any, ...]]],
           np.ndarray, np.ndarray, int]:
    data = memoryview(path.read_bytes())
    if data[:8].tobytes() != b"HSPNSCR1":
        raise ValueError("invalid reader scoring fixture")
    leaf_count, code_size = struct.unpack_from("<II", data, 8)
    if code_size != 144:
        raise ValueError(f"GPU kernel requires 144-byte codes, got {code_size}")
    offset = 16
    leaves = []
    all_ids = []
    all_scores = []
    for _ in range(leaf_count):
        leaf_id, count, sum_q_u = struct.unpack_from("<III", data, offset)
        offset += 12
        values = struct.unpack_from("<5f", data, offset)
        offset += 20
        planes = np.frombuffer(data[offset:offset + 512], dtype=np.uint8).copy()
        offset += 512
        records = np.frombuffer(data[offset:offset + count * 152],
                                dtype=np.dtype([("id", "<u4"),
                                                ("code", "u1", 144),
                                                ("score", "<f4")])).copy()
        offset += count * 152
        leaves.append((records["code"].copy(), planes,
                       (np.uint32(sum_q_u), *(np.float32(v) for v in values))))
        all_ids.append(records["id"].copy())
        all_scores.append(records["score"].copy())
    if offset != len(data):
        raise ValueError(f"trailing or missing fixture bytes: {len(data) - offset}")
    return leaves, np.concatenate(all_ids), np.concatenate(all_scores), leaf_count


def top_ids(ids: np.ndarray, scores: np.ndarray, k: int) -> set[int]:
    best: dict[int, float] = {}
    for id_value, score in zip(ids, scores):
        key = int(id_value)
        best[key] = min(best.get(key, float("inf")), float(score))
    return {id_value for id_value, _ in sorted(best.items(),
            key=lambda item: (item[1], item[0]))[:k]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--repetitions", type=int, default=5)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("repetitions must be positive")
    import cupy as cp

    leaves, ids, cpu_scores, leaf_count = read_fixture(args.fixture)
    kernel = cp.RawKernel(KERNEL, "score_codes")
    warm_codes = cp.asarray(leaves[0][0][:1])
    warm_planes = cp.asarray(leaves[0][1]).view(cp.uint64)
    warm_output = cp.empty(1, dtype=cp.float32)
    kernel((1,), (128,),
           (warm_codes, warm_planes, warm_output, np.int32(1), *leaves[0][2]))
    cp.cuda.Stream.null.synchronize()

    timings = []
    gpu_scores = None
    for _ in range(args.repetitions):
        start = time.perf_counter()
        outputs = []
        for codes, planes, values in leaves:
            if not len(codes):
                continue
            gpu_codes = cp.asarray(codes)
            gpu_planes = cp.asarray(planes).view(cp.uint64)
            output = cp.empty(len(codes), dtype=cp.float32)
            kernel(((len(codes) + 255) // 256,), (256,),
                   (gpu_codes, gpu_planes, output, np.int32(len(codes)), *values))
            outputs.append(output)
        gpu_scores = cp.asnumpy(cp.concatenate(outputs))
        timings.append((time.perf_counter() - start) * 1000)

    assert gpu_scores is not None
    absolute_errors = np.abs(cpu_scores - gpu_scores)
    report = {
        "fixture": str(args.fixture),
        "leaves": leaf_count,
        "codes": len(ids),
        "repetitions": args.repetitions,
        "median_gpu_scoring_with_transfers_ms": statistics.median(timings),
        "max_absolute_score_error": float(np.max(absolute_errors)),
        "mean_absolute_score_error": float(np.mean(absolute_errors)),
        "top_100_overlap": len(top_ids(ids, cpu_scores, 100) &
                               top_ids(ids, gpu_scores, 100)),
        "top_800_overlap": len(top_ids(ids, cpu_scores, 800) &
                               top_ids(ids, gpu_scores, 800)),
        "note": "Real saved-index codes and query quantization; this excludes navigation, posting loads, deduplication, reranking, and the Rust/Python boundary.",
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
