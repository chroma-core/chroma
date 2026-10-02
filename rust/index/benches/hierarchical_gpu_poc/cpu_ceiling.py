#!/usr/bin/env python3
"""Compare host CPU arithmetic with the completed GPU ceiling workloads."""

import argparse
import ctypes
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

import numpy as np
from threadpoolctl import threadpool_info, threadpool_limits
from component_kernels import cpu_quantize, file_sha256
from replay_split import load_fixture


def measure(run: Callable[[], Any], seconds: float) -> dict[str, Any]:
    run()
    timings: list[float] = []
    started = time.perf_counter()
    while time.perf_counter() - started < seconds or len(timings) < 3:
        begin = time.perf_counter()
        run()
        timings.append(time.perf_counter() - begin)
    return {
        "seconds": float(np.median(timings)),
        "min_seconds": min(timings),
        "repetitions": len(timings),
        "sustained_wall_seconds": time.perf_counter() - started,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", nargs="+", type=int, default=[14, 23])
    parser.add_argument("--seconds", type=float, default=2)
    args = parser.parse_args()
    lib = ctypes.CDLL(str(args.library))
    lib.omp_set_num_threads.argtypes = [ctypes.c_int]
    lib.centroid_cpu.argtypes = [ctypes.c_void_p] * 3 + [ctypes.c_size_t, ctypes.c_int]
    lib.quantize_cpu.argtypes = [ctypes.c_void_p] * 4 + [ctypes.c_size_t, ctypes.c_int]
    base, metadata = load_fixture(args.fixture)
    n, dim = 8000000, metadata["dim"]
    x = np.tile(base, (n // len(base), 1))
    centers = base[:4096].copy()
    center = base.mean(axis=0)
    labels = np.arange(n, dtype=np.int32) % 2
    norms = np.sum(centers[:2] ** 2, axis=1)
    centroid_out = np.empty((2, dim), dtype=np.float32)
    codes = np.empty((n, dim // 8), dtype=np.uint8)
    stats = np.empty((n, 4), dtype=np.float32)
    report: dict[str, Any] = {
        "cpu_lscpu": json.loads(
            subprocess.check_output(["lscpu", "--json"], text=True)
        ),
        "cpu_quota": Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
        "cpu_affinity": sorted(getattr(os, "sched_getaffinity")(0)),
        "fixture_sha256": file_sha256(args.fixture),
        "dimension": dim,
        "timing": "median warmed wall time, resident host input and preallocated outputs",
        "centroid_accumulation": "OpenMP with double-precision worker sums and float32 output",
        "quantization": "fused OpenMP C++ code and four float32 headers",
        "results": [],
    }

    def centroid(chosen: Any = labels) -> None:
        lib.centroid_cpu(
            x.ctypes.data, chosen.ctypes.data, centroid_out.ctypes.data, n, dim
        )

    def quantize() -> None:
        lib.quantize_cpu(
            x.ctypes.data,
            center.ctypes.data,
            codes.ctypes.data,
            stats.ctypes.data,
            n,
            dim,
        )

    try:
        for threads in args.threads:
            lib.omp_set_num_threads(threads)
            with threadpool_limits(limits=threads, user_api="blas"):
                report["threadpools"] = threadpool_info()

                def emit(
                    name: str, run: Callable[[], Any], vectors: int = n, **extra: Any
                ) -> None:
                    row = measure(run, args.seconds)
                    row.update(
                        component=name,
                        vectors=vectors,
                        threads=threads,
                        vectors_per_second=vectors / row["seconds"],
                        **extra
                    )
                    report["results"].append(row)
                    args.output.write_text(json.dumps(report, indent=2) + "\n")
                    print(json.dumps(row), flush=True)

                copied = np.empty_like(x)

                def copy_run(copied: Any = copied) -> None:
                    np.copyto(copied, x)

                emit("host_copy", copy_run)
                del copy_run, copied
                for count, rows in [(2, n), (128, n), (1024, n), (4096, 2000000)]:
                    out = np.empty((rows, count), dtype=np.float32)

                    def distance(out: Any = out) -> None:
                        np.matmul(x[:rows], centers[:count].T, out=out)

                    emit("distance_matrix", distance, vectors=rows, centers=count)
                    expected = base[:256] @ centers[:count].T
                    error = float(np.max(np.abs(out[:256] - expected)))
                    assert error < 1e-5, error
                    report["results"][-1]["max_sampled_abs_error"] = error
                    del distance, out
                emit("two_centroids_stream", centroid)
                expected_centers = np.stack(
                    [base[::2].mean(axis=0), base[1::2].mean(axis=0)]
                )
                error = float(np.max(np.abs(centroid_out - expected_centers)))
                assert error < 1e-5, error
                report["results"][-1]["max_center_abs_error"] = error
                scores = np.empty((n, 2), dtype=np.float32)
                chosen = np.empty(n, dtype=np.int32)

                def iteration(scores: Any = scores, chosen: Any = chosen) -> None:
                    np.matmul(x, centers[:2].T, out=scores)
                    np.multiply(scores, -2, out=scores)
                    np.add(scores, norms, out=scores)
                    chosen[:] = np.argmin(scores, axis=1)
                    centroid(chosen)

                emit("two_means_stream", iteration)
                base_chosen = np.argmin(norms - 2 * (base @ centers[:2].T), axis=1)
                checked = np.concatenate((np.arange(256), np.arange(n - 256, n)))
                assert np.array_equal(chosen[checked], base_chosen[checked % len(base)])
                expected_centers = np.stack(
                    [base[base_chosen == k].mean(axis=0) for k in range(2)]
                )
                error = float(np.max(np.abs(centroid_out - expected_centers)))
                assert error < 1e-5, error
                report["results"][-1]["max_center_abs_error"] = error
                report["results"][-1]["sampled_label_agreement"] = True
                del iteration, scores, chosen
                emit("quantization_stream", quantize)
                expected_codes, expected_stats = cpu_quantize(x[checked], center)
                assert np.array_equal(codes[checked], expected_codes)
                error = float(np.max(np.abs(stats[checked] - expected_stats)))
                assert error < 1e-4, error
                report["results"][-1].update(
                    sampled_code_agreement=True, max_sampled_header_abs_error=error
                )
        print("BENCHMARK_COMPLETE", flush=True)
    finally:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print("REPORT_JSON=" + json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
