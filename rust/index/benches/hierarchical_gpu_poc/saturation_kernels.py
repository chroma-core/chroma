#!/usr/bin/env python3
"""Measure sustained resident GPU component throughput and telemetry."""

import argparse
import json
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable

import cupy as cp
import numpy as np
from component_kernels import QUANTIZE, file_sha256
from replay_split import load_fixture


def measure(
    run: Callable[[], Any],
    seconds: float,
    byte_count: int,
    flop_count: int,
    samples: list[tuple[float, list[float]]],
) -> dict[str, Any]:
    for _ in range(3):
        run()
    cp.cuda.Stream.null.synchronize()
    start, end = cp.cuda.Event(), cp.cuda.Event()
    start.record()
    run()
    end.record()
    end.synchronize()
    estimate = cp.cuda.get_elapsed_time(start, end) / 1000
    iterations = max(1, min(10000, int(0.25 / max(estimate, 1e-6))))
    timings: list[float] = []
    wall_start = time.monotonic()
    while time.monotonic() - wall_start < seconds or len(timings) < 3:
        start.record()
        for _ in range(iterations):
            run()
        end.record()
        end.synchronize()
        timings.append(cp.cuda.get_elapsed_time(start, end) / 1000 / iterations)
    duration = time.monotonic() - wall_start
    selected = [s[1] for s in samples if wall_start <= s[0] <= wall_start + duration]
    median = float(np.median(timings))
    return {
        "seconds": median,
        "min_seconds": min(timings),
        "vectors_per_second": None,
        "effective_GB_per_second": byte_count / median / 1e9,
        "TFLOP_per_second": flop_count / median / 1e12,
        "sustained_wall_seconds": duration,
        "blocks": len(timings),
        "telemetry_samples": len(selected),
        "telemetry_mean": (
            dict(
                zip(
                    [
                        "gpu_util_percent",
                        "memory_util_percent",
                        "sm_clock_MHz",
                        "memory_clock_MHz",
                        "power_W",
                        "temperature_C",
                    ],
                    np.mean(selected, axis=0).tolist(),
                )
            )
            if selected
            else {}
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=float, default=2)
    parser.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        default=[65536, 262144, 1000000, 2000000, 4000000, 8000000, 16000000],
    )
    args = parser.parse_args()
    samples: list[tuple[float, list[float]]] = []
    process = subprocess.Popen(
        [
            "nvidia-smi",
            "--query-gpu=utilization.gpu,utilization.memory,clocks.sm,clocks.mem,power.draw,temperature.gpu",
            "--format=csv,noheader,nounits",
            "-lms",
            "100",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )

    def sample() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            try:
                samples.append(
                    (time.monotonic(), [float(v.strip()) for v in line.split(",")])
                )
            except ValueError:
                pass

    threading.Thread(target=sample, daemon=True).start()
    base, metadata = load_fixture(args.fixture)
    gb = cp.asarray(base)
    centers = cp.asarray(base[np.arange(4096) % len(base)])
    mean = gb.mean(axis=0)
    device = cp.cuda.runtime.getDeviceProperties(0)
    report = {
        "gpu": device["name"].decode(),
        "fixture_sha256": file_sha256(args.fixture),
        "dimension": metadata["dim"],
        "cupy": cp.__version__,
        "driver": cp.cuda.runtime.driverGetVersion(),
        "runtime": cp.cuda.runtime.runtimeGetVersion(),
        "device_total_bytes": cp.cuda.runtime.memGetInfo()[1],
        "timing": "median CUDA-event time across sustained blocks; no transfers",
        "bandwidth": "effective algorithm byte count, not a hardware counter",
        "results": [],
        "skipped": [],
    }
    try:
        for n in args.sizes:
            cp.get_default_memory_pool().free_all_blocks()
            free, _ = cp.cuda.runtime.memGetInfo()
            if n * metadata["dim"] * 4 > free * 0.60:
                report["skipped"].append(
                    {"vectors": n, "reason": "input exceeds 60% of free GPU memory"}
                )
                continue
            x = cp.tile(gb, (int(np.ceil(n / len(base))), 1))[:n].copy()
            dim = x.shape[1]

            def emit(
                name: str,
                run: Callable[[], Any],
                bytes_: int,
                flops_: int,
                **extra: Any
            ) -> None:
                result = measure(run, args.seconds, bytes_, flops_, samples)
                result.update(component=name, vectors=n, dimension=dim, **extra)
                result["vectors_per_second"] = n / result["seconds"]
                report["results"].append(result)
                print(json.dumps(result), flush=True)
                args.output.write_text(json.dumps(report, indent=2) + "\n")

            copy = cp.empty_like(x)

            def copy_run(copy: Any = copy, x: Any = x) -> None:
                cp.copyto(copy, x)

            emit(
                "device_copy",
                copy_run,
                x.nbytes * 2,
                0,
            )
            del copy_run, copy
            for count in [2, 128, 1024, 4096]:
                free, _ = cp.cuda.runtime.memGetInfo()
                if n * count * 4 > free * 0.75:
                    report["skipped"].append(
                        {
                            "vectors": n,
                            "centers": count,
                            "reason": "distance output exceeds free memory budget",
                        }
                    )
                    continue
                out = cp.empty((n, count), dtype=cp.float32)

                def distance_run(x: Any = x, out: Any = out) -> None:
                    cp.matmul(x, centers[:count].T, out=out)

                emit(
                    "distance_matrix",
                    distance_run,
                    x.nbytes + out.nbytes + count * dim * 4,
                    2 * n * dim * count,
                    centers=count,
                    precision="float32",
                )
                del distance_run, out
                cp.get_default_memory_pool().free_all_blocks()
            weights = cp.empty((2, n), dtype=cp.float32)
            weights[0] = cp.arange(n) % 2 == 0
            weights[1] = 1 - weights[0]
            out = cp.empty((2, dim), dtype=cp.float32)
            counts = weights.sum(axis=1)[:, None]

            def centroid(
                weights: Any = weights, x: Any = x, out: Any = out, counts: Any = counts
            ) -> None:
                cp.matmul(weights, x, out=out)
                cp.divide(out, counts, out=out)

            emit(
                "two_centroids",
                centroid,
                x.nbytes + weights.nbytes + out.nbytes,
                4 * n * dim,
            )
            del centroid, weights, out, counts
            two = centers[:2]
            norms = cp.sum(two * two, axis=1)

            def iteration(norms: Any = norms, x: Any = x, two: Any = two) -> Any:
                labels = cp.argmin(norms - 2 * (x @ two.T), axis=1)
                weights = cp.stack((labels == 0, labels == 1)).astype(cp.float32)
                return (weights @ x) / cp.maximum(weights.sum(axis=1)[:, None], 1)

            emit("two_means_iteration", iteration, 2 * x.nbytes + 32 * n, 8 * n * dim)
            codes = cp.empty((n, dim // 8), dtype=cp.uint8)
            stats = cp.empty((n, 4), dtype=cp.float32)

            def quantize_run(
                x: Any = x, codes: Any = codes, stats: Any = stats
            ) -> None:
                QUANTIZE((n,), (128,), (x, mean, codes, stats, n, dim))

            emit(
                "one_bit_quantization",
                quantize_run,
                x.nbytes + codes.nbytes + stats.nbytes,
                0,
            )
            del quantize_run, iteration, codes, stats, x, two, norms
            cp.get_default_memory_pool().free_all_blocks()
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print("REPORT_JSON=" + json.dumps(report), flush=True)
        print("BENCHMARK_COMPLETE", flush=True)
    finally:
        process.terminate()


if __name__ == "__main__":
    main()
