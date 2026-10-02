#!/usr/bin/env python3
"""Measure isolated SPANN arithmetic on CPU and one CUDA GPU."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, cast

os.environ.setdefault("OPENBLAS_NUM_THREADS", "14")
os.environ.setdefault("OMP_NUM_THREADS", "14")

import cupy as cp  # noqa: E402
import numpy as np  # noqa: E402

from replay_split import load_fixture  # noqa: E402


QUANTIZE = cp.RawKernel(r"""
extern "C" __global__ void quantize(const float* x, const float* center,
                                     unsigned char* codes, float* stats,
                                     int rows, int dim) {
    unsigned long long row = blockIdx.x;
    int lane = threadIdx.x;
    if (row >= rows) return;
    float sum_abs = 0.0f, sum_sq = 0.0f, dot = 0.0f;
    int ones = 0;
    for (int byte = lane; byte < dim / 8; byte += blockDim.x) {
        unsigned int packed = 0;
        #pragma unroll
        for (int bit = 0; bit < 8; ++bit) {
            int col = byte * 8 + bit;
            float residual = x[row * dim + col] - center[col];
            packed |= (residual >= 0.0f) << bit;
            sum_abs += fabsf(residual);
            sum_sq += residual * residual;
            dot += residual * center[col];
        }
        codes[row * (dim / 8) + byte] = (unsigned char)packed;
        ones += __popc(packed);
    }
    __shared__ float abs_shared[128], sq_shared[128], dot_shared[128];
    __shared__ int ones_shared[128];
    abs_shared[lane] = sum_abs;
    sq_shared[lane] = sum_sq;
    dot_shared[lane] = dot;
    ones_shared[lane] = ones;
    __syncthreads();
    for (int stride = 64; stride > 0; stride >>= 1) {
        if (lane < stride) {
            abs_shared[lane] += abs_shared[lane + stride];
            sq_shared[lane] += sq_shared[lane + stride];
            dot_shared[lane] += dot_shared[lane + stride];
            ones_shared[lane] += ones_shared[lane + stride];
        }
        __syncthreads();
    }
    if (lane == 0) {
        float norm = sqrtf(sq_shared[0]);
        stats[row * 4] = norm < 1.1920929e-7f ? 1.0f : 0.5f * abs_shared[0] / norm;
        stats[row * 4 + 1] = norm;
        stats[row * 4 + 2] = dot_shared[0];
        stats[row * 4 + 3] = (float)(2 * ones_shared[0] - dim);
    }
}
""", "quantize")


def median_wall(run: Callable[[], Any], repetitions: int) -> tuple[float, Any]:
    times = []
    result = None
    for _ in range(repetitions):
        start = time.perf_counter()
        result = run()
        cp.cuda.Stream.null.synchronize()
        times.append(time.perf_counter() - start)
    return float(np.median(times)), result


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def median_gpu(run: Callable[[], Any], repetitions: int) -> float:
    times = []
    for _ in range(repetitions):
        start = cp.cuda.Event()
        end = cp.cuda.Event()
        start.record()
        run()
        end.record()
        end.synchronize()
        times.append(cp.cuda.get_elapsed_time(start, end) / 1000.0)
    return float(np.median(times))


def cpu_quantize(x: np.ndarray, center: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    codes = np.empty((len(x), x.shape[1] // 8), dtype=np.uint8)
    stats = np.empty((len(x), 4), dtype=np.float32)
    for start in range(0, len(x), 8192):
        stop = min(start + 8192, len(x))
        residual = x[start:stop] - center
        codes[start:stop] = np.packbits(residual >= 0, axis=1, bitorder="little")
        sum_abs = np.sum(np.abs(residual), axis=1)
        norm = np.sqrt(np.sum(residual * residual, axis=1))
        stats[start:stop, 0] = np.where(norm < np.finfo(np.float32).eps,
                                        1.0, 0.5 * sum_abs / norm)
        stats[start:stop, 1] = norm
        stats[start:stop, 2] = np.sum(residual * center, axis=1)
        stats[start:stop, 3] = 2 * np.count_nonzero(residual >= 0, axis=1) - x.shape[1]
    return codes, stats


def load_cpu_quantizer(path: Path) -> Callable[[np.ndarray, np.ndarray],
                                               tuple[np.ndarray, np.ndarray]]:
    function = ctypes.CDLL(str(path)).quantize_cpu
    function.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
                         ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
    function.restype = None

    def run(x: np.ndarray, center: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        codes = np.empty((len(x), x.shape[1] // 8), dtype=np.uint8)
        stats = np.empty((len(x), 4), dtype=np.float32)
        function(x.ctypes.data, center.ctypes.data, codes.ctypes.data,
                 stats.ctypes.data, len(x), x.shape[1])
        return codes, stats

    return run


def bench_distance(x: np.ndarray, centers: np.ndarray, repeats: int) -> dict[str, Any]:
    cx = cp.asarray(x)
    cc = cp.asarray(centers)

    def cpu_run() -> np.ndarray:
        return cast(np.ndarray, x @ centers.T)

    def gpu_run() -> Any:
        return cx @ cc.T

    cpu_run()
    gpu_run()
    cp.cuda.Stream.null.synchronize()
    cpu_s, cpu_result = median_wall(cpu_run, repeats)
    resident_s = median_gpu(gpu_run, repeats)

    def transfer() -> np.ndarray:
        result = cp.asarray(x) @ cc.T
        return cast(np.ndarray, cp.asnumpy(result))

    transfer_s, gpu_result = median_wall(transfer, repeats)
    err = float(np.max(np.abs(cpu_result[:256] - gpu_result[:256])))
    return {"component": "distance_matrix", "vectors": len(x),
            "centers": len(centers), "cpu_seconds": cpu_s,
            "gpu_resident_seconds": resident_s,
            "gpu_with_transfer_seconds": transfer_s,
            "gpu_resident_speedup": cpu_s / resident_s,
            "gpu_with_transfer_speedup": cpu_s / transfer_s,
            "max_abs_error_first_256": err}


def bench_centroid(x: np.ndarray, repeats: int) -> dict[str, Any]:
    labels = np.arange(len(x), dtype=np.int32) % 2
    weights = np.empty((2, len(x)), dtype=np.float32)
    weights[0] = (labels == 0)
    weights[1] = (labels == 1)
    counts = weights.sum(axis=1)[:, None]
    gx = cp.asarray(x)
    gw = cp.asarray(weights)
    gc = cp.asarray(counts)

    def cpu_run() -> np.ndarray:
        return cast(np.ndarray, (weights @ x) / counts)

    def gpu_run() -> Any:
        return (gw @ gx) / gc

    cpu_run()
    gpu_run()
    cp.cuda.Stream.null.synchronize()
    cpu_s, cpu_result = median_wall(cpu_run, repeats)
    resident_s = median_gpu(gpu_run, repeats)

    def transfer() -> np.ndarray:
        result = (gw @ cp.asarray(x)) / gc
        return cast(np.ndarray, cp.asnumpy(result))

    transfer_s, gpu_result = median_wall(transfer, repeats)
    return {"component": "two_centroids", "vectors": len(x),
            "cpu_seconds": cpu_s, "gpu_resident_seconds": resident_s,
            "gpu_with_transfer_seconds": transfer_s,
            "gpu_resident_speedup": cpu_s / resident_s,
            "gpu_with_transfer_speedup": cpu_s / transfer_s,
            "max_abs_error": float(np.max(np.abs(cpu_result - gpu_result)))}


def bench_two_means_iteration(x: np.ndarray, centers: np.ndarray,
                              repeats: int) -> dict[str, Any]:
    two = np.ascontiguousarray(centers[:2])
    norms = np.sum(two * two, axis=1)
    gx = cp.asarray(x)
    gc = cp.asarray(two)
    gn = cp.asarray(norms)

    def cpu_run() -> tuple[np.ndarray, np.ndarray]:
        labels = np.argmin(norms - 2.0 * (x @ two.T), axis=1)
        weights = np.stack((labels == 0, labels == 1)).astype(np.float32)
        counts = weights.sum(axis=1)[:, None]
        return labels, (weights @ x) / np.maximum(counts, 1)

    def gpu_run(vectors: Any) -> tuple[Any, Any]:
        labels = cp.argmin(gn - 2.0 * (vectors @ gc.T), axis=1)
        weights = cp.stack((labels == 0, labels == 1)).astype(cp.float32)
        counts = weights.sum(axis=1)[:, None]
        return labels, (weights @ vectors) / cp.maximum(counts, 1)

    cpu_run()
    gpu_run(gx)
    cp.cuda.Stream.null.synchronize()
    cpu_s, (cpu_labels, cpu_centers) = median_wall(cpu_run, repeats)
    resident_s = median_gpu(lambda: gpu_run(gx), repeats)

    def transfer() -> tuple[np.ndarray, np.ndarray]:
        labels, updated = gpu_run(cp.asarray(x))
        return cp.asnumpy(labels), cp.asnumpy(updated)

    transfer_s, (gpu_labels, gpu_centers) = median_wall(transfer, repeats)
    return {"component": "two_means_iteration", "vectors": len(x),
            "cpu_seconds": cpu_s, "gpu_resident_seconds": resident_s,
            "gpu_with_transfer_seconds": transfer_s,
            "gpu_resident_speedup": cpu_s / resident_s,
            "gpu_with_transfer_speedup": cpu_s / transfer_s,
            "label_agreement": float(np.mean(cpu_labels == gpu_labels)),
            "max_center_abs_error": float(np.max(np.abs(cpu_centers - gpu_centers)))}


def bench_quantize(x: np.ndarray, center: np.ndarray, repeats: int,
                   cpu_run: Callable[[np.ndarray, np.ndarray],
                                     tuple[np.ndarray, np.ndarray]]) -> dict[str, Any]:
    gx = cp.asarray(x)
    gc = cp.asarray(center)
    gpu_codes = cp.empty((len(x), x.shape[1] // 8), dtype=cp.uint8)
    gpu_stats = cp.empty((len(x), 4), dtype=cp.float32)

    def launch(vectors: Any, codes: Any, stats: Any) -> None:
        QUANTIZE((len(x),), (128,),
                 (vectors, gc, codes, stats, len(x), x.shape[1]))

    launch(gx, gpu_codes, gpu_stats)
    cp.cuda.Stream.null.synchronize()
    cpu_s, (cpu_codes, cpu_stats) = median_wall(
        lambda: cpu_run(x, center), repeats)
    resident_s = median_gpu(lambda: launch(gx, gpu_codes, gpu_stats), repeats)

    def transfer() -> tuple[np.ndarray, np.ndarray]:
        dx = cp.asarray(x)
        dc = cp.empty_like(gpu_codes)
        ds = cp.empty_like(gpu_stats)
        launch(dx, dc, ds)
        return cp.asnumpy(dc), cp.asnumpy(ds)

    transfer_s, (codes, stats) = median_wall(transfer, repeats)
    return {"component": "one_bit_quantization", "vectors": len(x),
            "cpu_seconds": cpu_s, "gpu_resident_seconds": resident_s,
            "gpu_with_transfer_seconds": transfer_s,
            "gpu_resident_speedup": cpu_s / resident_s,
            "gpu_with_transfer_speedup": cpu_s / transfer_s,
            "code_agreement": bool(np.array_equal(cpu_codes, codes)),
            "max_header_abs_error": float(np.max(np.abs(cpu_stats - stats)))}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--sizes", nargs="+", type=int,
                        default=[65536, 262144, 1000000])
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cpu-quant-library", type=Path)
    args = parser.parse_args()
    base, metadata = load_fixture(args.fixture)
    if metadata["dim"] % 8:
        parser.error("quantization requires dimension divisible by 8")
    centers = np.ascontiguousarray(base[np.linspace(
        0, len(base) - 1, 128, dtype=np.int64)])
    center = np.ascontiguousarray(base.mean(axis=0))
    cpu_quantizer = (load_cpu_quantizer(args.cpu_quant_library)
                     if args.cpu_quant_library else cpu_quantize)
    report: dict[str, Any] = {"gpu": cp.cuda.runtime.getDeviceProperties(0)["name"].decode(),
                              "fixture": str(args.fixture), "dimension": metadata["dim"],
                              "fixture_sha256": file_sha256(args.fixture),
                              "fixture_vectors": len(base), "repetitions": args.repetitions,
                              "cpu_threads_requested": int(os.environ["OPENBLAS_NUM_THREADS"]),
                              "cpu_quantization": "OpenMP fused C++" if args.cpu_quant_library else "NumPy",
                              "results": []}
    for count in args.sizes:
        x = np.ascontiguousarray(base[np.arange(count) % len(base)])
        for chosen_centers in (2, 128):
            result = bench_distance(x, centers[:chosen_centers], args.repetitions)
            report["results"].append(result)
            print(json.dumps(result), flush=True)
        for result in (bench_centroid(x, args.repetitions),
                       bench_two_means_iteration(x, centers, args.repetitions),
                       bench_quantize(x, center, args.repetitions, cpu_quantizer)):
            report["results"].append(result)
            print(json.dumps(result), flush=True)
        if args.output:
            args.output.write_text(json.dumps(report, indent=2) + "\n")
        cp.get_default_memory_pool().free_all_blocks()
    print(json.dumps({"complete": True, "cases": len(report["results"])}), flush=True)


if __name__ == "__main__":
    main()
