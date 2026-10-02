#!/usr/bin/env python3
"""Measure GPU scoring for the reader's 1-bit code format."""

from __future__ import annotations

import argparse
import json
import statistics
import struct
import time

import numpy as np


KERNEL = r"""
extern "C" __global__
void score_codes(const unsigned char* codes, const unsigned long long* planes,
                 float* output, int count, unsigned int sum_q_u,
                 float v_l, float delta, float c_norm, float c_dot_q, float q_norm) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const unsigned char* row = codes + (size_t)i * 144;
    float correction = *(const float*)(row);
    float norm = *(const float*)(row + 4);
    float radial = *(const float*)(row + 8);
    int signed_sum = *(const int*)(row + 12);
    const unsigned long long* bits = (const unsigned long long*)(row + 16);
    unsigned int dot = 0;
    for (int j = 0; j < 16; ++j) {
        unsigned long long x = bits[j];
        dot += __popcll(x & planes[j]);
        dot += 2 * __popcll(x & planes[16 + j]);
        dot += 4 * __popcll(x & planes[32 + j]);
        dot += 8 * __popcll(x & planes[48 + j]);
    }
    float signed_dot = 2.0f * (float)dot - (float)sum_q_u;
    float g_dot_r_q = 0.5f * (delta * signed_dot + v_l * (float)signed_sum);
    float r_dot_r_q = norm * g_dot_r_q / correction;
    float d_dot_q = c_dot_q + radial + r_dot_r_q;
    float d_norm_sq = c_norm * c_norm + 2.0f * radial + norm * norm;
    output[i] = d_norm_sq + q_norm * q_norm - 2.0f * d_dot_q;
}
"""


def cpu_score(row: np.ndarray, planes: np.ndarray, values: tuple[float, ...]) -> float:
    correction, norm, radial, signed_sum = struct.unpack_from("<fffi", row)
    sum_q_u, v_l, delta, c_norm, c_dot_q, q_norm = values
    dot = 0
    for j in range(16):
        x = struct.unpack_from("<Q", row, 16 + 8 * j)[0]
        for bit in range(4):
            q = struct.unpack_from("<Q", planes, 128 * bit + 8 * j)[0]
            dot += (1 << bit) * (x & q).bit_count()
    signed_dot = 2 * dot - sum_q_u
    g_dot_r_q = 0.5 * (delta * signed_dot + v_l * signed_sum)
    r_dot_r_q = norm * g_dot_r_q / correction
    d_dot_q = c_dot_q + radial + r_dot_r_q
    d_norm_sq = c_norm * c_norm + 2 * radial + norm * norm
    return float(d_norm_sq + q_norm * q_norm - 2 * d_dot_q)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codes", type=int, default=133_100)
    parser.add_argument("--repetitions", type=int, default=5)
    args = parser.parse_args()
    if args.codes <= 0 or args.repetitions <= 0:
        parser.error("codes and repetitions must be positive")
    import cupy as cp

    rng = np.random.default_rng(0x7810)
    codes = rng.integers(0, 256, size=(args.codes, 144), dtype=np.uint8)
    headers = np.ndarray(
        shape=(args.codes,),
        dtype=[("correction", "<f4"), ("norm", "<f4"),
               ("radial", "<f4"), ("signed_sum", "<i4")],
        buffer=codes,
        strides=(144,),
    )
    headers["correction"] = rng.uniform(1, 3, args.codes)
    headers["norm"] = rng.uniform(0.5, 2, args.codes)
    headers["radial"] = rng.uniform(-0.2, 0.2, args.codes)
    headers["signed_sum"] = rng.integers(-1024, 1025, args.codes)
    planes = rng.integers(0, 256, size=512, dtype=np.uint8)
    values = (760, -0.2, 0.03, 1.1, 0.4, 1.5)
    kernel_values = (np.uint32(values[0]), *(np.float32(x) for x in values[1:]))

    kernel = cp.RawKernel(KERNEL, "score_codes")
    warm_codes = cp.asarray(codes[:1])
    warm_planes = cp.asarray(planes).view(cp.uint64)
    warm_output = cp.empty(1, dtype=cp.float32)
    kernel(
        (1,),
        (128,),
        (warm_codes, warm_planes, warm_output, np.int32(1), *kernel_values),
    )
    cp.cuda.Stream.null.synchronize()

    transfer_ms = []
    kernel_ms = []
    total_ms = []
    output = None
    for _ in range(args.repetitions):
        start = time.perf_counter()
        gpu_codes = cp.asarray(codes)
        gpu_planes = cp.asarray(planes).view(cp.uint64)
        gpu_output = cp.empty(args.codes, dtype=cp.float32)
        cp.cuda.Stream.null.synchronize()
        copied = time.perf_counter()
        kernel(((args.codes + 255) // 256,), (256,),
               (gpu_codes, gpu_planes, gpu_output,
                np.int32(args.codes), *kernel_values))
        cp.cuda.Stream.null.synchronize()
        scored = time.perf_counter()
        output = cp.asnumpy(gpu_output)
        total = time.perf_counter()
        transfer_ms.append((copied - start) * 1000)
        kernel_ms.append((scored - copied) * 1000)
        total_ms.append((total - start) * 1000)

    assert output is not None
    expected = np.array([cpu_score(row, planes, values)
                         for row in codes[:min(256, args.codes)]])
    measured = output[:len(expected)]
    report = {
        "codes": args.codes,
        "bytes_per_code": 144,
        "repetitions": args.repetitions,
        "median_host_to_gpu_ms": statistics.median(transfer_ms),
        "median_gpu_kernel_ms": statistics.median(kernel_ms),
        "median_total_with_transfers_ms": statistics.median(total_ms),
        "max_reference_error": float(np.max(np.abs(expected - measured))),
        "reference_match": bool(np.allclose(expected, measured, rtol=1e-4, atol=1e-3)),
        "note": "Synthetic codes use the reader's 1-bit layout and Euclidean formula; this measures scoring only, not tree search or disk reads.",
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
