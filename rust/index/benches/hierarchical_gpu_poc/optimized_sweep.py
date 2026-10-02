#!/usr/bin/env python3
"""Measure streaming alternatives for component ceilings."""

import argparse
import json
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import cupy as cp
import numpy as np
from ceiling_variants import GROUP_SUM, QUANTIZE_STREAM, QUANTIZE_WARP
from component_kernels import file_sha256, cpu_quantize
from replay_split import load_fixture
from saturation_kernels import measure


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=float, default=2)
    parser.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        default=[1000000, 2000000, 4000000, 8000000, 16000000],
    )
    parser.add_argument("--tiles", nargs="+", type=int, default=[256, 512, 1024])
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
    centers = gb[:2].copy()
    norms = cp.sum(centers * centers, axis=1)
    mean = gb.mean(axis=0)
    report: dict[str, Any] = {
        "gpu": cp.cuda.runtime.getDeviceProperties(0)["name"].decode(),
        "cpu_lscpu": json.loads(
            subprocess.check_output(["lscpu", "--json"], text=True)
        ),
        "fixture_sha256": file_sha256(args.fixture),
        "dimension": metadata["dim"],
        "results": [],
        "skipped": [],
        "bandwidth": "effective minimum algorithm bytes; no hardware bandwidth counter",
    }
    try:
        for n in args.sizes:
            free, _ = cp.cuda.runtime.memGetInfo()
            if n * metadata["dim"] * 4 > free * 0.60:
                report["skipped"].append(n)
                continue
            x = cp.tile(gb, (int(np.ceil(n / len(base))), 1))[:n].copy()
            cp.get_default_memory_pool().free_all_blocks()
            dim = x.shape[1]
            labels = cp.arange(n, dtype=cp.int32) % 2
            counts = cp.bincount(labels, minlength=2)[:, None]
            out = cp.empty((2, dim), dtype=cp.float32)
            for tile in args.tiles:
                partial = cp.empty((int(np.ceil(n / tile)), 2, dim), dtype=cp.float32)

                def centroid(
                    x: Any = x,
                    labels: Any = labels,
                    partial: Any = partial,
                    out: Any = out,
                    counts: Any = counts,
                ) -> None:
                    GROUP_SUM(
                        (int(np.ceil(dim / 128)), len(partial)),
                        (128,),
                        (x, labels, partial, n, dim, tile),
                    )
                    cp.sum(partial, axis=0, out=out)
                    cp.divide(out, counts, out=out)

                row = measure(
                    centroid,
                    args.seconds,
                    x.nbytes + labels.nbytes + 2 * partial.nbytes,
                    n * dim,
                    samples,
                )
                row.update(
                    component="two_centroids_stream",
                    vectors=n,
                    tile=tile,
                    vectors_per_second=n / row["seconds"],
                )
                expected = np.stack((base[::2].mean(axis=0), base[1::2].mean(axis=0)))
                # Every requested size is an even multiple of the public fixture.
                error = float(np.max(np.abs(cp.asnumpy(out) - expected)))
                assert error < 1e-5, error
                row["max_center_abs_error"] = error
                report["results"].append(row)
                print(json.dumps(row), flush=True)

                def iteration(
                    x: Any = x, partial: Any = partial, out: Any = out
                ) -> None:
                    chosen = cp.argmin(norms - 2 * (x @ centers.T), axis=1).astype(
                        cp.int32
                    )
                    GROUP_SUM(
                        (int(np.ceil(dim / 128)), len(partial)),
                        (128,),
                        (x, chosen, partial, n, dim, tile),
                    )
                    cp.sum(partial, axis=0, out=out)
                    cp.divide(
                        out,
                        cp.maximum(cp.bincount(chosen, minlength=2)[:, None], 1),
                        out=out,
                    )

                row = measure(
                    iteration,
                    args.seconds,
                    2 * x.nbytes + 16 * n + 2 * partial.nbytes,
                    5 * n * dim,
                    samples,
                )
                row.update(
                    component="two_means_stream",
                    vectors=n,
                    tile=tile,
                    vectors_per_second=n / row["seconds"],
                )
                base_labels = np.argmin(
                    cp.asnumpy(norms) - 2 * (base @ cp.asnumpy(centers).T), axis=1
                )
                expected = np.stack(
                    [base[base_labels == k].mean(axis=0) for k in range(2)]
                )
                error = float(np.max(np.abs(cp.asnumpy(out) - expected)))
                assert error < 1e-5, error
                row["max_center_abs_error"] = error
                report["results"].append(row)
                print(json.dumps(row), flush=True)
                del centroid, iteration, partial
                cp.get_default_memory_pool().free_all_blocks()
            codes = cp.empty((n, dim // 8), dtype=cp.uint8)
            stats = cp.empty((n, 4), dtype=cp.float32)

            for kernel, name, rows_per_block in [
                (QUANTIZE_STREAM, "quantization_stream", 1),
                (QUANTIZE_WARP, "quantization_warp", 4),
            ]:

                def quantize(
                    x: Any = x, codes: Any = codes, stats: Any = stats
                ) -> None:
                    kernel(
                        (int(np.ceil(n / rows_per_block)),),
                        (128,),
                        (x, mean, codes, stats, n, dim),
                    )

                row = measure(
                    quantize,
                    args.seconds,
                    x.nbytes + codes.nbytes + stats.nbytes,
                    0,
                    samples,
                )
                checked = np.concatenate((np.arange(256), np.arange(n - 256, n)))
                expected_codes, expected_stats = cpu_quantize(
                    cp.asnumpy(x[checked]), cp.asnumpy(mean)
                )
                assert np.array_equal(expected_codes, cp.asnumpy(codes[checked]))
                header_error = float(
                    np.max(np.abs(expected_stats - cp.asnumpy(stats[checked])))
                )
                assert header_error < 1e-4, header_error
                row.update(
                    component=name,
                    vectors=n,
                    vectors_per_second=n / row["seconds"],
                    sampled_code_agreement=True,
                    max_sampled_header_abs_error=header_error,
                )
                report["results"].append(row)
                print(json.dumps(row), flush=True)
                del quantize
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            del codes, stats, x, labels, counts, out
            cp.get_default_memory_pool().free_all_blocks()
        print("BENCHMARK_COMPLETE", flush=True)
    finally:
        process.terminate()
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print("REPORT_JSON=" + json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
