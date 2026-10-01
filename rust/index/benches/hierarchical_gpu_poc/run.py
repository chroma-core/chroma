#!/usr/bin/env python3
"""Run a pinned hierarchical SPANN baseline and save its inputs and output."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


REPO = Path(__file__).resolve().parents[4]


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=REPO, text=True).strip()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_output(command: list[str]) -> str | None:
    if shutil.which(command[0]) is None:
        return None
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dataset", choices=["wikipedia-en", "ms-marco"], required=True)
    parser.add_argument("--checkpoint", type=int, default=2)
    parser.add_argument("--checkpoint-size", type=int, default=150_000)
    parser.add_argument("--threads", type=int, required=True)
    parser.add_argument("--shard", type=Path, action="append", required=True,
                        help="Dataset parquet file in benchmark load order; repeat for every used shard")
    parser.add_argument("--ground-truth", type=Path,
                        help="Exact-neighbor reference file, if recall will be evaluated")
    parser.add_argument("--allow-dirty", action="store_true",
                        help="Allow an uncommitted development run; the manifest records this state")
    parser.add_argument("--capture-splits", type=int, default=0,
                        help="Capture this many real leaf inputs for replay; this run is not for timing")
    parser.add_argument("benchmark_args", nargs=argparse.REMAINDER,
                        help="Additional benchmark flags after --")
    args = parser.parse_args()

    if args.checkpoint <= 0 or args.checkpoint_size <= 0 or args.threads <= 0:
        parser.error("checkpoint, checkpoint-size, and threads must be positive")
    if args.capture_splits < 0:
        parser.error("capture-splits must be nonnegative")
    dirty = bool(git("status", "--porcelain", "--untracked-files=normal"))
    if dirty and not args.allow_dirty:
        parser.error("source checkout is dirty; commit changes or pass --allow-dirty for a development run")
    if args.output_dir.exists():
        parser.error(f"output directory already exists: {args.output_dir}")
    extra = args.benchmark_args
    if extra and extra[0] == "--":
        extra = extra[1:]
    command = ["cargo", "bench", "-p", "chroma-index", "--bench",
               "hierarchical_spann_profile_quantized", "--", "--dataset", args.dataset,
               "--checkpoint", str(args.checkpoint), "--checkpoint-size",
               str(args.checkpoint_size), "--threads", str(args.threads),
               "--parallel-balancing", "true", "--write-navigation", "fp",
               "--fp-npa", "--validate-postings", "--num-queries", "1000", *extra]

    files = []
    for path in [*args.shard, *([args.ground_truth] if args.ground_truth else [])]:
        resolved = path.resolve(strict=True)
        if not resolved.is_file():
            parser.error(f"not a file: {resolved}")
        files.append({"path": str(resolved), "bytes": resolved.stat().st_size,
                      "sha256": sha256(resolved)})

    args.output_dir.mkdir(parents=True)
    run_env = os.environ.copy()
    if args.capture_splits:
        capture_dir = args.output_dir / "split-fixtures"
        capture_dir.mkdir()
        run_env["HSPANN_SPLIT_CAPTURE_DIR"] = str(capture_dir.resolve())
        run_env["HSPANN_SPLIT_CAPTURE_LIMIT"] = str(args.capture_splits)
    manifest = {
        "schema_version": 1,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_commit": git("rev-parse", "HEAD"),
        "source_dirty": dirty,
        "cargo_lock_sha256": sha256(REPO / "Cargo.lock"),
        "command": command,
        "split_capture_limit": args.capture_splits,
        "working_directory": str(REPO / "rust"),
        "dataset_shards_in_load_order": files[:len(args.shard)],
        "ground_truth": files[-1] if args.ground_truth else None,
        "host": {"platform": platform.platform(), "machine": platform.machine(),
                 "processor": platform.processor(), "logical_cpus": os.cpu_count(),
                 "python": platform.python_version(),
                 "rustc": command_output(["rustc", "--version"]),
                 "cargo": command_output(["cargo", "--version"]),
                 "nvidia_smi": command_output(["nvidia-smi", "--query-gpu=name,driver_version,memory.total", "--format=csv,noheader"])},
        "environment": {key: os.environ.get(key) for key in
                        ("RUSTFLAGS", "RUSTUP_TOOLCHAIN", "CARGO_BUILD_TARGET", "CUDA_VISIBLE_DEVICES", "HF_HOME", "HF_HUB_CACHE")},
    }
    manifest_path = args.output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    with (args.output_dir / "benchmark.log").open("wb") as log:
        process = subprocess.Popen(command, cwd=REPO / "rust", env=run_env,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert process.stdout is not None
        for chunk in process.stdout:
            log.write(chunk)
            log.flush()
            sys.stdout.buffer.write(chunk)
            sys.stdout.buffer.flush()
        return_code = process.wait()
    manifest["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    manifest["exit_code"] = return_code
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return return_code


if __name__ == "__main__":
    sys.exit(main())
