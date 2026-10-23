#!/usr/bin/env python3
"""Run a deterministic shard of a pytest selection."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from typing import Sequence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard-index", type=int, default=1)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument(
        "--pytest-arg",
        action="append",
        default=[],
        help="Argument to pass to the pytest run command. Repeat as needed.",
    )
    parser.add_argument("pytest_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.pytest_args[:1] == ["--"]:
        args.pytest_args = args.pytest_args[1:]
    if args.shard_count < 1:
        parser.error("--shard-count must be >= 1")
    if args.shard_index < 1 or args.shard_index > args.shard_count:
        parser.error("--shard-index must be between 1 and --shard-count")
    return args


def run(cmd: Sequence[str]) -> subprocess.CompletedProcess[str]:
    print("+ " + " ".join(cmd), flush=True)
    return subprocess.run(cmd, text=True)


def main() -> int:
    args = parse_args()
    if args.shard_count == 1:
        cmd = [
            sys.executable,
            "-m",
            "pytest",
            *args.pytest_arg,
            *args.pytest_args,
        ]
        return run(cmd).returncode

    plugin_dir = None
    try:
        plugin_dir = tempfile.TemporaryDirectory(prefix="pytest-shard-")
        with open(
            os.path.join(plugin_dir.name, "pytest_shard_plugin.py"),
            "w",
            encoding="utf-8",
        ) as f:
            f.write(
                """
import os


def pytest_collection_modifyitems(config, items):
    shard_index = int(os.environ["PYTEST_SHARD_INDEX"]) - 1
    shard_count = int(os.environ["PYTEST_SHARD_COUNT"])
    selected = []
    deselected = []
    for offset, item in enumerate(items):
        if offset % shard_count == shard_index:
            selected.append(item)
        else:
            deselected.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected

    print(
        f"selected {len(selected)} of {len(selected) + len(deselected)} tests "
        f"for shard {shard_index + 1}/{shard_count}",
        flush=True,
    )
"""
            )

        env = os.environ.copy()
        env["PYTEST_SHARD_INDEX"] = str(args.shard_index)
        env["PYTEST_SHARD_COUNT"] = str(args.shard_count)
        env["PYTHONPATH"] = (
            plugin_dir.name
            if not env.get("PYTHONPATH")
            else plugin_dir.name + os.pathsep + env["PYTHONPATH"]
        )
        cmd = [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "pytest_shard_plugin",
            *args.pytest_arg,
            *args.pytest_args,
        ]
        print("+ " + " ".join(cmd), flush=True)
        return subprocess.run(cmd, env=env, text=True).returncode
    finally:
        if plugin_dir is not None:
            plugin_dir.cleanup()


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    raise SystemExit(main())
