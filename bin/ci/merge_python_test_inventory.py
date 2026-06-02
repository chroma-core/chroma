#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import datetime
import hashlib
import json
import pathlib
from typing import Any, Dict, List, Sequence, Tuple


SHARD_KIND = "chroma-python-test-inventory-shard"
MERGED_KIND = "chroma-python-test-inventory"


def main() -> None:
    args = parse_args()
    input_dir = pathlib.Path(args.input_dir)
    output_path = pathlib.Path(args.output_json)

    shards, errors = read_shards(input_dir)
    payload = merged_payload(shards, errors)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(f"{output_path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
    tmp_path.replace(output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge Chroma Python pytest inventory shard artifacts."
    )
    parser.add_argument("input_dir", help="Directory containing shard artifacts")
    parser.add_argument("output_json", help="Merged inventory JSON output path")
    return parser.parse_args()


def read_shards(input_dir: pathlib.Path) -> Tuple[List[Dict[str, Any]], List[str]]:
    shards: List[Dict[str, Any]] = []
    errors: List[str] = []

    if not input_dir.exists():
        errors.append(f"input directory does not exist: {input_dir}")
        return shards, errors

    for path in sorted(input_dir.rglob("*.json")):
        try:
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as e:
            errors.append(f"{path}: failed to read JSON: {e}")
            continue

        if not isinstance(payload, dict):
            errors.append(f"{path}: shard is not a JSON object")
            continue

        if payload.get("kind") != SHARD_KIND:
            errors.append(f"{path}: unexpected kind {payload.get('kind')!r}")
            continue

        shard = copy.deepcopy(payload)
        shard["source"] = {
            "path": str(path),
            "artifact": artifact_name(input_dir, path),
        }
        shards.append(shard)

    return shards, errors


def merged_payload(
    shards: Sequence[Dict[str, Any]], errors: Sequence[str]
) -> Dict[str, Any]:
    collected_count = 0
    unique_nodeids = set()
    shard_hashes: List[str] = []

    for shard in shards:
        collection = shard.get("collection", {})
        tests = collection.get("tests", [])
        if isinstance(tests, list):
            collected_count += len(tests)
            unique_nodeids.update(str(test) for test in tests)

        sha256 = collection.get("sha256")
        if isinstance(sha256, str):
            shard_hashes.append(sha256)

    return {
        "schema_version": 1,
        "kind": MERGED_KIND,
        "generated_at": utc_now(),
        "summary": {
            "shard_count": len(shards),
            "collected_count": collected_count,
            "unique_nodeid_count": len(unique_nodeids),
            "sha256": aggregate_sha256(shard_hashes),
        },
        "errors": list(errors),
        "shards": list(shards),
    }


def artifact_name(input_dir: pathlib.Path, path: pathlib.Path) -> str:
    try:
        relative = path.relative_to(input_dir)
    except ValueError:
        return ""

    if len(relative.parts) > 1:
        return relative.parts[0]
    return ""


def aggregate_sha256(shard_hashes: Sequence[str]) -> str:
    hasher = hashlib.sha256()
    for shard_hash in sorted(shard_hashes):
        hasher.update(shard_hash.encode("utf-8"))
        hasher.update(b"\0")
    return hasher.hexdigest()


def utc_now() -> str:
    return (
        datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    )


if __name__ == "__main__":
    main()
