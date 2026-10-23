#!/usr/bin/env python3
"""Prepare one fixed 100K before/after arm; perform no remote operations."""
import argparse
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import time

HERE = Path(__file__).resolve().parent
SOURCE = importlib.util.spec_from_file_location("epoch7_spec", HERE / "make-performance-spec-epoch7.py")
frozen = importlib.util.module_from_spec(SOURCE)
SOURCE.loader.exec_module(frozen)
remote_job = frozen.remote_job
WORKER, PEER = frozen.WORKER, frozen.PEER
BASE = "/home/ubuntu/spann-two-machine-v1/epoch9"
DESCRIPTOR = "/home/ubuntu/spann-two-machine-v1/epoch2/input/100k.json"
INPUT = "/home/ubuntu/spann-two-machine-v1/epoch2/input/fineweb100k.f32"


def artifact_bindings(arm):
    epoch = 7 if arm == "100k-before" else 9
    path = HERE / f"runtime-binaries-epoch{epoch}.json"
    hashes = json.loads(path.read_text())
    for name in ("two_machine_spann", "two_machine_reader"):
        if not re.fullmatch(r"[0-9a-f]{64}", hashes.get(name, "")):
            raise ValueError("exact reviewed executable hashes are required")
    return f"/home/ubuntu/spann-two-machine-v1/epoch{epoch}/build", hashes, hashlib.sha256(path.read_bytes()).hexdigest()


def generate(args, now):
    if args.arm not in ("100k-before", "100k-after"):
        raise ValueError("select exactly one fixed 100K before or after arm")
    if not re.fullmatch(r"epoch9-[a-z0-9-]{1,53}", args.window_id):
        raise ValueError("a fresh epoch9 window identity is required")
    if args.base != BASE or args.corpus_descriptor != DESCRIPTOR or args.input_file != INPUT:
        raise ValueError("reuse the exact retained 100K inputs and fresh epoch9 output base")
    if not now < args.job_deadline <= now + 590:
        raise ValueError("the immutable absolute job deadline must be within 590 seconds")
    underlying = copy.copy(args)
    underlying.arm = "100k-baseline-distributed"
    underlying.base = "/home/ubuntu/spann-two-machine-v1/epoch7"
    value = frozen.generate(underlying, now)
    binary_root, hashes, binding_digest = artifact_bindings(args.arm)
    owner = value["args"]["--owner"]
    value["job_id"] = f"{args.arm}-owner{owner}"
    value["binary"] = binary_root + "/two_machine_spann"
    value["binary_sha256"] = hashes["two_machine_spann"]
    value["reader"] = binary_root + "/two_machine_reader"
    value["reader_sha256"] = hashes["two_machine_reader"]
    value["args"]["--reader-binary"] = value["reader"]
    value["args"]["--output"] = BASE + "/" + value["job_id"]
    value["args"]["--run"] = int.from_bytes(hashlib.sha256((args.window_id + ":" + args.arm).encode()).digest()[:16], "big")
    value["provenance"].update(arm=args.arm, artifact_hash_file_sha256=binding_digest,
        comparison="epoch7 baseline versus reviewed reservation correction; source scope requires separate review; identical fixed100K input/settings",
        algorithm_workload="100k-baseline-distributed", intended_profile_scope="first balance on owner1, before placement")
    remote_job.validate(value, now)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", choices=("100k-before", "100k-after"), required=True)
    parser.add_argument("--window-id", required=True)
    parser.add_argument("--window-deadline", type=int, required=True)
    parser.add_argument("--job-deadline", type=int, required=True)
    parser.add_argument("--mode", choices=("run", "serve"), required=True)
    parser.add_argument("--worker-ip", required=True)
    parser.add_argument("--peer-ip", required=True)
    parser.add_argument("--base", default=BASE)
    parser.add_argument("--corpus-descriptor", default=DESCRIPTOR)
    parser.add_argument("--input-file", default=INPUT)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    value = generate(args, time.time())
    body = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    sidecar = args.output.with_name(args.output.name + ".sha256")
    if args.output.exists() or sidecar.exists():
        raise ValueError("preserve existing spec and SHA256 sidecar")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as file:
        file.write(body)
    digest = hashlib.sha256(body).hexdigest()
    with sidecar.open("x") as file:
        file.write(digest + "  " + args.output.name + "\n")
    print(json.dumps({"spec": str(args.output), "sha256": digest, "job_id": value["job_id"]}))


if __name__ == "__main__":
    main()
