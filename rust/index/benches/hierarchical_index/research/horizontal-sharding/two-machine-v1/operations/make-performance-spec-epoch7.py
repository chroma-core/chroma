#!/usr/bin/env python3
"""Generate one frozen performance-arm job spec; perform no remote operations."""
import argparse
import hashlib
import importlib.util
import ipaddress
import json
from pathlib import Path, PurePosixPath
import re
import time

HERE = Path(__file__).resolve().parent
PLAN_SHA256 = "39f31ba042f79d29e678278a5147343f218088a6675fd9cb31746f606ccb46b4"
WORKER = "i-011ff180f03584f40"
PEER = "i-00268d1e1dfea0357"
BINARY_ROOT = "/home/ubuntu/spann-two-machine-v1/epoch7/build"
BINARY_HASHES = json.loads((HERE / "runtime-binaries-epoch7.json").read_text())
PERFORMANCE_SHA256 = BINARY_HASHES["two_machine_spann"]
READER_SHA256 = BINARY_HASHES["two_machine_reader"]
REFERENCE = "/home/ubuntu/spann-single-shard-3m-attribution-v1/epoch2/run/reference.bin"
REFERENCE_SHA256 = "6ff2767b809716df3994a5b5a76a38d7fcc267f69f4336cbc56a04d41ba18885"
REMOTE = importlib.util.spec_from_file_location("spann_remote_job", HERE / "remote-job.py")
remote_job = importlib.util.module_from_spec(REMOTE)
REMOTE.loader.exec_module(remote_job)


def absolute(value):
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts or "\x00" in value:
        raise ValueError("a normalized absolute remote path is required")
    return str(path)


def private_ip(value):
    address = ipaddress.ip_address(value)
    networks = [ipaddress.ip_network(n) for n in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")]
    if address.version != 4 or not any(address in network for network in networks):
        raise ValueError("an IPv4 private sandbox address is required")
    return str(address)


def generate(args, now):
    plan_bytes = (HERE / "workload-plan.json").read_bytes()
    if hashlib.sha256(plan_bytes).hexdigest() != PLAN_SHA256:
        raise ValueError("the frozen six-arm workload plan changed; review a new generator binding")
    plan = json.loads(plan_bytes)
    if args.arm not in plan["order"] or args.arm == "correctness":
        raise ValueError("select exactly one of the six frozen performance arms")
    if not re.fullmatch(r"[a-z0-9-]{1,60}", args.window_id):
        raise ValueError("invalid exact shutdown-window identity")
    if args.mode not in ("run", "serve"):
        raise ValueError("invalid workload mode")
    reference = args.arm.endswith("-reference")
    if reference and args.mode != "run":
        raise ValueError("the reference runs on the worker only")
    suffix = "-reference" if reference else "-distributed"
    workload = next(w for w in plan["workloads"] if w["name"] == args.arm.removesuffix(suffix))
    shared = plan["shared"]
    worker_ip, peer_ip = private_ip(args.worker_ip), private_ip(args.peer_ip)
    if worker_ip == peer_ip:
        raise ValueError("two distinct physical owner addresses are required")
    base = absolute(args.base)
    if base != "/home/ubuntu/spann-two-machine-v1/epoch7":
        raise ValueError("the admitted output base is the fresh epoch7 namespace")
    descriptor = absolute(args.corpus_descriptor)
    owner = 1 if args.mode == "run" else 2
    job_id = args.arm + ("-owner1" if owner == 1 else "-owner2")
    run_id = int.from_bytes(hashlib.sha256((args.window_id + ":" + args.arm).encode()).digest()[:16], "big")
    reader = BINARY_ROOT + "/two_machine_reader"
    spec = {
        "combined_review_passed": True,
        "window_id": args.window_id,
        "job_id": job_id,
        "instance_id": WORKER if owner == 1 else PEER,
        "kind": "reference" if reference else "performance",
        "mode": args.mode,
        "binary": REFERENCE if reference else BINARY_ROOT + "/two_machine_spann",
        "binary_sha256": REFERENCE_SHA256 if reference else PERFORMANCE_SHA256,
        "reader": reader,
        "reader_sha256": READER_SHA256,
        "window_deadline_unix": args.window_deadline,
        "deadline_unix": args.job_deadline,
        "workload_timeout_seconds": 600,
        "minimum_mem_available_bytes": 32 * 1024**3,
        "minimum_free_bytes": (12 if reference and workload["rows"] == 3_000_000 else 2) * 1024**3,
        "provenance": {"workload_plan_sha256": PLAN_SHA256, "arm": args.arm, "replicates": 1, "input_bytes": workload["input_bytes"], "input_sha256": workload["input_sha256"], "physical_cores_required_by_remote_job": 32},
    }
    if reference:
        if not args.input_file:
            raise ValueError("the reference requires an explicit --input-file")
        spec["args"] = {"--input": absolute(args.input_file), "--input-sha256": workload["input_sha256"], "--output-dir": base + "/" + job_id, "--rows": workload["rows"], "--dim": shared["dimensions"], "--workers": 32, "--initial-batch-rows": workload["initial_batch_rows"], "--batch-transition-rows": shared["transition_rows"], "--later-batch-rows": shared["later_batch_rows"]}
    else:
        host_ip, other_ip = (worker_ip, peer_ip) if owner == 1 else (peer_ip, worker_ip)
        spec["args"] = {"--mode": args.mode, "--owner": owner, "--run": run_id, "--output": base + "/" + job_id, "--data-bind": host_ip + ":9100", "--control-bind": host_ip + ":9101", "--peer-data": other_ip + ":9100", "--peer-control": other_ip + ":9101", "--token-file": "/home/ubuntu/spann-two-machine-v1/epoch1/run-auth.token", "--shutdown-at-unix": args.job_deadline, "--reader-binary": reader, "--input-descriptor": descriptor, "--validation-queries": 16, "--vector-cache-bytes": 12 * 1024**3, "--workers": 32, "--rows": workload["rows"], "--initial-batch": workload["initial_batch_rows"], "--transition": shared["transition_rows"], "--later-batch": shared["later_batch_rows"], "--owner-budget": workload["owner_budget"], "--first-owner-budget": workload["first_owner_budget"], "--split-threshold": shared["split_threshold"], "--merge-threshold": shared["merge_threshold"], "--branching-factor": shared["branching_factor"]}
    remote_job.validate(spec, now)
    return spec


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-id", required=True)
    parser.add_argument("--window-deadline", type=int, required=True)
    parser.add_argument("--job-deadline", type=int, required=True)
    parser.add_argument("--arm", required=True)
    parser.add_argument("--mode", choices=("run", "serve"), required=True)
    parser.add_argument("--worker-ip", required=True)
    parser.add_argument("--peer-ip", required=True)
    parser.add_argument("--base", default="/home/ubuntu/spann-two-machine-v1/epoch7")
    parser.add_argument("--corpus-descriptor", required=True)
    parser.add_argument("--input-file", help="explicit retained canonical f32 file for the reference")
    parser.add_argument("--output", type=Path, required=True, help="fresh local spec path; .sha256 sidecar is also created")
    args = parser.parse_args()
    spec = generate(args, time.time())
    body = (json.dumps(spec, indent=2, sort_keys=True) + "\n").encode()
    sidecar = args.output.with_name(args.output.name + ".sha256")
    if args.output.exists() or sidecar.exists():
        raise ValueError("preserve existing spec and SHA-256 sidecar")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as file:
        file.write(body)
    digest = hashlib.sha256(body).hexdigest()
    with sidecar.open("x") as file:
        file.write(digest + "  " + args.output.name + "\n")
    print(json.dumps({"spec": str(args.output), "sha256": digest, "sha256_file": str(sidecar), "instance_id": spec["instance_id"], "job_id": spec["job_id"]}))


if __name__ == "__main__":
    main()
