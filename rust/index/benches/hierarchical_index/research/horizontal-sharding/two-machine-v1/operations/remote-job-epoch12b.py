#!/usr/bin/env python3
"""Run one reviewed binary under an existing, independently installed guard."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import time
import urllib.request

ACCOUNT, REGION = "079819354890", "us-east-1"
INSTANCES = {"i-011ff180f03584f40", "i-00268d1e1dfea0357"}
LOCK = Path("/home/ubuntu/spann-benchmark.lock")
COMMON = {"--mode", "--owner", "--run", "--output", "--data-bind", "--control-bind", "--peer-data", "--peer-control", "--token-file", "--shutdown-at-unix", "--reader-binary"}
ALLOWED = {
    "correctness": COMMON | {"--input-file"},
    "performance": COMMON | {"--input-descriptor", "--validation-queries", "--vector-cache-bytes", "--workers", "--rows", "--initial-batch", "--transition", "--later-batch", "--owner-budget", "--first-owner-budget", "--split-threshold", "--merge-threshold", "--branching-factor"},
    "reference": {"--input", "--output-dir", "--rows", "--dim", "--workers", "--input-sha256", "--initial-batch-rows", "--batch-transition-rows", "--later-batch-rows"},
}


def sha(path):
    value = hashlib.sha256()
    with open(path, "rb") as file:
        for block in iter(lambda: file.read(8 * 1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def write(path, body):
    temporary = path.with_name(path.name + ".tmp")
    with open(temporary, "x") as file:
        json.dump(body, file, indent=2)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    os.replace(temporary, path)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate reviewed JSON field")
        result[key] = value
    return result


def validate(spec, now):
    if spec.get("combined_review_passed") is not True or spec.get("instance_id") not in INSTANCES:
        raise ValueError("reviewed admission and an exact authorized instance are required")
    for field in ("job_id", "window_id"):
        if not re.fullmatch(r"[a-z0-9-]{1,100}", spec.get(field, "")):
            raise ValueError("invalid job/window identity")
    kind, mode = spec.get("kind"), spec.get("mode")
    if kind not in ALLOWED or mode not in ("run", "serve") or (kind == "reference" and mode != "run"):
        raise ValueError("invalid executable role")
    for field in ("deadline_unix","window_deadline_unix","workload_timeout_seconds","minimum_mem_available_bytes","minimum_free_bytes"):
        if not isinstance(spec.get(field),int) or isinstance(spec[field],bool):
            raise ValueError("integer time/resource bounds are required")
    if not now < spec["deadline_unix"] <= spec["window_deadline_unix"] - 10 or spec["window_deadline_unix"] - now > 5400 or not 0 < spec["workload_timeout_seconds"] <= 600:
        raise ValueError("job needs a fresh bounded deadline inside its existing window")
    for field in ("binary_sha256", "reader_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", spec.get(field, "")):
            raise ValueError("binary and reader SHA-256 bindings are required")
    for field in ("binary", "reader"):
        if not Path(spec[field]).is_absolute():
            raise ValueError("binary paths must be absolute")
    if spec["minimum_mem_available_bytes"] <= 0 or spec["minimum_free_bytes"] <= 0:
        raise ValueError("explicit positive memory and disk headroom are required")
    args = spec["args"]
    if not isinstance(args, dict) or set(args) - ALLOWED[kind] or any(not isinstance(v, (str, int)) or isinstance(v, bool) or "\x00" in str(v) for v in args.values()):
        raise ValueError("arguments differ from the selected executable allowlist")
    output_flag = "--output-dir" if kind == "reference" else "--output"
    required = {output_flag, "--input", "--input-sha256", "--rows", "--workers"} if kind == "reference" else COMMON - {"--reader-binary"}
    required |= {"--input-file"} if kind == "correctness" else {"--input-descriptor", "--owner-budget", "--rows", "--workers"} if kind == "performance" else set()
    if not required <= set(args):
        raise ValueError("required workload arguments must be explicit")
    if kind != "reference":
        if args["--mode"] != mode or int(args["--owner"]) != (1 if mode == "run" else 2) or int(args["--shutdown-at-unix"]) != int(spec["deadline_unix"]):
            raise ValueError("mode, owner or binary deadline differs from reviewed job")
        if mode == "run" and args.get("--reader-binary") != spec["reader"]:
            raise ValueError("fresh-reader path differs from reviewed binary")
    if not Path(str(args[output_flag])).is_absolute():
        raise ValueError("output must be a fresh absolute task path")
    gate=spec.get("profile_gate")
    if gate is not None:
        expected_job="100k-reference-owner1" if kind=="reference" else "100k-distributed-owner1"
        if (kind not in ("reference","performance") or mode!="run" or args.get("--rows")!=100000
            or args.get("--workers")!=32 or spec["job_id"]!=expected_job
            or not spec["window_id"].startswith("epoch12-")
            or str(args[output_flag])!="/home/ubuntu/spann-two-machine-v1/epoch12b/"+expected_job
            or gate!=str(args[output_flag])+"-profile-gate"):
            raise ValueError("profile gate must bind the fixed100K owner1 workload and fresh sibling directory")
    return [spec["binary"]] + [part for key in sorted(args) for part in (key, str(args[key]))], Path(str(args[output_flag]))


def imds(expected):
    # Ignore proxy environment settings for the link-local identity service.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    token_request = urllib.request.Request("http://169.254.169.254/latest/api/token", method="PUT", headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
    with opener.open(token_request, timeout=5) as response:
        token = response.read().decode()
    request = urllib.request.Request("http://169.254.169.254/latest/dynamic/instance-identity/document", headers={"X-aws-ec2-metadata-token": token})
    with opener.open(request, timeout=5) as response:
        identity = json.load(response)
    if (identity["accountId"], identity["region"], identity["instanceId"]) != (ACCOUNT, REGION, expected):
        raise ValueError("actual instance identity differs from reviewed sandbox resource")
    return identity


def output(command):
    return subprocess.check_output(command, text=True, timeout=5).strip()


def shutdown_guard(spec):
    unit = "spann-two-machine-" + spec["window_id"] + "-stop"
    properties = output(["systemctl", "show", unit + ".timer", "-p", "ActiveState", "-p", "NextElapseUSecRealtime", "-p", "Triggers"])
    fields = dict(line.split("=", 1) for line in properties.splitlines())
    if fields.get("ActiveState") != "active" or fields.get("Triggers") != unit + ".service":
        raise ValueError("the exact remote shutdown timer is not active")
    timer_time = int(output(["date", "-d", fields["NextElapseUSecRealtime"], "+%s"]))
    if abs(timer_time - spec["window_deadline_unix"]) > 2:
        raise ValueError("remote shutdown timer has a different absolute deadline")
    service = output(["systemctl", "show", unit + ".service", "-p", "ExecStart", "-p", "User"])
    values = dict(line.split("=", 1) for line in service.splitlines())
    if values.get("User") not in ("", "root") or not re.search(r"argv\[\]=/(?:usr/)?sbin/shutdown -h now(?: ;|$)", values.get("ExecStart", "")):
        raise ValueError("timer does not invoke the expected root shutdown action")
    return {"unit": unit, "properties": fields, "service": values, "deadline_unix": timer_time}


def available_memory():
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise ValueError("MemAvailable is unavailable")


def start_ticks(pid):
    return int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19])


def owned_group(pgid, marker):
    found = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            if os.getpgid(pid) != pgid:
                continue
            fields=(entry / "stat").read_text().rsplit(")",1)[1].split()
            if fields[0] in ("Z","X"):
                continue
            environment = (entry / "environ").read_bytes().split(b"\x00")
            if ("SPANN_OWNED_JOB=" + marker).encode() not in environment:
                raise ValueError("refuse to signal a process group containing an unowned process")
            found.append({"pid": pid, "start_ticks": int(fields[19])})
        except (FileNotFoundError, ProcessLookupError):
            continue
    return found


def terminate(pgid, marker):
    victims = owned_group(pgid, marker)
    if not victims:
        return []
    os.killpg(pgid, signal.SIGTERM)
    end = time.monotonic() + 5
    while time.monotonic() < end:
        if not owned_group(pgid, marker):
            return victims
        time.sleep(0.1)
    if owned_group(pgid, marker):
        os.killpg(pgid, signal.SIGKILL)
    end=time.monotonic()+5
    while owned_group(pgid,marker):
        if time.monotonic()>=end:
            raise RuntimeError("owned process group did not terminate")
        time.sleep(0.1)
    return victims


def cleanup_owned(child, marker, evidence, host_deadline):
    """Return only after readback is clean or the fixed host deadline arrives.

    The caller still owns the exclusive lock throughout this function. Repeated
    operator signals cannot interrupt cleanup and release that lock early.
    """
    signals=(signal.SIGTERM,signal.SIGINT,signal.SIGHUP)
    previous={sig:signal.getsignal(sig) for sig in signals}
    for sig in signals:
        signal.signal(sig,signal.SIG_IGN)
    def record(name,body):
        try:
            write(evidence/name,body)
        except OSError:
            pass  # A full disk must not release ownership of a live writer.
    try:
        error=None
        try:
            terminate(child.pid,marker)
            child.wait(timeout=10)
            survivors=owned_group(child.pid,marker)
            if not survivors:
                record("cleanup.json",{"confirmed":True,"survivors":[],"observed_unix":time.time()})
                return True
            error="owned processes remain after termination"
        except BaseException as failure:
            error=str(failure)
        record("cleanup-failed.json",{"error":error,"lock_retained":True,"host_deadline_unix":host_deadline,"observed_unix":time.time()})
        while time.time()<host_deadline:
            try:
                survivors=owned_group(child.pid,marker)
                if not survivors and child.poll() is not None:
                    child.wait(timeout=1)
                    record("cleanup.json",{"confirmed":True,"survivors":[],"after_failed_termination":True,"observed_unix":time.time()})
                    return True
            except BaseException as failure:
                survivors={"readback_error":str(failure)}
            record("cleanup-survivors.json",{"survivors":survivors,"lock_retained":True,"host_deadline_unix":host_deadline,"observed_unix":time.time()})
            time.sleep(min(1,max(0,host_deadline-time.time())))
        record("cleanup.json",{"confirmed":False,"lock_held_until_fixed_host_deadline":True,"host_deadline_unix":host_deadline,"observed_unix":time.time()})
        return False
    finally:
        for sig,handler in previous.items():
            signal.signal(sig,handler)


def workload_environment(spec,marker):
    environment=dict(os.environ,SPANN_OWNED_JOB=marker)
    environment.pop("BENCH_PROFILE_GATE",None)
    if spec.get("profile_gate") is not None:
        environment["BENCH_PROFILE_GATE"]=spec["profile_gate"]
    return environment


def run(args):
    if sha(args.spec) != args.spec_sha256:
        raise ValueError("reviewed job spec SHA-256 mismatch")
    spec = json.loads(args.spec.read_text(), object_pairs_hook=unique_object)
    command, workload_output = validate(spec, time.time())
    args.evidence.mkdir(parents=True, exist_ok=False, mode=0o700)
    write(args.evidence / "spec.json", spec)
    child, marker, reason = None, spec["job_id"] + ":" + args.spec_sha256, "preflight_failed"
    started = time.time()
    lock = LOCK.open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        identity = imds(spec["instance_id"])
        guard = shutdown_guard(spec)
        if workload_output.exists():
            raise ValueError("workload output already exists; preserve previous artifacts")
        for field, expected in (("binary", "binary_sha256"), ("reader", "reader_sha256")):
            path = Path(spec[field])
            if path.is_symlink() or not path.is_file() or not os.access(path, os.X_OK) or sha(path) != spec[expected]:
                raise ValueError("admitted executable bytes differ: " + field)
        parent = workload_output.parent
        parent.mkdir(parents=True, exist_ok=True)
        free, memory = shutil.disk_usage(parent).free, available_memory()
        if free < spec["minimum_free_bytes"] or memory < spec["minimum_mem_available_bytes"]:
            raise ValueError("insufficient memory or disk headroom; retained artifacts remain untouched")
        topology=output(["lscpu","-p=CPU,CORE,SOCKET"])
        cores={tuple(line.split(",")[1:]) for line in topology.splitlines() if line and not line.startswith("#")}
        if len(cores)!=32:
            raise ValueError("the admitted machine must expose 32 physical cores")
        if spec["kind"] == "reference" and sha(Path(str(spec["args"]["--input"]))) != spec["args"]["--input-sha256"]:
            raise ValueError("reference input bytes differ from reviewed corpus")
        deadline = min(spec["deadline_unix"], time.time() + spec["workload_timeout_seconds"])
        if deadline - time.time() < 10:
            raise ValueError("insufficient remaining workload window")
        write(args.evidence / "preflight.json", {"spec_sha256": args.spec_sha256, "identity": identity, "remote_guard": guard, "binary_sha256": spec["binary_sha256"], "reader_sha256": spec["reader_sha256"], "argv": command, "memory_available_bytes": memory, "disk_free_bytes": free, "physical_cores":len(cores), "cpu_topology":topology, "deadline_unix": deadline, "workload_timeout_seconds": spec["workload_timeout_seconds"]})
        with (args.evidence / "stdout").open("xb") as stdout, (args.evidence / "stderr").open("xb") as stderr:
            environment=workload_environment(spec,marker)
            if spec.get("profile_gate") is not None:
                Path(spec["profile_gate"]).mkdir(mode=0o755,exist_ok=False)
            child = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=environment, start_new_session=True)
            running = {"job_id": spec["job_id"], "instance_id": spec["instance_id"], "spec_sha256": args.spec_sha256, "pid": child.pid, "pgid": child.pid, "start_ticks": start_ticks(child.pid), "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(), "marker": marker, "deadline_unix": deadline}
            write(args.evidence / "running.json", running)
            reason = "exited"
            while child.poll() is None:
                if (args.evidence / "stop-request.json").exists():
                    reason = "operator_stop"
                    break
                if time.time() >= deadline:
                    reason = "absolute_deadline_or_600s_timeout"
                    break
                time.sleep(0.1)
            if reason=="exited" and (args.evidence / "stop-request.json").exists():
                reason="operator_stop"
    except BaseException as error:
        if isinstance(error,(InterruptedError,KeyboardInterrupt)):
            reason="wrapper_interrupted"
        try:
            write(args.evidence / "failure.json", {"error": str(error), "reason": reason, "observed_unix": time.time()})
        except OSError:
            pass
        raise
    finally:
        cleanup_confirmed=True if child is None else cleanup_owned(child,marker,args.evidence,spec["window_deadline_unix"])
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        write(args.evidence / "exit.json", {"job_id": spec["job_id"], "spec_sha256": args.spec_sha256, "reason": reason, "cleanup_confirmed":cleanup_confirmed, "exit_code": None if child is None else child.returncode, "wall_seconds": time.time() - started, "observed_unix": time.time(), "children_user_seconds": usage.ru_utime, "children_system_seconds": usage.ru_stime, "children_peak_rss_kib": usage.ru_maxrss})
        lock.close()
    if not cleanup_confirmed or reason != "exited" or child.returncode != 0:
        raise SystemExit(1)


def stop(args):
    running = json.loads((args.evidence / "running.json").read_text())
    if running["spec_sha256"] != args.spec_sha256 or running["instance_id"] not in INSTANCES:
        raise ValueError("stop does not identify this admitted task process")
    imds(running["instance_id"])
    if Path("/proc/sys/kernel/random/boot_id").read_text().strip() != running["boot_id"]:
        raise ValueError("process receipt belongs to another boot")
    try:
        ticks = start_ticks(running["pid"])
    except FileNotFoundError:
        ticks = None
    if ticks is not None and ticks != running["start_ticks"]:
        raise ValueError("PID has been reused; refuse to signal it")
    victims = owned_group(running["pgid"], running["marker"])
    write(args.evidence / "stop-request.json", {"job_id": running["job_id"], "spec_sha256": args.spec_sha256, "requested_unix": time.time(), "owned_processes": victims})
    terminate(running["pgid"], running["marker"])
    end=time.monotonic()+10
    while not (args.evidence / "exit.json").exists():
        if time.monotonic()>=end:
            raise RuntimeError("owned group stopped but wrapper exit receipt is not confirmed")
        time.sleep(0.1)
    receipt=json.loads((args.evidence / "exit.json").read_text())
    if receipt["spec_sha256"]!=args.spec_sha256:
        raise ValueError("exit receipt belongs to another job")
    print(json.dumps({"stop_requested": running["job_id"], "owned_processes": victims, "exit_receipt": receipt}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("run", "stop"))
    parser.add_argument("--spec", type=Path)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{64}", args.spec_sha256):
        parser.error("a reviewed spec SHA-256 is required")
    if args.mode == "run" and args.spec is None:
        parser.error("run requires --spec")
    def interrupted(signum,frame):
        raise InterruptedError("wrapper received signal " + str(signum))
    signal.signal(signal.SIGTERM,interrupted)
    signal.signal(signal.SIGINT,interrupted)
    signal.signal(signal.SIGHUP,interrupted)
    run(args) if args.mode == "run" else stop(args)


if __name__ == "__main__":
    main()
