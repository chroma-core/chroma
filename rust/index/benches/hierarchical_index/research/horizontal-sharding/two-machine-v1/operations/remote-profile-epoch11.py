#!/usr/bin/env python3
"""Collect one bounded owner1 userspace CPU sample; never start a workload."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import time

PERF = "/usr/bin/perf"
TIMEOUT = "/usr/bin/timeout"
BASE = "/home/ubuntu/spann-two-machine-v1/epoch11"
BINARY = "/home/ubuntu/spann-two-machine-v1/epoch9/build/two_machine_spann"
BINARY_SHA = "57f6baa4b09749dfede42247a3bf728748151c2df1a3b9ae5754877541f27de1"
MAX_BYTES = 256 * 1024**2


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        for block in iter(lambda: file.read(1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def write(path, value):
    if path.exists():raise ValueError("preserve existing collector receipt")
    temporary=path.with_name(path.name+".tmp")
    with temporary.open("x") as file:
        json.dump(value, file, indent=2)
        file.write("\n")
        file.flush();os.fchmod(file.fileno(),0o644);os.fsync(file.fileno())
    os.replace(temporary,path)


def stamp():
    return {"realtime": time.time(), "monotonic": time.monotonic()}


def selected_spec(spec, digest, now):
    args = spec.get("args", {})
    expected = {"--mode":"run", "--owner":1, "--rows":100000, "--workers":32,
                "--initial-batch":100000, "--owner-budget":80000, "--first-owner-budget":0,
                "--transition":1000000,"--later-batch":1000000,
                "--split-threshold":2048,"--merge-threshold":512,"--branching-factor":100,
                "--validation-queries":16,"--vector-cache-bytes":12884901888,
                "--input-descriptor":"/home/ubuntu/spann-two-machine-v1/epoch2/input/100k.json",
                "--output":BASE+"/100k-profile-owner1"}
    if (spec.get("kind") != "performance" or spec.get("mode") != "run"
        or spec.get("job_id") != "100k-profile-owner1"
        or spec.get("instance_id") != "i-011ff180f03584f40"
        or not spec.get("window_id", "").startswith("epoch11-")
        or spec.get("binary") != BINARY or spec.get("binary_sha256") != BINARY_SHA
        or spec.get("workload_timeout_seconds") != 600
        or spec.get("provenance",{}).get("input_sha256") != "5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c"
        or spec.get("provenance",{}).get("input_bytes") != 307200000
        or not now < spec["deadline_unix"] <= spec["window_deadline_unix"] - 10
        or any(args.get(key) != value for key,value in expected.items())
        or not isinstance(digest,str) or len(digest)!=64):
        raise ValueError("collector accepts only the exact corrected100K owner1 specification")


def start_ticks(pid):
    return int(Path(f"/proc/{pid}/stat").read_text().rsplit(")",1)[1].split()[19])


def verify_process(receipt, spec, digest):
    pid = receipt["pid"]
    if (not isinstance(pid,int) or isinstance(pid,bool) or pid <= 1
        or receipt.get("spec_sha256") != digest
        or receipt.get("job_id") != spec["job_id"]
        or receipt.get("instance_id") != spec["instance_id"]
        or receipt.get("deadline_unix",spec["deadline_unix"]) > spec["deadline_unix"]
        or receipt.get("boot_id") != Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        or start_ticks(pid) != receipt.get("start_ticks")
        or os.getpgid(pid) != receipt.get("pgid")
        or str(Path(f"/proc/{pid}/exe").resolve(strict=True)) != spec["binary"]):
        raise ValueError("owned workload process binding changed")
    marker = spec["job_id"]+":"+digest
    if receipt.get("marker") != marker or ("SPANN_OWNED_JOB="+marker).encode() not in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0"):
        raise ValueError("owned workload marker is absent")
    return pid


def preflight():
    for tool in (PERF,TIMEOUT):
        if not Path(tool).is_file() or not os.access(tool,os.X_OK):
            raise ValueError("required installed profiler/timeout tool is unavailable")
    version = subprocess.run([PERF,"--version"],capture_output=True,text=True,timeout=3,check=True)
    help_result = subprocess.run([PERF,"record","-h"],capture_output=True,text=True,timeout=3)
    help_text = help_result.stdout+help_result.stderr
    # perf's existing --pid option selects the process threads; inheritance is
    # enabled by default unless --no-inherit is selected. Never select that flag.
    required = ("--pid","--freq","--event","--clockid","--call-graph","--output","--no-inherit","--no-buildid-cache")
    if any(option not in help_text for option in required):
        raise ValueError("installed perf help does not establish required attachment/thread options")
    paranoid=Path("/proc/sys/kernel/perf_event_paranoid").read_text().strip()
    if os.geteuid()!=0:
        raise PermissionError("epoch11 requires the explicitly reviewed noninteractive sudo collector; no permissions change is allowed")
    return {"version":version.stdout.strip(),"help_sha256":hashlib.sha256(help_text.encode()).hexdigest(),
            "help_exit_code":help_result.returncode,"required_options":list(required),
            "perf_event_paranoid":paranoid,
            "permissions_changed":False,"effective_uid":os.geteuid(),"scope":"one verified workload PID and its threads; no system-wide sampling"}


def observations(path):
    if not path.exists():return []
    lines=path.read_text().splitlines(keepends=True)
    return [json.loads(line) for line in lines if line.endswith("\n") and line.strip()]


def phase(events, name):
    return next((event for event in events if event.get("event")==name and event.get("rows")==100000),None)


def recording_command(pid, output):
    # Normal SIGINT occurs at nine seconds; the one-second SIGKILL grace makes
    # even an unresponsive profiler stop inside the ten-second recording bound.
    return [TIMEOUT,"--signal=INT","--kill-after=1","9",PERF,"record","--freq","99",
            "--event","cpu-clock:u","--clockid","CLOCK_REALTIME","--call-graph","fp",
            "--pid",str(pid),"--no-buildid-cache","--output",str(output)]


def file_cap():
    resource.setrlimit(resource.RLIMIT_FSIZE,(MAX_BYTES,MAX_BYTES))


def collector_members(pgid, marker):
    found=[]
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():continue
        pid=int(entry.name)
        try:
            if os.getpgid(pid)!=pgid:continue
            state=(entry/"stat").read_text().rsplit(")",1)[1].split()[0]
            if state in ("Z","X"):continue
            if ("SPANN_PROFILE_GROUP="+marker).encode() not in (entry/"environ").read_bytes().split(b"\0"):
                raise ValueError("collector group contains an unowned process; refuse signal")
            found.append(pid)
        except (FileNotFoundError,ProcessLookupError):continue
    return found


def stop_collector(child):
    if child is None:return
    marker=child.profile_marker
    if collector_members(child.pid,marker):
        os.killpg(child.pid,signal.SIGINT)
        end=time.monotonic()+1
        while collector_members(child.pid,marker) and time.monotonic()<end:time.sleep(.02)
        if collector_members(child.pid,marker):os.killpg(child.pid,signal.SIGKILL)
    try:child.wait(timeout=1)
    except subprocess.TimeoutExpired:raise RuntimeError("collector group shutdown did not finish")
    if collector_members(child.pid,marker):raise RuntimeError("collector group survivors remain")


def collect(args):
    launched = time.monotonic()
    spec_bytes=args.spec.read_bytes()
    if hashlib.sha256(spec_bytes).hexdigest()!=args.spec_sha256:raise ValueError("spec hash mismatch")
    spec=json.loads(spec_bytes);selected_spec(spec,args.spec_sha256,time.time())
    if sha(Path(__file__)) != spec.get("provenance",{}).get("collector_script_sha256"):
        raise ValueError("collector source differs from the reviewed spec binding")
    expected_evidence=Path(BASE+"/100k-profile-owner1-process")
    if args.evidence != expected_evidence or args.output != Path(BASE+"/100k-profile-collector"):
        raise ValueError("collector paths must use fresh fixed epoch11 evidence/output")
    args.output.mkdir(exist_ok=False,mode=0o755)
    os.chmod(args.output,0o755)
    deadline=min(launched+115,launched+spec["deadline_unix"]-time.time()-3,
                 launched+spec["window_deadline_unix"]-time.time()-3)
    result={"started":stamp(),"spec_sha256":args.spec_sha256,"sample_bound_seconds":10,
            "normal_sample_seconds":9,"overall_bound_seconds":120,"max_profile_bytes":MAX_BYTES,
            "phase_boundaries":"externally observed flushed events; polling and snapshot overhead are not exact timed-phase boundaries",
            "status":"failed"}
    child=None
    try:
        result["preflight"]=preflight()
        if sha(spec["binary"]) != spec["binary_sha256"]:raise ValueError("workload binary hash changed")
        write(args.output/"ready.json",{"ready":True,"observed":stamp(),"spec_sha256":args.spec_sha256})
        running=args.evidence/"running.json"
        events_path=Path(spec["args"]["--output"])/"events.jsonl"
        receipt=None
        while time.monotonic()<deadline-15:
            if (args.evidence/"exit.json").exists():raise ValueError("workload ended before collection")
            if running.exists():
                receipt=json.loads(running.read_text());pid=verify_process(receipt,spec,args.spec_sha256)
                deadline=min(deadline,time.monotonic()+receipt["deadline_unix"]-time.time()-3)
                events=observations(events_path)
                if phase(events,"balance_complete"):raise ValueError("first balance already completed; do not retry")
                if phase(events,"balance_started"):
                    result["balance_started_observed"]=stamp();break
            time.sleep(.02)
        else:raise TimeoutError("collector wait reached its fixed overall deadline")
        pid=verify_process(receipt,spec,args.spec_sha256)
        result["process"]={key:receipt[key] for key in ("pid","pgid","start_ticks","boot_id","marker")}
        command=recording_command(pid,args.output/"perf.data");result["argv"]=command
        result["collection_started"]=stamp()
        with (args.output/"perf.stderr").open("xb") as error:
            os.fchmod(error.fileno(),0o644)
            child=subprocess.Popen(command,stdout=subprocess.DEVNULL,stderr=error,start_new_session=True,preexec_fn=file_cap,
                env=dict(os.environ,SPANN_PROFILE_GROUP=args.spec_sha256))
            child.profile_marker=args.spec_sha256
            result["stop_reason"]="profiler_exit_or_nine_second_timeout"
            while child.poll() is None:
                events=observations(events_path)
                if phase(events,"balance_complete"):
                    result["balance_complete_observed"]=stamp();result["stop_reason"]="balance_complete";break
                try:verify_process(receipt,spec,args.spec_sha256)
                except FileNotFoundError:result["stop_reason"]="workload_exit";break
                if time.monotonic()>=deadline-15:
                    result["stop_reason"]="overall_deadline";break
                if (args.output/"perf.data").exists() and (args.output/"perf.data").stat().st_size>=MAX_BYTES:
                    result["stop_reason"]="profile_byte_cap";break
                time.sleep(.02)
            stop_collector(child)
        result["collection_stopped"]=stamp();result["record_exit_code"]=child.returncode
        result["collector_cleanup_confirmed"]=True
        profile=args.output/"perf.data"
        if child.returncode not in (0,124,130,-signal.SIGINT) or not profile.exists() or profile.stat().st_size==0:
            raise ValueError("perf attachment failed or produced no recording; inspect perf.stderr")
        result["profile_bytes"]=profile.stat().st_size
        if result["profile_bytes"]>MAX_BYTES:raise ValueError("profile exceeded its byte cap")
        report_command=[PERF,"report","--stdio","--no-children","--percent-limit","0.1",
                        "--call-graph","graph,0.5,caller","--input",str(profile)]
        with (args.output/"perf-report.txt").open("xb") as report,(args.output/"report.stderr").open("xb") as error:
            os.fchmod(report.fileno(),0o644);os.fchmod(error.fileno(),0o644)
            completed=subprocess.run(report_command,stdout=report,stderr=error,
                timeout=max(.1,min(10,deadline-time.monotonic())),check=True,
                preexec_fn=lambda:resource.setrlimit(resource.RLIMIT_FSIZE,(1024**2,1024**2)))
        result["status"]="completed"
        result["coverage"]="Only the observed sample interval is attributed. Earlier/later balance work is uncovered; event observation can overlap phase boundaries. CPU samples do not measure blocked time."
    except BaseException as error:
        result["error"]=str(error)
        raise
    finally:
        try:stop_collector(child)
        finally:
            result["finished"]=stamp()
            result["elapsed_seconds"]=time.monotonic()-launched
            write(args.output/"result.json",result)


def interrupted(signum, frame):
    raise InterruptedError("collector received stop signal")


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec",required=True,type=Path)
    parser.add_argument("--spec-sha256",required=True)
    parser.add_argument("--evidence",required=True,type=Path)
    parser.add_argument("--output",required=True,type=Path)
    args=parser.parse_args()
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,interrupted)
    collect(args)


if __name__=="__main__":main()
