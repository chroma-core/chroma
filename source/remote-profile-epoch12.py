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
BASE = "/home/ubuntu/spann-two-machine-v1/epoch12"
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


def selected_spec(spec,digest,now):
    kind=spec.get("kind");args=spec.get("args",{})
    arm="100k-reference" if kind=="reference" else "100k-distributed"
    executable="reference.bin" if kind=="reference" else "two_machine_spann"
    output_flag="--output-dir" if kind=="reference" else "--output"
    expected_output=BASE+"/"+arm+"-owner1"
    if (kind not in ("reference","performance") or spec.get("mode")!="run"
        or spec.get("job_id")!=arm+"-owner1" or spec.get("instance_id")!="i-011ff180f03584f40"
        or not spec.get("window_id","").startswith("epoch12-")
        or spec.get("binary")!=BASE+"/build/"+executable
        or args.get("--rows")!=100000 or args.get("--workers")!=32
        or args.get(output_flag)!=expected_output or spec.get("profile_gate")!=expected_output+"-profile-gate"
        or spec.get("workload_timeout_seconds")!=600
        or not now<spec["deadline_unix"]<=spec["window_deadline_unix"]-10
        or spec.get("provenance",{}).get("input_sha256")!="5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c"
        or spec.get("provenance",{}).get("input_bytes")!=307200000
        or len(digest)!=64):raise ValueError("collector requires one exact instrumented100K reference/distributed spec")
    input_flag="--input" if kind=="reference" else "--input-descriptor"
    input_path="fineweb100k.f32" if kind=="reference" else "100k.json"
    if args.get(input_flag)!="/home/ubuntu/spann-two-machine-v1/epoch2/input/"+input_path:
        raise ValueError("collector input binding changed")


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
    required = ("--pid","--freq","--event","--clockid","--call-graph","--output","--no-inherit","--no-buildid-cache","--control","--delay")
    if any(option not in help_text for option in required):
        raise ValueError("installed perf help does not establish required attachment/thread options")
    paranoid=Path("/proc/sys/kernel/perf_event_paranoid").read_text().strip()
    if os.geteuid()!=0:
        raise PermissionError("epoch12 requires the explicitly reviewed noninteractive sudo collector; no permissions change is allowed")
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


def recording_command(pid,output,control,ack,hard_bound_seconds=120):
    if not isinstance(hard_bound_seconds,int) or not 2<=hard_bound_seconds<=120:
        raise ValueError("recording hard bound must be2..120seconds")
    return [TIMEOUT,"--signal=INT","--kill-after=1",str(hard_bound_seconds-1),PERF,"record","--freq","99",
            "--event","cpu-clock:u","--clockid","CLOCK_REALTIME","--call-graph","fp",
            "--pid",str(pid),"--no-buildid-cache","--delay=-1",
            "--control=fifo:"+str(control)+","+str(ack),"--output",str(output)]


def gate_binding(value,pid):
    if (value.get("format_version")!=1 or value.get("phase_id")!="balance-0"
        or value.get("rows")!=100000 or value.get("pid")!=pid):
        raise ValueError("profile gate identity changed")
    return value


def control_ack(control_fd,ack_fd,command,child,deadline):
    os.write(control_fd,(command+"\n").encode())
    reply=b""
    while time.monotonic()<deadline:
        if child.poll() is not None:raise RuntimeError("profiler exited before control acknowledgment")
        try:reply+=os.read(ack_fd,4096)
        except BlockingIOError:pass
        if b"\n" in reply:
            if reply!=b"ack\n":raise ValueError("unexpected perf control acknowledgment")
            return {"command":command,"ack":reply.decode(),"observed":stamp()}
        time.sleep(.01)
    raise TimeoutError("perf control acknowledgment exceeded bounded wait")


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
    launched=time.monotonic();spec_bytes=args.spec.read_bytes()
    if hashlib.sha256(spec_bytes).hexdigest()!=args.spec_sha256:raise ValueError("spec hash mismatch")
    spec=json.loads(spec_bytes);selected_spec(spec,args.spec_sha256,time.time())
    if sha(Path(__file__))!=spec.get("provenance",{}).get("collector_script_sha256"):
        raise ValueError("collector source differs from spec")
    arm=spec["provenance"]["arm"]
    if args.evidence!=Path(BASE+"/"+spec["job_id"]+"-process") or args.output!=Path(BASE+"/"+arm+"-collector"):
        raise ValueError("collector paths must bind the exact fresh arm")
    args.output.mkdir(exist_ok=False,mode=0o755);os.chmod(args.output,0o755)
    deadline=min(launched+235,launched+spec["deadline_unix"]-time.time()-3,launched+spec["window_deadline_unix"]-time.time()-3)
    result={"started":stamp(),"spec_sha256":args.spec_sha256,"record_hard_bound_seconds":120,
            "overall_bound_seconds":240,"max_profile_bytes":MAX_BYTES,"status":"failed","full_timed_balance_covered":False}
    child=None;control_fd=None;ack_fd=None
    try:
        result["preflight"]=preflight()
        if sha(spec["binary"])!=spec["binary_sha256"]:raise ValueError("binary hash changed")
        write(args.output/"ready.json",{"ready":True,"observed":stamp(),"spec_sha256":args.spec_sha256})
        running=args.evidence/"running.json";gate=Path(spec["profile_gate"]);receipt=None
        while time.monotonic()<deadline-20:
            if (args.evidence/"exit.json").exists():raise ValueError("workload ended before gate")
            if running.exists():
                receipt=json.loads(running.read_text());pid=verify_process(receipt,spec,args.spec_sha256)
                deadline=min(deadline,time.monotonic()+receipt["deadline_unix"]-time.time()-3)
                if (gate/"done.json").exists():raise ValueError("balance already ended without acknowledged profiling")
                if (gate/"ready.json").exists():
                    result["gate_ready"]=gate_binding(json.loads((gate/"ready.json").read_text()),pid);break
            time.sleep(.02)
        else:raise TimeoutError("collector reached fixed overall wait deadline")
        pid=verify_process(receipt,spec,args.spec_sha256)
        result["process"]={key:receipt[key] for key in ("pid","pgid","start_ticks","boot_id","marker")}
        control=args.output/"perf-control.fifo";ack=args.output/"perf-ack.fifo"
        os.mkfifo(control,0o600);os.mkfifo(ack,0o600)
        control_fd=os.open(control,os.O_RDWR|os.O_NONBLOCK);ack_fd=os.open(ack,os.O_RDWR|os.O_NONBLOCK)
        record_bound=min(120,int(deadline-time.monotonic()-12))
        command=recording_command(pid,args.output/"perf.data",control,ack,record_bound);result["argv"]=command
        result["actual_record_hard_bound_seconds"]=record_bound
        result["recording_started"]=stamp()
        with (args.output/"perf.stderr").open("xb") as error:
            os.fchmod(error.fileno(),0o644)
            child=subprocess.Popen(command,stdout=subprocess.DEVNULL,stderr=error,start_new_session=True,
                preexec_fn=file_cap,env=dict(os.environ,SPANN_PROFILE_GROUP=args.spec_sha256))
            child.profile_marker=args.spec_sha256
            result["enabled"]=control_ack(control_fd,ack_fd,"enable",child,min(deadline-15,time.monotonic()+10))
            verify_process(receipt,spec,args.spec_sha256)
            binding={"format_version":1,"phase_id":"balance-0","rows":100000,"pid":pid}
            write(gate/"go.json",dict(binding,observed=stamp(),perf_enable_ack=result["enabled"]))
            result["go_written"]=stamp();record_deadline=min(deadline-15,result["recording_started"]["monotonic"]+record_bound-2)
            while time.monotonic()<record_deadline:
                if (gate/"done.json").exists():
                    result["gate_done"]=gate_binding(json.loads((gate/"done.json").read_text()),pid)
                    if result["gate_done"].get("status")!="completed":raise RuntimeError("timed balance failed")
                    break
                if child.poll() is not None:raise RuntimeError("profiler exited before timed balance completed; coverage incomplete")
                verify_process(receipt,spec,args.spec_sha256)
                if (args.output/"perf.data").exists() and (args.output/"perf.data").stat().st_size>=MAX_BYTES:
                    raise RuntimeError("profile byte cap reached before done; coverage incomplete")
                time.sleep(.02)
            else:raise TimeoutError("full balance did not fit the fixed recording bound; coverage incomplete")
            result["disabled"]=control_ack(control_fd,ack_fd,"disable",child,min(deadline-12,time.monotonic()+5))
            stop_collector(child)
        result["recording_stopped"]=stamp();result["record_exit_code"]=child.returncode;result["collector_cleanup_confirmed"]=True
        profile=args.output/"perf.data"
        if child.returncode not in (0,124,130,-signal.SIGINT) or not profile.exists() or profile.stat().st_size==0:
            raise RuntimeError("recording failed or empty")
        result["profile_bytes"]=profile.stat().st_size
        if result["profile_bytes"]>MAX_BYTES:raise RuntimeError("profile exceeds byte cap")
        report_command=[PERF,"report","--stdio","--no-children","--percent-limit","0.1","--call-graph","graph,0.5,caller","--input",str(profile)]
        with (args.output/"perf-report.txt").open("xb") as report,(args.output/"report.stderr").open("xb") as error:
            os.fchmod(report.fileno(),0o644);os.fchmod(error.fileno(),0o644)
            subprocess.run(report_command,stdout=report,stderr=error,timeout=max(.1,min(10,deadline-time.monotonic())),check=True,
                preexec_fn=lambda:resource.setrlimit(resource.RLIMIT_FSIZE,(1024**2,1024**2)))
        result["full_timed_balance_covered"]=True;result["status"]="completed"
        result["coverage"]="Acknowledged enable precedes go; done follows the balance timer; acknowledged disable follows done. Samples include small pre/post timer baseline and do not measure blocked time."
        write(gate/"released.json",dict(binding,status="completed",observed=stamp()))
    except BaseException as error:
        result["error"]=str(error);raise
    finally:
        try:stop_collector(child)
        finally:
            for descriptor in (control_fd,ack_fd):
                if descriptor is not None:os.close(descriptor)
            result["finished"]=stamp();result["elapsed_seconds"]=time.monotonic()-launched
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
