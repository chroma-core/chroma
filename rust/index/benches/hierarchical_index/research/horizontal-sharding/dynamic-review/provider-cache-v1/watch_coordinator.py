"""Own the exact protocol test session until cleanup; never reap before group stop."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

root=Path(__file__).resolve().parent
output=root/"protocol-watchdog"
output.mkdir(exist_ok=False)
child=None
receipt={"status":"running","work_deadline_s":120,"reconcile_wait_s":5}
def terminate(signum,frame):
    raise SystemExit("watchdog termination")
signal.signal(signal.SIGTERM,terminate)
try:
    with (output/"stdout.log").open("wb") as stdout,(output/"stderr.log").open("wb") as stderr:
        child=subprocess.Popen([sys.executable,str(root/"run_coordinator.py")],stdout=stdout,stderr=stderr,start_new_session=True)
        # Keep the leader unreaped. Its PID anchors this newly owned session/group.
        receipt.update(pid=child.pid,pgid=child.pid)
        deadline=time.monotonic()+120
        while True:
            if time.monotonic()>deadline:raise TimeoutError("owned protocol group deadline")
            stat=subprocess.run(["ps","-o","stat=","-p",str(child.pid)],capture_output=True,text=True,timeout=2,check=True).stdout.strip()
            if stat.startswith("Z"):break
            if not stat:raise RuntimeError("unreaped leader identity unavailable")
            if sum(p.stat().st_size for p in output.glob("*.log"))>32*1024*1024:raise RuntimeError("protocol output threshold")
            time.sleep(.05)
        receipt["status"]="completed-pending-reconciliation"
except BaseException as error:
    receipt.update(status="failed",error=repr(error))
finally:
    if child is not None:
        # Neither poll nor wait runs before this kill: no unanchored PGID reuse.
        try:os.killpg(child.pid,signal.SIGKILL)
        except ProcessLookupError:pass
        try:receipt["exit_code"]=child.wait(timeout=5)
        except BaseException as error:receipt.update(status="unreconciled",reconcile_error=repr(error))
        absent=False
        for _ in range(20):
            try:os.killpg(child.pid,0)
            except ProcessLookupError:
                absent=True
                break
            time.sleep(.05)
        receipt["owned_group_absent"]=absent
        if not absent:receipt["status"]="unreconciled"
    if receipt["status"]=="completed-pending-reconciliation":receipt["status"]="passed" if receipt.get("exit_code")==0 else "failed"
    (output/"receipt.json").write_text(json.dumps(receipt,indent=2,sort_keys=True))
raise SystemExit(0 if receipt["status"]=="passed" else 1)
