"""Bound calibration runtime and capture its coordinator process resources."""
import pathlib,json,subprocess,time,hashlib,os,signal
base=pathlib.Path('/mnt/spann');out=base/'results/fineweb10m-calibration-driver-resources-v1';out.mkdir(exist_ok=False);command=[str(base/'pilot-venv/bin/python'),str(base/'run_calibration.py')];log=open(base/'fineweb10m-calibration-v1.log','w');started=time.time();p=subprocess.Popen(command,stdout=log,stderr=log,start_new_session=True);(out/'groups.json').write_text(json.dumps({'coordinator':[p.pid]}));s=subprocess.Popen(['python3',str(base/'sample_process_groups.py'),'--groups',str(out/'groups.json'),'--output',str(out/'processes.jsonl'),'--duration','2400'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
(out/'launch.json').write_text(json.dumps(dict(command=command,pid=p.pid,started_unix=started,whole_grid_timeout_s=2400,plan_sha256=hashlib.sha256((base/'read-grid-v1.json').read_bytes()).hexdigest(),source_sha256=hashlib.sha256((base/'run_calibration.py').read_bytes()).hexdigest()),indent=2));status=None
try:status=p.wait(timeout=2400)
except subprocess.TimeoutExpired:
 os.killpg(p.pid,signal.SIGTERM);p.wait(timeout=5);status=124
finally:
 if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=5)
 s.terminate();s.wait(timeout=5)
 (out/'outcome.json').write_text(json.dumps(dict(returncode=status,finished_unix=time.time())))
raise SystemExit(status)
