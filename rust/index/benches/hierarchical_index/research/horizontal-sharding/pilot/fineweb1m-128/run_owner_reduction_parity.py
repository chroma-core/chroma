"""Run one parity attempt with a coordinator sampler started before query work."""
import pathlib,json,subprocess,time,hashlib,signal
base=pathlib.Path('/mnt/spann');resources=base/'results/owner-reduction-gen1-query-parity-v2-resources';resources.mkdir(exist_ok=False)
command=[str(base/'pilot-venv/bin/python'),str(base/'check_owner_reduction_parity.py'),'--source',str(base/'owner-reduction-source-v1'),'--case',str(base/'results/owner-reduction-gen1-parity-v1'),'--gold',str(base/'owner-reduction-baseline-test-cohost.jsonl'),'--data',str(base/'fineweb1m-pilot-data'),'--output',str(base/'results/owner-reduction-gen1-query-parity-v2')]
(resources/'launch.json').write_text(json.dumps(dict(command=command,started_unix=time.time(),input_sha256={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [base/'fineweb1m-pilot-data/test.jsonl',base/'fineweb1m-pilot-data/test.exact.jsonl',base/'owner-reduction-baseline-test-cohost.jsonl',base/'check_owner_reduction_parity.py']}),indent=2));log=open(base/'owner-reduction-gen1-parity-v2.log','w');p=subprocess.Popen(command,stdout=log,stderr=log);(resources/'groups.json').write_text(json.dumps({'coordinator':[p.pid]}));sampler=subprocess.Popen(['python3',str(base/'sample_process_groups.py'),'--groups',str(resources/'groups.json'),'--output',str(resources/'processes.jsonl'),'--duration','600'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
try:status=p.wait(timeout=600)
finally:
 if p.poll() is None:p.terminate();p.wait(timeout=5)
 sampler.terminate();sampler.wait(timeout=5)
(resources/'outcome.json').write_text(json.dumps(dict(returncode=status,finished_unix=time.time(),pid=p.pid,sampler_pid=sampler.pid)));raise SystemExit(status)
