import json,os,subprocess,fcntl,time
from pathlib import Path
r=Path('/mnt/msmarco/version-free-followup-20261020');a=r/'artifacts';lock=open('/mnt/data/chroma-benchmark.lock','w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
source=r/'wikipedia-diagnostic-9m-1/snapshots/8000000';meta=(source/'checkpoint.json').read_bytes();assert json.loads(meta)['total_vectors']==8000000
args=json.loads((a/'wikipedia-instrumented-3m-1.command.json').read_text());args[0]=str(a/'instrumented-audit-binary');args[args.index('--save-dir')+1]=str(source);args.remove('--save');args+=['--resume','--no-commit']
env=dict(os.environ,HOME=str(r/'wikipedia-instrumented-3m-1/home'),HF_HOME='/mnt/data/huggingface',HF_HUB_OFFLINE='1',VF_BENCHMARK_AUDIT_SAMPLES=str(a/'wikipedia-audit-samples.json'),RAYON_NUM_THREADS='32',TOKIO_WORKER_THREADS='32',TMPDIR=str(r/'tmp'))
(a/'retained-8m.audit-command.json').write_text(json.dumps(args,indent=2))
with (a/'retained-8m.audit.log').open('w') as log:rc=subprocess.run(args,cwd=r,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
assert (source/'checkpoint.json').read_bytes()==meta
(a/'retained-8m.audit-result.json').write_text(json.dumps({'returncode':rc,'finished_unix':time.time(),'metadata_unchanged':True}));assert rc==0
