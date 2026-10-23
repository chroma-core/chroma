import json,os,subprocess,time,sys
from pathlib import Path
import pyarrow.parquet as pq
r=Path('/mnt/msmarco/version-free-followup-20261020');a=r/'artifacts'; variant=sys.argv[1]; assert variant in ['instrumented','optimized','baseline','diagnostic','filtered','replay']
meta=json.loads((a/'gt-verification.json').read_text());d=pq.read_table(a/'wikipedia-gt.parquet').to_pydict()
samples=list(zip(meta['query_ids'],d['query_vector'][:100]));f=a/'wikipedia-audit-samples.json';f.write_text(json.dumps(samples))
for result in sorted(a.glob(f'wikipedia-{variant}-*.result.json')):
 data=json.loads(result.read_text());assert data['returncode']==0;name=result.name.removesuffix('.result.json');done=a/f'{name}.audit-result.json'
 if done.exists() and json.loads(done.read_text())['returncode']==0:continue
 args=json.loads((a/f'{name}.command.json').read_text());args[0]=str(a/f'{variant}-audit-binary');args.remove('--save');args+=['--no-commit']
 if '--resume' not in args:args.append('--resume')
 if '--checkpoint-snapshots-dir' in args:
  i=args.index('--checkpoint-snapshots-dir');del args[i:i+2]
 env=dict(os.environ,HOME=str(r/name/'home'),HF_HOME='/mnt/data/huggingface',HF_HUB_OFFLINE='1',VF_BENCHMARK_AUDIT_SAMPLES=str(f),RAYON_NUM_THREADS='32',TOKIO_WORKER_THREADS='32',TMPDIR=str(r/'tmp'))
 (a/f'{name}.audit-command.json').write_text(json.dumps(args,indent=2))
 with (a/f'{name}.audit.log').open('w') as log:rc=subprocess.run(args,cwd=r/name,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
 (a/f'{name}.audit-result.json').write_text(json.dumps({'returncode':rc,'finished_unix':time.time()}));assert rc==0,name
