import json,os,subprocess,time,fcntl
from pathlib import Path
import pyarrow.parquet as pq
r=Path('/mnt/data/temporary-deletions-20261020');a=r/'artifacts'
lock=open('/mnt/data/chroma-benchmark.lock','w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
meta=json.loads((a/'gt-verification.json').read_text());q=pq.read_table(a/'wikipedia-gt.parquet').to_pydict()['query_vector'][:100];base=list(zip(meta['query_ids'],q))
cache=Path('/mnt/data/huggingface/hub/datasets--CohereLabs--wikipedia-2023-11-embed-multilingual-v3');snap=cache/'snapshots'/(cache/'refs/main').read_text().strip()
for millions in [int(v) for v in __import__('sys').argv[1:]]:
 samples=list(base)
 for shard in range((millions-1)*10,millions*10):
  t=pq.read_table(snap/'en'/f'{shard:04}.parquet',columns=['emb']);assert len(t)==100000
  for row in range(0,100000,10000):samples.append((shard*100000+row,t.column(0)[row].as_py()))
 f=a/f'wikipedia-audit-extended-{millions}m-samples.json';f.write_text(json.dumps(samples))
 for result in sorted(a.glob(f'wikipedia-*-{millions}m-*.result.json')):
  d=json.loads(result.read_text());assert d['returncode']==0;name=result.name.removesuffix('.result.json');variant=d['variant'];done=a/f'{name}.audit-extended-result.json'
  if done.exists():assert json.loads(done.read_text())['returncode']==0;continue
  args=json.loads((a/f'{name}.command.json').read_text());args[0]=str(a/'reference-audit-binary');args.remove('--save');args+=['--no-commit']
  if '--resume' not in args:args.append('--resume')
  env=dict(os.environ,HOME=str(r/name/'home'),HF_HOME='/mnt/data/huggingface',HF_HUB_OFFLINE='1',VF_BENCHMARK_AUDIT_SAMPLES=str(f),RAYON_NUM_THREADS='32',TOKIO_WORKER_THREADS='32',TMPDIR=str(r/'tmp'))
  (a/f'{name}.audit-extended-command.json').write_text(json.dumps(args,indent=2))
  with (a/f'{name}.audit-extended.log').open('x') as log:rc=subprocess.run(args,cwd=r/name,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
  done.write_text(json.dumps({'returncode':rc,'sample_count':len(samples),'newest_million_samples':100,'finished_unix':time.time()}));assert rc==0,name
  print(name,'extended audit pass',flush=True)
