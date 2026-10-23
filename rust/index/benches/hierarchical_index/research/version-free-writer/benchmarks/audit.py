import json,os,subprocess,time
from pathlib import Path
import pyarrow.parquet as pq
r=Path('/mnt/msmarco/version-free-writer-7927');a=r/'artifacts'
for ds in ['wikipedia','msmarco']:
 meta=json.loads((a/f'{ds}-gt-provenance.json').read_text());d=pq.read_table(a/f'{ds}-gt.parquet').to_pydict()
 samples=list(zip(meta['query_ids'],d['query_vector'][:100]));f=a/f'{ds}-audit-samples.json';f.write_text(json.dumps(samples))
 for result in sorted(a.glob(f'{ds}-*.result.json')):
  data=json.loads(result.read_text());assert data['returncode']==0;variant=data['variant'];name=result.name.removesuffix('.result.json');args=json.loads((a/f'{name}.command.json').read_text());args[0]=str(a/f'{variant}-audit-binary');args.remove('--validate-postings');args.remove('--save');args+=['--resume','--no-commit']
  env=dict(os.environ,HOME=str(r/name/'home'),HF_HOME='/mnt/data/huggingface',HF_HUB_OFFLINE='1',VF_BENCHMARK_AUDIT_SAMPLES=str(f),RAYON_NUM_THREADS=str(data['threads']),TOKIO_WORKER_THREADS=str(data['threads']))
  (a/f'{name}.audit-command.json').write_text(json.dumps(args,indent=2))
  with (a/f'{name}.audit.log').open('w') as log:rc=subprocess.run(args,cwd=r/name,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
  (a/f'{name}.audit-result.json').write_text(json.dumps({'returncode':rc,'finished_unix':time.time()}));assert rc==0,name
