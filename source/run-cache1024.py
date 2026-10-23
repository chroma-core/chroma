import json,pathlib,hashlib,subprocess,time,os,shutil
import numpy as np
base=pathlib.Path('/home/ubuntu/owner3m');r=base/'cache1024';r.mkdir();deadline=1792281037
for name in ['smoke-1-exit.json','smoke-4-exit.json','smoke-1','smoke-4','smoke-1.json','smoke-4.json','input-binding.json','wikipedia3m.f32','queries.json']:(r/name).symlink_to(base/name)
assert subprocess.check_output(['systemctl','is-active','owner3m-20261017-stop.timer'],text=True).strip()=='active'
for n in [1,4]:
 assert json.loads((r/f'smoke-{n}-exit.json').read_text())['exit_code']==0
 x=json.loads((r/f'smoke-{n}/result.json').read_text());assert x['census']['current_rows']==16384 and x['census']['vector_bits_verified']
 if n==4: assert sum(v['current_rows']>0 for v in x['after_save']['owners'])==4
bins=[p for p in pathlib.Path('/home/ubuntu/reference3m/source/target/release/deps').glob('hierarchical_spann_shard-*') if p.is_file() and os.access(p,os.X_OK)];assert len(bins)==1;binary=bins[0];binarysha=hashlib.sha256(binary.read_bytes()).hexdigest()
def run(name,args,seconds):
 assert time.time()+seconds+90<deadline
 assert shutil.disk_usage(r).free>8*1024**3
 start=time.monotonic()
 with (r/f'{name}.log').open('x') as log:
  rc=subprocess.run(['/usr/bin/time','-v','-o',str(r/f'{name}.time'),'timeout','--signal=TERM','--kill-after=15',str(seconds),str(binary)]+args,stdout=log,stderr=subprocess.STDOUT).returncode
 (r/f'{name}-exit.json').write_text(json.dumps(dict(exit_code=rc,elapsed_seconds=time.monotonic()-start,binary_sha256=binarysha,end_unix=time.time()))+'\n')
 return rc
# Query API correctness gates passed on v2; this build changes only provider capacity.
for n in [1,4]: assert json.loads((base/f'smoke-query-{n}-exit.json').read_text())['exit_code']==0
binding=json.loads((r/'input-binding.json').read_text());assert binding['rows']==3000000 and binding['sha256']=='dbbff5086b32f0be34c21870d6edb42478f13ee46d9d42aec189b07c4a9cb022'
for n in [1,4]:
 assert shutil.disk_usage(r).free>20*1024**3
 s=json.loads((r/f'smoke-{n}.json').read_text());s.update(rows=3000000,balance_every=1000000,source_id='wikipedia-en-pinned-ade45fb5-3m',vectors=dict(path=str(r/'wikipedia3m.f32'),bytes=binding['bytes'],sha256=binding['sha256']),prefix_sha256=binding['sha256'],output=str(r/f'owner-{n}'))
 p=r/f'owner-{n}-spec.json';p.write_text(json.dumps(s,indent=2)+'\n');print(f'Starting3Mowner{n}',flush=True);rc=run(f'owner-{n}',['reference-owner-diagnostic','--spec',str(p)],1200)
 if rc==124:
  print(f'Owner{n}timedout; preservingpartial evidence and continuingrequestednextarm',flush=True);continue
 if rc:raise SystemExit(rc)
 result=json.loads((r/f'owner-{n}/result.json').read_text());assert result['status']=='passed' and result['census']['current_rows']==3000000
 spec=dict(snapshot=str(r/f'owner-{n}/snapshot64.json'),store=str(r/f'owner-{n}/store'),queries=str(r/'queries.json'),output=str(r/f'owner-query-{n}.json'));p=r/f'owner-query-{n}-spec.json';p.write_text(json.dumps(spec));rc=run(f'owner-query-{n}',['owner-query-benchmark','--spec',str(p)],600)
 if rc:raise SystemExit(rc)
print('Bothrequestedarms reachedterminalstate; inspectreceiptsforcompletionortimeout',flush=True)
