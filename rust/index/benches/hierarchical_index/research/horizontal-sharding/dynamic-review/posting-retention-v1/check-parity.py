"""Bounded operation replay; no latency or RSS assertion."""
import hashlib,json,subprocess,threading
from pathlib import Path
HERE=Path(__file__).resolve().parent
spec=json.loads((HERE/'raw.json').read_text())
manifest=json.loads((HERE/'manifest.json').read_text())
binary=Path(manifest['binary_path']);assert hashlib.sha256(binary.read_bytes()).hexdigest()==manifest['binary_sha256']
requests=[r['request'] for r in spec['records']]
leaves=spec['leaves']
# Revisit after eviction, exercise exact rerank, then malformed-input cleanup.
requests += [dict(op='score_selected_leaves',query=[0,0,0,0],nodes=leaves,candidate_budget=16),dict(op='rerank_selected',query=[0,0,0,0],local_ids=list(range(8))),dict(op='score_selected_leaves',query=[0,0,0,0],nodes=leaves,candidate_budget=0),dict(op='score_selected_leaves',query=[0,0,0,0],nodes=leaves,candidate_budget=16)]
results={}
for mode,flags in [('retain',[]),('evict',['--evict-postings-after-operation'])]:
 with (HERE/(mode+'-stderr.log')).open('w') as err:
  p=subprocess.Popen([str(binary),'read','--checkpoint-dir',spec['checkpoint'],'--generation','1','--local-ids-only']+flags,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True)
  watchdog=threading.Timer(5,p.kill);watchdog.start()
  rows=[]
  try:
   for i,request in enumerate(requests):
    q=dict(request,profile_memory=True,request_id=str(i),generation=1)
    p.stdin.write(json.dumps(q)+'\n');p.stdin.flush();raw=p.stdout.readline();assert raw,'reader exited';r=json.loads(raw);rows.append(dict(request=q,response=r))
    if mode=='evict':assert r['retained_postings']['materialized_leaves']==0 and r['retained_postings']['payload_bytes']==0,r
  finally:
   p.stdin.close()
   try:p.wait(timeout=2)
   except subprocess.TimeoutExpired:p.kill();p.wait(timeout=2)
   watchdog.cancel()
  assert p.poll() is not None
  assert p.returncode==0
 results[mode]=rows
for a,b in zip(results['retain'],results['evict']):
 strip=lambda r:{k:v for k,v in r.items() if k not in ('elapsed_ms','memory_bytes','retained_postings')}
 assert strip(a['response'])==strip(b['response'])
assert 'error' in results['evict'][-2]['response']
assert results['retain'][-1]['response']['retained_postings']['materialized_leaves']==len(leaves)
(HERE/'parity-runtime.json').write_text(json.dumps(dict(status='passed',operations_per_mode=len(requests),scope='logical operation response parity; excludes instrumentation fields only',records=results),indent=2)+'\n')
print('PASS: logical responses identical; eviction arrays zero after every success/error operation; revisit and rerank succeed')
