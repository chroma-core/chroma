"""Test the saved calibration policy on two placements of exactly one immutable tree."""
import pathlib,json,sys,hashlib,time
source='/mnt/spann/owner-reduction-source-v2';sys.path.insert(0,source)
from tools.hierarchical_sharding.subtree_benchmark import measure,rows
base=pathlib.Path('/mnt/spann');chosen=dict(beam_width=512,candidate_budget=1000);case=base/'results/fineweb1m1-gen2-quality-v3';metadata=json.loads((case/'case.json').read_text());assert metadata['tree_sha256']=='04d57d52834fdd41b2065202775dcdd6769ed85e7721d8d8907174771951ea27'
out=base/'results/fineweb1m1-gen2-heldout-v1';out.mkdir(exist_ok=False);data=base/'fineweb1m1-quality-data';plan=dict(started_unix=time.time(),tree_sha256=metadata['tree_sha256'],policy_origin='Frozen1M calibration B512/M1000; no tuning on1.1M heldout',selected=chosen,read_epoch=json.loads((case/'read-epoch.json').read_text()),input_sha256={n:hashlib.sha256((data/n).read_bytes()).hexdigest() for n in ('test.jsonl','test.exact.jsonl')},cache='Both placements restart their application readers after calibration. Each receives one unmeasured first-query warmup; OS/page cache is warm and uncontrolled.');(out/'plan.json').write_text(json.dumps(plan,indent=2));queries=rows(data/'test.jsonl');exact=rows(data/'test.exact.jsonl');summaries=[]
try:
 for label in ('cohost','two-hosts'):
  r=measure(metadata['tree'],case/('placement-'+label+'.json'),queries,exact,512,1000,out/('test-'+label+'.jsonl'));summaries.append(dict(placement=label,**r));print(json.dumps(dict(phase='heldout',placement=label,**r)),flush=True)
  if r['status']!='passed':raise RuntimeError('Held-out query phase failed; retain its outcomes')
 first={r['query_id']:r for r in rows(out/'test-cohost.jsonl')};second={r['query_id']:r for r in rows(out/'test-two-hosts.jsonl')};assert set(first)==set(second) and len(first)==100
 fields=('ids','distances','levels','reached_leaves','selected_candidates','global_candidate_count','candidate_contributions')
 for key in first:
  assert first[key]['recall']==second[key]['recall']
  for field in fields:
   if first[key]['result'][field]!=second[key]['result'][field]:raise ValueError('Placement parity differs for '+key+' field '+field)
 result=dict(status='passed',tree_sha256=metadata['tree_sha256'],policy=chosen,tests=summaries,full_logical_trace_matches=100,scope='One selected immutable tree placed on one and two hosts. Same policy selected using calibration only;100 held-out queries support exploratory mean recall/latency, not p99 or broad capacity claims.');(out/'summary.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
except BaseException as error:
 (out/'failure.json').write_text(json.dumps(dict(status='failed',error=repr(error),completed_phases=summaries,finished_unix=time.time()),indent=2));raise
