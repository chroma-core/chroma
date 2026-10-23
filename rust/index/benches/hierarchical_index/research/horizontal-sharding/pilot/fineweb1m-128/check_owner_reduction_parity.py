"""Compare an optional wire protocol against retained full logical search traces."""
import argparse,pathlib,json,sys,time,statistics,hashlib
p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--case',required=True);p.add_argument('--gold',required=True);p.add_argument('--data',required=True);p.add_argument('--output',required=True);a=p.parse_args();sys.path.insert(0,a.source)
from tools.hierarchical_sharding.subtree_benchmark import measure,rows
case=pathlib.Path(a.case);out=pathlib.Path(a.output);out.mkdir(exist_ok=False);metadata=json.loads((case/'case.json').read_text());data=pathlib.Path(a.data);gold={r['query_id']:r for r in rows(a.gold) if r.get('status')=='ok'};assert len(gold)==100
plan=dict(tree_sha256=metadata['tree_sha256'],beam_width=512,candidate_budget=1000,k=100,queries=100,baseline_gold_sha256=hashlib.sha256(pathlib.Path(a.gold).read_bytes()).hexdigest(),optional_source=a.source,scope='Identical immutable1Mtree andV4binary; compares logical search traces, while wire contributions/bytes may differ. This gate does not tune policy.')
(out/'plan.json').write_text(json.dumps(plan,indent=2));summary=measure(metadata['tree'],case/'placement-cohost.json',rows(data/'test.jsonl'),rows(data/'test.exact.jsonl'),512,1000,out/'queries.jsonl');assert summary['status']=='passed',summary
keys=('ids','distances','levels','reached_leaves','selected_candidates','global_candidate_count','candidate_contributions');compared=[]
for row in rows(out/'queries.jsonl'):
 if row.get('status')!='ok':raise ValueError('Incomplete optional parity outcome')
 previous=gold[row['query_id']];assert row['recall']==previous['recall']
 for key in keys:
  if row['result'][key]!=previous['result'][key]:raise ValueError('Logical parity differs for '+row['query_id']+' field '+key)
 r=row['result'];compared.append(dict(query_id=row['query_id'],logical_candidate_contributions=r['candidate_contributions'],wire_candidate_contributions=r['wire_candidate_contributions'],response_body_bytes=sum(t.get('response_bytes',0) for t in r['transport']),request_body_bytes=sum(t.get('request_bytes',0) for t in r['transport'])))
assert len(compared)==100
result=dict(status='passed',plan=plan,measurement=summary,full_logical_trace_matches=100,mean_wire_candidates=statistics.mean(r['wire_candidate_contributions'] for r in compared),mean_logical_candidates=statistics.mean(r['logical_candidate_contributions'] for r in compared),mean_response_body_bytes=statistics.mean(r['response_body_bytes'] for r in compared),counts=compared)
(out/'summary.json').write_text(json.dumps(result,indent=2));print(json.dumps({k:v for k,v in result.items() if k not in ('counts','plan')}),flush=True)
