"""Summarize direct job request attribution; inclusive request time is not a critical path."""
from pathlib import Path
import json,sys
p=Path(__file__).resolve().parent;name=sys.argv[1] if len(sys.argv)>1 else '34-diagnostic';folder=p/name
r=json.loads((folder/'owner1-receipts.json').read_text());result=json.loads(next(v for k,v in r.items() if k.endswith('/result.json')));assert result['status']=='writer_validated';c=result['cycles'][-1]
events=[json.loads(line) for k,v in r.items() if k.endswith('/events.jsonl') for line in v.splitlines()];end=next(e['realtime_ns'] for e in events if e['event']=='balance_complete')
def durations(d):return {k.replace('_nanos','_seconds'):v/1e9 if k.endswith('_nanos') else v for k,v in d.items()}
def job(j):
 return {'kind':j['kind'],'node_id':j['node_id'],'duration_seconds':j['wall_nanos']/1e9,'seconds_before_balance_complete':(end-j['started_unix_nanos']-j['wall_nanos'])/1e9,'requests':{k:durations(v) for k,v in j['work'].get('remote_requests',{}).items()},'group_counters':{k:v for k,v in j['work']['counters'].items() if k.startswith('Expansion')},'scope_seconds':{k:v['wall_nanos']/1e9 for k,v in j['work']['inclusive_scopes'].items() if v['wall_nanos']>1e6}}
out={'name':name,'balance_seconds':c['balance_seconds'],'insertion_seconds':c['add_seconds'],'owners':{},'phase_requests':{}}
for a in c['before_placement']:
 out['owners'][str(a['owner'])]={'longest':[job(j) for j in a['work']['longest_jobs'][:5]],'last_substantive':[job(j) for j in sorted(a['work']['last_finished_jobs'],key=lambda j:j['started_unix_nanos']+j['wall_nanos'],reverse=True) if j['wall_nanos']>50000000][:8]}
for phase,before,after in [('insert',c['before_add'],c['after_add']),('balance',c['before_balance'],c['before_placement'])]:
 totals={};counts={}
 for a in after:
  b=next(x for x in before if x['owner']==a['owner'])
  for k,v in a['work']['counters'].items():
   if k.startswith('Expansion'):counts[k]=counts.get(k,0)+v-b['work']['counters'].get(k,0)
 rpc=c['rpc_phases']['add' if phase=='insert' else 'balance']
 a=rpc['after']['coordinator_client']['diagnostics'].get('remote_requests',{})
 b=rpc['before']['coordinator_client']['diagnostics'].get('remote_requests',{})
 for k,v in a.items():
  totals[k]={f:n-b.get(k,{}).get(f,0) for f,n in v.items() if not f.startswith('max_')}
 out['phase_requests'][phase]={'endpoint':'coordinator client only; peer endpoint not added','requests':{k:durations(v) for k,v in totals.items()},'group_counters':counts}
(p/(name+'-summary.json')).write_text(json.dumps(out,indent=2)+'\n')
for phase,v in out['phase_requests'].items():
 print(phase,v['group_counters'])
 for k,t in sorted(v['requests'].items(),key=lambda x:x[1]['client_elapsed_seconds'],reverse=True)[:6]:print(k,t)
for owner,v in out['owners'].items():
 print('OWNER',owner)
 for j in v['last_substantive'][:3]:print(j)
