"""Report recent job completions and endpoint transport evidence without adding overlaps."""
from pathlib import Path
import json
p=Path(__file__).resolve().parent;out={}
for name in ['33-transport-smoke','33-navigation-detailed']:
 folder=p/name
 if not (folder/'owner1-receipts.json').exists():continue
 receipts=json.loads((folder/'owner1-receipts.json').read_text());r=json.loads(next(v for k,v in receipts.items() if k.endswith('/result.json')));c=r['cycles'][-1]
 events=[json.loads(line) for k,v in receipts.items() if k.endswith('/events.jsonl') for line in v.splitlines()]
 end=next(x for x in events if x['event']=='balance_complete')['realtime_ns'];start=next(x for x in events if x['event']=='balance_started')['realtime_ns']
 owners={}
 for x in c['before_placement']:
  jobs=[]
  for j in x['work']['last_finished_jobs']:
   finish=j['started_unix_nanos']+j['wall_nanos']
   if finish<start:continue
   jobs.append({'kind':j['kind'],'node_id':j['node_id'],'wall_s':j['wall_nanos']/1e9,'cpu_s':j['thread_cpu_nanos']/1e9 if j['thread_cpu_nanos'] is not None else None,'seconds_before_balance_complete':(end-finish)/1e9,'inclusive_scope_wall_s':{k:v['wall_nanos']/1e9 for k,v in j['work']['inclusive_scopes'].items()}})
  owners[str(x['owner'])]={'last_finished_jobs':jobs,'longest_job_seconds':max((j['wall_nanos']/1e9 for j in x['work']['longest_jobs']),default=0)}
 peer=json.loads((folder/'owner2-receipts.json').read_text());peer_events=[json.loads(line) for k,v in peer.items() if k.endswith('/events.jsonl') for line in v.splitlines()];samples=[x for x in peer_events if x['event']=='peer_transport_sample']
 out[name]={'phase_rpc':c['rpc_phases']['balance'],'owners':owners,'peer_transport_sample_count':len(samples),'last_peer_transport_sample':samples[-1] if samples else None,'clock_note':'Cross-owner completion estimates use wall start plus monotonic duration; inter-host clock skew is not independently bounded. Recent32 records do not reconstruct every dependency.'}
(p/'diagnostics-summary.json').write_text(json.dumps(out,indent=2)+'\n')
for name,x in out.items():
 print(name,'peer_samples',x['peer_transport_sample_count'])
 for owner,v in x['owners'].items():
  print('owner',owner,'latest',v['last_finished_jobs'][:2])
