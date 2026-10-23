import json
from pathlib import Path
P=Path(__file__).resolve().parent
for folder in P.iterdir():
 if not folder.is_dir() or not (folder/'summary.json').exists():continue
 receipts={}
 for f in folder.glob('owner*-receipts.json'):receipts.update(json.loads(f.read_text()))
 events=[json.loads(x) for k,v in receipts.items() if k.endswith('owner1/events.jsonl') for x in v.splitlines()]
 starts=[e for e in events if e.get('event')=='balance_started'];ends=[e for e in events if e.get('event')=='balance_complete']
 if not starts or not ends:continue
 begin,finish=starts[-1]['realtime_ns'],ends[-1]['realtime_ns'];out={}
 for owner in [1,2]:
  samples=[json.loads(x) for k,v in receipts.items() if k.endswith('owner'+str(owner)+'/owner-seconds.jsonl') for x in v.splitlines()]
  selected=[s for s in samples if begin<=s['realtime_ns']<=finish]
  cpu=[]
  for a,b in zip(samples,samples[1:]):
   if begin<=a['realtime_ns'] and b['realtime_ns']<=finish:
    cpu.append((b['counts']['process_cpu_seconds']-a['counts']['process_cpu_seconds'])/((b['realtime_ns']-a['realtime_ns'])/1e9))
  final=next(x for x in ends[-1]['owners'] if x['owner']==owner)
  completed=[s for s in samples if s['realtime_ns']>=begin and s['counts']['jobs_completed']>=final['jobs_completed']]
  out[str(owner)]={'sample_count_inside_balance':len(selected),'mean_process_cpu_cores_over_complete_intervals':sum(cpu)/len(cpu) if cpu else None,'maximum_sampled_process_cpu_cores':max(cpu,default=None),'maximum_active_admitted_jobs':max((s['counts']['active_workers'] for s in selected),default=None),'maximum_queue_ready':max((s['counts']['scheduler'].get('ready',0) for s in selected),default=None),'maximum_queue_blocked':max((s['counts']['scheduler'].get('blocked',0) for s in selected),default=None),'first_sample_reaching_final_completed_count_seconds_after_balance_start':(completed[0]['realtime_ns']-begin)/1e9 if completed else None,'maximum_observed_sampling_interval_seconds':max(((b['realtime_ns']-a['realtime_ns'])/1e9 for a,b in zip(samples,samples[1:])),default=None)}
 (folder/'telemetry-summary.json').write_text(json.dumps(out,indent=2))
 if folder.name=='baseline-detailed':print(json.dumps(out,indent=2))
