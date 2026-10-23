import json,sys
from pathlib import Path

def summary(path):
 d=json.loads(Path(path).read_text()); c=d['cycles'][0]
 before={o['owner']:o for o in c['before_balance']}; after={o['owner']:o for o in c['before_placement']}
 counters={};scopes={}
 for owner,a in after.items():
  if owner != 1: continue
  b=before[owner]
  for k,v in a['work']['counters'].items():counters[k]=counters.get(k,0)+v-b['work']['counters'].get(k,0)
  for k,v in a['work']['inclusive_scopes'].items():
   prior=b['work']['inclusive_scopes'].get(k,{})
   dst=scopes.setdefault(k,{})
   for stat,value in v.items():
    if stat.startswith('max_'): continue
    dst[stat]=dst.get(stat,0)+value-prior.get(stat,0)
 kernel=c['clustering_local_process_delta']; cpu=c['coordinator_balance_cpu_seconds']
 return {'balance_seconds':c['balance_seconds'],'process_cpu_seconds':cpu,'kernel':kernel,'non_kernel_cpu_seconds':cpu-kernel['thread_cpu_nanos']/1e9,'counters':counters,'inclusive_scopes':scopes,'before_placement_shape':[{k:v for k,v in a.items() if k in ['owner','nodes','leaves','vectors','jobs_completed','splits','distances','vector_fetches']} for a in after.values()],'reservation_end':{str(o):a['reservation_state'] for o,a in after.items()},'active_peak':max(a['active_peak'] for a in after.values()),'final_memberships':{str(o['owner']):o['vectors'] for o in d['owners']},'routing_shape':d['routing_shape'],'validation_complete':d['validation_complete'],'settings':d['settings'],'rpc_phases':c['rpc_phases']}
if __name__=='__main__':
 s=summary(sys.argv[1]);print(json.dumps(s,indent=2))
