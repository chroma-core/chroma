"""Summarize completed runs; retain phase deltas and do not sum nested scopes."""
import json,statistics
from pathlib import Path
P=Path(__file__).resolve().parent

def receipts(path):
 values={}
 for f in path.glob('owner*-receipts.json'):values.update(json.loads(f.read_text()))
 return values

def delta(a,b):
 if isinstance(a,dict):return {k:delta(v,b.get(k,0 if isinstance(v,(int,float)) else {})) for k,v in a.items() if k not in ['max_wall_nanos','max_thread_cpu_nanos']}
 if isinstance(a,list) and isinstance(b,list):return [x-y for x,y in zip(a,b)]
 if isinstance(a,(int,float)) and isinstance(b,(int,float)):return a-b
 return a

def main():
 summaries=[]
 for folder in sorted(P.iterdir()):
  if not folder.is_dir():continue
  values=receipts(folder)
  entries=[json.loads(v) for k,v in values.items() if k.endswith('/result.json')]
  if not entries:continue
  r=entries[0];c=r['cycles'][-1];before={x['owner']:x for x in c['before_balance']};after={x['owner']:x for x in c['before_placement']}
  owners={str(i):{'cpu_seconds':after[i]['process_cpu_seconds']-before[i]['process_cpu_seconds'],'splits':after[i]['splits']-before[i]['splits'],'jobs_completed':after[i]['jobs_completed']-before[i]['jobs_completed'],'vectors_moved':after[i]['transfers']-before[i]['transfers'],'work':delta(after[i]['work'],before[i]['work']),'cumulative_longest_scopes':{k:v['max_wall_nanos'] for k,v in after[i]['work']['inclusive_scopes'].items() if k.startswith('Scheduled')}} for i in after}
  summary={'name':folder.name,'status':r['status'],'rows':r['rows'],'resume_input':r['resume_input'],'construction_seconds':r['construction_seconds'],'add_seconds':c['add_seconds'],'balance_seconds':c['balance_seconds'],'placement_seconds':c['placement_seconds'],'validation_seconds':r['validation_seconds'],'owners_before_add':{str(x['owner']):x['vectors'] for x in c['before_add']},'owners_before_balance':{str(x['owner']):x['vectors'] for x in c['before_balance']},'owners_after_placement':{str(x['owner']):x['vectors'] for x in c['after_placement']},'balance_owners':owners,'balance_rpc':delta(c['rpc_phases']['balance']['after'],c['rpc_phases']['balance']['before'])}
  (folder/'summary.json').write_text(json.dumps(summary,indent=2));summaries.append(summary)
  print(folder.name,'add/balance/place',*[round(summary[k],3) for k in ['add_seconds','balance_seconds','placement_seconds']],'validation',round(r['validation_seconds'],3))
 (P/'summary.json').write_text(json.dumps(summaries,indent=2))
if __name__=='__main__':main()
