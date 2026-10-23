"""Extract phase CPU and work without treating inclusive scopes as exclusive."""
from pathlib import Path
import json,statistics,math
p=Path(__file__).resolve().parent
rows=[]
for folder in sorted(p.iterdir()):
 f=folder/'owner1-receipts.json'
 if not f.exists():continue
 receipts=json.loads(f.read_text());entries=[json.loads(v) for k,v in receipts.items() if k.endswith('/result.json')]
 if not entries:continue
 r=entries[0]
 if r.get('status')=='writer_checkpoint_complete':continue
 c=r['cycles'][-1];b={x['owner']:x for x in c['before_balance']};a=c['before_placement'];row={'name':folder.name,'status':r['status'],'balance_s':c['balance_seconds'],'construction_s':r['construction_seconds'],'insertion_s':c['add_seconds'],'balance_cpu_s':sum(x['process_cpu_seconds']-b[x['owner']]['process_cpu_seconds'] for x in a)}
 before_add={x['owner']:x for x in c['before_add']};row['insertion_cpu_s']=sum(x['process_cpu_seconds']-before_add[x['owner']]['process_cpu_seconds'] for x in c['after_add'])
 row['detailed_enabled']=bool(json.loads((folder/'run.json').read_text()).get('detailed'))
 row['fine_counter_note']='Boundary refresh and new fine attribution counters are uncollected when detailed_enabled is false; zeros in those fields are not measured zeros.'
 row['work']={k:sum(x['work']['counters'][k]-b[x['owner']]['work']['counters'][k] for x in a) for k in a[0]['work']['counters']}
 row['scopes']={k:{m:sum(x['work']['inclusive_scopes'][k][m]-b[x['owner']]['work']['inclusive_scopes'][k][m] for x in a)/1e9 for m in ['thread_cpu_nanos','wall_nanos']} for k in a[0]['work']['inclusive_scopes']}
 row['clustering']={k:sum(x['clustering_process'][k]-b[x['owner']]['clustering_process'][k] for x in a) for k in a[0]['clustering_process']}
 row['owner_cpu_s']={str(x['owner']):x['process_cpu_seconds']-b[x['owner']]['process_cpu_seconds'] for x in a}
 for key in ['save_seconds','validation_complete','repeat_save']:
  if key in r:row[key]=r[key]
 rows.append(row)
(p/'comparison.json').write_text(json.dumps(rows,indent=2)+'\n')
print(json.dumps({'completed_construction_runs':len(rows)}))
