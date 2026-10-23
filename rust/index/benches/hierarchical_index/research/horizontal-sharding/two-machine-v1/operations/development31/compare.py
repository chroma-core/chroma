import json,statistics
from pathlib import Path
p=Path(__file__).resolve().parent
out=[]
for folder in sorted(p.iterdir()):
 f=folder/'owner1-receipts.json'
 if not f.exists():continue
 receipts=json.loads(f.read_text());entries=[json.loads(v) for k,v in receipts.items() if k.endswith('/result.json')]
 if not entries:continue
 r=entries[0];c=r['cycles'][-1];row={'name':folder.name,'status':r['status'],'construction':r['construction_seconds'],'insert':c['add_seconds'],'balance':c['balance_seconds'],'redistribute':c['placement_seconds']}
 for phase,start,end in [('insert','before_add','after_add'),('balance','before_balance','before_placement')]:
  before={x['owner']:x for x in c[start]};after={x['owner']:x for x in c[end]}
  row[phase+'_cpu']=sum(x['process_cpu_seconds']-before[i]['process_cpu_seconds'] for i,x in after.items())
  row[phase+'_bytes']=c['rpc_phases']['add' if phase=='insert' else phase]['after']['application_framed_bytes_total']-c['rpc_phases']['add' if phase=='insert' else phase]['before']['application_framed_bytes_total']
  if 'detailed' in folder.name:
   row[phase+'_owners']={}
   for i,x in after.items():
    scopes={k:{metric:(v[metric]-before[i]['work']['inclusive_scopes'][k][metric])/1e9 for metric in ['thread_cpu_nanos','wall_nanos']} for k,v in x['work']['inclusive_scopes'].items() if k.startswith(('Insertion','Boundary','SplitInstallation'))}
    counters={k:v-before[i]['work']['counters'][k] for k,v in x['work']['counters'].items() if k.startswith('Boundary')}
    row[phase+'_owners'][i]={'scopes':scopes,'counters':counters,'refresh_longest':x.get('centroid_refresh_longest')}
 for key in ['save_seconds','save_profile','repeat_save','fresh_reader_validation_seconds','validation_complete']:
  if key in r:row[key]=r[key]
 out.append(row)
(p/'comparison.json').write_text(json.dumps(out,indent=2)+'\n')
for x in out:print(x['name'],x['status'],*(f'{k}={x[k]:.3f}' for k in ['insert','balance','construction']),f"save={x.get('save_seconds')}")
for prefix in ['control','readiness','window256','workers64','refreshgate','combined']:
 rows=[x for x in out if x['name'].startswith(prefix+'-') and x['name'].split('-')[-1].isdigit()]
 if rows:print('MEDIAN',prefix,len(rows),{k:round(statistics.median(x[k] for x in rows),3) for k in ['insert','balance','construction','insert_cpu','balance_cpu']})
