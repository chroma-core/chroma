from pathlib import Path
import json,statistics,math,subprocess,sys
p=Path(__file__).resolve().parent
subprocess.run([sys.executable,str(p/'analyze.py')],check=True,stdout=subprocess.DEVNULL)
rows=json.loads((p/'comparison.json').read_text());by={r['name']:r for r in rows};out={'blocks':4,'comparisons':{},'detailed_runs':{},'measurement_note':'CPU scopes are inclusive. New fine counters require detailed mode. Remote job origin is unassigned; transport request type and sampled CPU profiles are separate evidence.'}
for a,b in [('off','on'),('on','early')]:
 pairs=[(by[f'32-fixed-{a}-{i}'],by[f'32-fixed-{b}-{i}']) for i in range(1,5) if f'32-fixed-{a}-{i}' in by and f'32-fixed-{b}-{i}' in by]
 if not pairs:continue
 entry={'completed_pairs':len(pairs),'metrics':{}}
 for key in ['balance_s','balance_cpu_s','construction_s']:
  aa=[x[key] for x,y in pairs];bb=[y[key] for x,y in pairs];dd=[y-x for x,y in zip(aa,bb)];v={'control_mean':statistics.mean(aa),'candidate_mean':statistics.mean(bb),'control_range':[min(aa),max(aa)],'candidate_range':[min(bb),max(bb)],'paired_difference':statistics.mean(dd),'paired_differences':dd}
  if len(pairs)==4:
   margin=3.182446305284263*statistics.stdev(dd)/2;v['approximate_95pct_paired_t_interval']=[statistics.mean(dd)-margin,statistics.mean(dd)+margin]
  entry['metrics'][key]=v
 entry['mean_work']={k:{a:statistics.mean(x['work'][k] for x,y in pairs),b:statistics.mean(y['work'][k] for x,y in pairs)} for k in pairs[0][0]['work'] if not k.startswith('Boundary')}
 out['comparisons'][f'{b}_versus_{a}']=entry
for arm in ['off','on','early']:
 name=f'32-fixed-{arm}-detailed';f=p/name/'owner1-receipts.json'
 if not f.exists():continue
 receipts=json.loads(f.read_text());r=json.loads(next(v for k,v in receipts.items() if k.endswith('/result.json')));c=r['cycles'][-1];before={x['owner']:x for x in c['before_balance']};after=c['before_placement'];entry={'phase':by[name],'owners':{}}
 for x in after:
  old=before[x['owner']];origins={}
  for origin,values in x['work']['by_origin'].items():
   previous=old['work']['by_origin'].get(origin,{});v={'counters':{k:n-previous.get('counters',{}).get(k,0) for k,n in values['counters'].items()},'inclusive_scopes':{}}
   for scope,measures in values['inclusive_scopes'].items():
    prev=previous.get('inclusive_scopes',{}).get(scope,{})
    v['inclusive_scopes'][scope]={k:n-prev.get(k,0) for k,n in measures.items() if isinstance(n,(int,float)) and k not in ['max_wall_nanos']}
   origins[origin]=v
  entry['owners'][str(x['owner'])]={'by_origin':origins,'longest_jobs':x['work']['longest_jobs'],'scheduler':x['scheduler'],'refresh_longest':x.get('centroid_refresh_longest'),'rpc_phases':c['rpc_phases']['balance'] if x['owner']==1 else None}
 out['detailed_runs'][arm]=entry
(p/'fixed-summary.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out['comparisons'],indent=2))
