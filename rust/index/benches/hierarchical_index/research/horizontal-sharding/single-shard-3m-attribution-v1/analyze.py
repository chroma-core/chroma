"""Compare matched measured cycles without equating random construction work."""
import argparse,json,statistics
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('evidence',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
paths=sorted(a.evidence.glob('0*/result.json'))
assert len(paths)==4, 'Expected complete R,C,C,R; preserve partial evidence separately'
runs=[json.loads(x.read_text()) for x in paths]
assert [r['arm'] for r in runs]==['unsharded_reference','single_shard_owner','single_shard_owner','unsharded_reference']
keys=['rows','dimensions','workers','metric','settings','batch_schedule','input_sha256_operator_verified']
assert all({k:r[k] for k in keys}=={k:runs[0][k] for k in keys} for r in runs)
assert all(r['rows']==3_000_000 and r['validation']['exact_once_vectors']==3_000_000 for r in runs)
expected=list(range(100000,1000001,100000))+[2000000,3000000]
assert all([c['rows_after'] for c in r['cycles']]==expected for r in runs)
summary={'configuration':{k:runs[0][k] for k in keys},'runs':[],'comparisons':[]}
for path,r in zip(paths,runs):
 row={'source':str(path),'arm':r['arm'],'build_save_seconds':r['build_and_save_total']['wall_seconds'],'total_seconds':r['total']['wall_seconds'],'phases':r['phases'],'levels':r['shape']['level_node_counts'],'leaf_count':len(r['shape']['leaf_sizes']),'leaf_min':min(r['shape']['leaf_sizes']),'leaf_max':max(r['shape']['leaf_sizes']),'coordination':r['coordination'],'cycles':[]}
 for c in r['cycles']:
  cycle={'rows_after':c['rows_after'],'shape':c['shape']['levels'],'phase':{},'coordination_cumulative':c['coordination_cumulative'],'add_breakdown':c['add_breakdown']}
  for phase in ['add','balance']:
   w=c[phase+'_work'];v=dict(c[phase]);v.update(navigate_calls=w['calls'][1],navigate_distances=w['navigate_dist_count'],navigate_worker_seconds=w['nanos'][1]/1e9,navigate_distance_loop_worker_seconds=w['navigate_substeps'][0]/1e9,navigate_sort_worker_seconds=w['navigate_substeps'][1]/1e9,split_calls=w['calls'][3],merge_calls=w['calls'][4],leaf_split_input_rows=sum(w['split_sizes']),leaf_kmeans_worker_seconds=w['split_substeps'][0]/1e9,npa_cluster_worker_seconds=w['split_substeps'][2]/1e9,npa_neighbor_worker_seconds=w['split_substeps'][3]/1e9,transferred_vectors=w['transferred_vectors'],transfer_retries=w['transfer_retries'],claim_conflicts=w['claim_conflicts'],balance_rounds=w['balance_rounds'])
   cycle['phase'][phase]=v
  row['cycles'].append(cycle)
 summary['runs'].append(row)
for n,stop in enumerate(expected):
 for phase in ['add','balance']:
  rr=[runs[i]['cycles'][n][phase] for i in [0,3]];cc=[runs[i]['cycles'][n][phase] for i in [1,2]]
  rw=statistics.mean(x['wall_seconds'] for x in rr);cw=statistics.mean(x['wall_seconds'] for x in cc)
  summary['comparisons'].append({'rows_after':stop,'phase':phase,'reference_mean_wall_seconds':rw,'candidate_mean_wall_seconds':cw,'difference_seconds':cw-rw,'ratio':cw/rw,'reference_mean_cpu_seconds':statistics.mean(x['process_cpu_seconds'] for x in rr),'candidate_mean_cpu_seconds':statistics.mean(x['process_cpu_seconds'] for x in cc)})
summary['phase_comparisons']=[]
for phase in runs[0]['phases']:
 rr=[runs[i]['phases'][phase] for i in [0,3]];cc=[runs[i]['phases'][phase] for i in [1,2]]
 rw=statistics.mean(x['wall_seconds'] for x in rr);cw=statistics.mean(x['wall_seconds'] for x in cc)
 summary['phase_comparisons'].append({'phase':phase,'reference_wall_seconds':[x['wall_seconds'] for x in rr],'candidate_wall_seconds':[x['wall_seconds'] for x in cc],'reference_mean_wall_seconds':rw,'candidate_mean_wall_seconds':cw,'difference_seconds':cw-rw,'ratio':cw/rw if rw else None,'reference_mean_cpu_seconds':statistics.mean(x['process_cpu_seconds'] for x in rr),'candidate_mean_cpu_seconds':statistics.mean(x['process_cpu_seconds'] for x in cc)})
summary['mean_build_ratio']=statistics.mean(runs[i]['build_and_save_total']['wall_seconds'] for i in [1,2])/statistics.mean(runs[i]['build_and_save_total']['wall_seconds'] for i in [0,3])
summary['limits']=['Two random constructions per arm are descriptive, not a stable distribution.','Task/substep worker times overlap and are not an additive wall-time explanation.','Counter deltas describe differing work; split input rows do not measure kmeans iterations or internal clustering.','Coordination totals are cumulative; peak active jobs is a running maximum, not an additive counter.','Process high-water RSS is cumulative, not a reset phase peak.']
a.output.write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({'mean_build_ratio':summary['mean_build_ratio'],'runs':[{k:v for k,v in r.items() if k not in ['cycles','phases']} for r in summary['runs']]},indent=2))
