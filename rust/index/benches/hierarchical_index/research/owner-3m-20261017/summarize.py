import json,pathlib,statistics
base=pathlib.Path(__file__).resolve().parent
r=base/'evidence/cache1024'
out={'status':'incomplete','arms':{},'limitations':['Four owners are local actors on one machine; each tree starts on owner0.','Independent stochastic trees; no fixed-work speedup claim.','Construction matches data/order/metric/workers/thresholds/search policies and balance cadence; owner checkpoints publish without writer reopen.','Owner queries use FP navigation and32independent readers; reference query-cache/traversal semantics differ. Fresh-reader retains provider and OS cache.','100 reused corpus queries are not held out.']}
for n in [1,4]:
 p=r/f'owner-{n}/result.json'
 if not p.exists():
  ep=r/f'owner-{n}-exit.json'; lp=r/f'owner-{n}.log'
  progress=[]
  if lp.exists():
   for line in lp.read_text().splitlines():
    try:v=json.loads(line)
    except ValueError:continue
    if 'progress_stage' in v:progress.append(v)
  out['arms'][str(n)]={'status':'incomplete','process_receipt':json.loads(ep.read_text()) if ep.exists() else None,'last_completed_insertion_checkpoint':max([v['completed_rows_or_queries'] for v in progress if v['progress_stage']=='add' and v['event']=='exit'],default=0),'last_completed_balance_checkpoint':max([v['completed_rows_or_queries'] for v in progress if v['progress_stage']=='balance' and v['event']=='exit'],default=0),'last_progress':progress[-1] if progress else None,'query_rows':[]}
  continue
 x=json.loads(p.read_text());out['arms'][str(n)]={'rows':x['census']['current_rows'],'input_verification_seconds':x['input_seconds'],'insertion_seconds':x['add_seconds'],'input_copy_seconds_nested_in_insertion':x['copy_seconds_nested_add'],'balancing_seconds':x['balance_seconds'],'redistribution_seconds':x['placement_seconds'],'publication_seconds':x['save_seconds']+sum(v['seconds'] for v in x['checkpoint_publications']),'validation_seconds':x['audit_seconds'],'construction_phase_seconds':x['add_seconds']+x['balance_seconds']+x['placement_seconds']+x['save_seconds']+sum(v['seconds'] for v in x['checkpoint_publications']),'peak_rss_bytes':x['process_memory']['peak_rss_bytes_linux'],'shape':x['after_save']['shape'],'owners':x['after_save']['owners'],'counts':x['after_save']['counts'],'effective_balance_concurrency':x['after_save']['peak_distinct_subtree_workers'],'vector_store':x['after_save']['vector_store'],'query_rows':[]}
 q=r/f'owner-query-{n}.json'
 if q.exists():
  for row in json.loads(q.read_text())['rows']:
   qs=row['queries'];out['arms'][str(n)]['query_rows'].append({'tau':row['tau'],'rerank_factor':row['rerank_factor'],'pass':row['pass'],'recall100_percent':100*statistics.mean(v['recall100'] for v in qs),'mean_latency_ms':1000*statistics.mean(v['seconds'] for v in qs),'p95_latency_ms':1000*sorted(v['seconds'] for v in qs)[94],'mean_scanned_vectors':statistics.mean(v['scanned_vectors'] for v in qs),'wall_seconds':row['wall_seconds']})
if len(out['arms'])==2 and all(len(v['query_rows'])==12 for v in out['arms'].values()):out['status']='completed'
(base/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items() if k!='arms'},indent=2))
