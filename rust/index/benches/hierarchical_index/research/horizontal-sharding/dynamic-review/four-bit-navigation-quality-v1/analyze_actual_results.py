from pathlib import Path
import json,hashlib,statistics
w=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding');e=w/'fleet/four-bit-navigation-quality-v1/runtime-evidence';p=e/'runtime-attempt1';old=w/'fleet/fp-navigation-quality-v1/runtime-evidence/runtime-attempt1';o=w/'dynamic-review/four-bit-navigation-quality-v1'
def d(f):b=f.read_bytes();return dict(bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
for item in json.load(open(e/'retrieval-manifest.json')):assert d(e/item['path'])=={k:item[k] for k in ['bytes','sha256']}
r=json.load(open(p/'result.json'));prior=json.load(open(old/'result.json'));assert r['status']=='passed';cells=[];checks=[];pairs=[]
for c in r['cells']:
 samples=c['measured'];arm=c['arm'];policy=c['navigation_policy'];raw=[json.loads(v) for v in (p/(arm+'-'+policy+'.stdout')).read_text().splitlines()];assert len(raw)==32
 for v in samples:
  t=v['stage_coverage'];assert t['selected_leaf_gt_hits']==t['pretrim_gt_hits'] and t['returned_gt_hits']==v['gt_hits'];assert sum(t[k] for k in ['lost_at_navigation','lost_at_candidate_trim','lost_at_exact_return'])==100-v['gt_hits']
 if policy!='four-bit':
  before=next(x for x in prior['cells'] if x['arm']==arm and x['navigation_policy']==policy)
  for a,b in zip(samples,before['measured']):assert a['query_id']==b['query_id'] and a['returned_ids']==b['returned_ids'] and a['returned_f32_distance_bits']==b['returned_f32_distance_bits'] and a['counts']==b['counts']
  priorraw=[json.loads(v) for v in (old/(arm+'-'+policy+'.stdout')).read_text().splitlines()]
  for a,b in zip(raw,priorraw):assert a['results']==b['results'] and a['query_stage_trace']['selected_leaf_ids']==b['query_stage_trace']['selected_leaf_ids'];assert set(a['query_stage_trace']['pretrim_candidate_ids'])==set(b['query_stage_trace']['pretrim_candidate_ids']) and set(a['query_stage_trace']['retained_candidate_ids'])==set(b['query_stage_trace']['retained_candidate_ids'])
  checks.append(dict(arm=arm,policy=policy,retained_ids_bits_counts_equal=16,raw_results_selectedleaves_equal=32,raw_internal_candidate_sets_equal=32))
 def means(key):return {k:statistics.mean(x[key][k] for x in samples) for k in samples[0][key] if isinstance(samples[0][key][k],(int,float))}
 cache=c.get('compressed_navigation_cache') if policy=='four-bit' else c['fp_navigation_cache'];prep=None
 if cache:
  load=cache['fp_source_load'] if policy=='four-bit' else cache;prep=dict(loaded_nodes=load['nodes'],source_owned_fp_bytes=load['owned_f32_payload_bytes'],source_load_seconds=load['load_seconds'],source_centroid_bits_sha256=load['centroid_bits_sha256'],retained_payload_bytes=cache['retained_code_payload_bytes'] if policy=='four-bit' else load['owned_f32_payload_bytes']);blocks=load['vector_access']['block_access'];prep['actual_source_block_totals']={k:sum(v[k] for v in blocks.values()) for k in ['reader_requests','pin_hits','pin_misses','provider_obtains','provider_hits','provider_misses','storage_callbacks','encoded_bytes','decode_attempts','decode_successes']}
  if policy=='four-bit':
   for k in ['prepare_seconds','accounted_preparation_peak_payload_bytes','per_distance_unpack_logical_payload_bytes','code_bytes_per_node','code_bits_sha256']:prep[k]=cache[k]
 cells.append(dict(arm=arm,policy=policy,mean_recall=c['mean_recall_at_100'],mean_counts=means('counts'),mean_stage_counts=means('stage_coverage'),mean_vector_io=means('vector_io'),median_warm_trace_elapsed_ms=c['median_warm_native_elapsed_ms'],reader_open_seconds=c['reader_open_seconds'],preparation=prep,per_query=samples))
for arm in ['reference','owner']:
 compressed=next(c for c in cells if c['arm']==arm and c['policy']=='four-bit')
 for controlpolicy in ['one-bit','fp']:
  control=next(c for c in cells if c['arm']==arm and c['policy']==controlpolicy);rows=[]
  for x,y in zip(control['per_query'],compressed['per_query']):
   assert x['query_id']==y['query_id'];rows.append(dict(query_id=x['query_id'],returned_gt_delta=y['gt_hits']-x['gt_hits'],selected_leaf_gt_delta=y['stage_coverage']['selected_leaf_gt_hits']-x['stage_coverage']['selected_leaf_gt_hits'],trim_loss_delta=y['stage_coverage']['lost_at_candidate_trim']-x['stage_coverage']['lost_at_candidate_trim'],selected_leaves=[x['counts']['selected_leaves'],y['counts']['selected_leaves']],centroid_evaluations=[x['counts']['centroid_distance_evaluations'],y['counts']['centroid_distance_evaluations']],posting_rows=[x['counts']['posting_rows_scored_before_dedup'],y['counts']['posting_rows_scored_before_dedup']]))
  pairs.append(dict(arm=arm,control=controlpolicy,mean_recall_delta=compressed['mean_recall']-control['mean_recall'],queries_improved=sum(v['returned_gt_delta']>0 for v in rows),queries_worse=sum(v['returned_gt_delta']<0 for v in rows),queries_tied=sum(v['returned_gt_delta']==0 for v in rows),per_query=rows))
result=dict(status='scientific-passed-terminal-cost-pending',evidence={str(f.relative_to(w)):d(f) for f in [e/'retrieval-manifest.json',e/'build/result.json',p/'result.json',old/'result.json']},controller_seconds=r['elapsed_seconds'],join_setup_seconds=r['join_setup_seconds'],join_setup_payload=r['join_setup_payload'],cells=cells,paired_differences=pairs,prior_controls=checks,limits=['Reused16exploratoryqueries/disjointnessunproven; no generalization.','Sameper-levelbeam, actualleaves/evaluations andpostingrowsreported, notassumedmatchedwork.','Allinternalnodesexpanded in controls: leafranking evidence, not deeppruning.','PreparationfirstreadsFP; originalstorage/cachebuffers remain. Accountedpayload excludesquantizerscratch/RSS; unpackingcostperdistanceexplicit.','Warmtraceelapsed+newvalidationoverhead/fixedpolicyorder/cachepreloading prohibitcausallatencyclaim.','Global savedreader notactorfanout/network; encodedreturnsnotphysicalIO; FP32/FP64ties retained.'])
(o/'ACTUAL-RESULT-AND-NEXT-DECISION.json').write_text(json.dumps(result,indent=2)+'\n')
for c in cells:print(c['arm'],c['policy'],c['mean_recall'],c['mean_stage_counts'],c['mean_counts'],c['preparation'])
print([(x['arm'],x['control'],x['queries_improved'],x['queries_worse'],x['queries_tied']) for x in pairs])
