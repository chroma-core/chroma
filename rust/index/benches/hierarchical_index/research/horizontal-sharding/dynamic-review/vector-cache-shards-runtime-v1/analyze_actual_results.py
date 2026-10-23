from pathlib import Path
import json,hashlib,statistics,importlib.util,sys
w=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding');o=w/'dynamic-review/vector-cache-shards-runtime-v1';e=w/'fleet/vector-cache-shards-runtime-v1/runtime-evidence';p=e/'runtime-attempt1'
sys.path.insert(0,str(o))
def d(f):b=f.read_bytes();return dict(bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
for v in json.loads((e/'retrieval-manifest.json').read_text()):assert d(e/v['path'])=={k:v[k] for k in ['bytes','sha256']}
sp=importlib.util.spec_from_file_location('glue',o/'run-cache.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m);r=json.loads((p/'result.json').read_text());expected=json.loads((o/'fixed-decisions.json').read_text());assert r['status']=='passed';cells=[];verified=0
for c in r['cells']:
 arm=c['arm'];nav=c['navigation_policy'];shards=c['cache_policy']['shards'];name=f'{arm}-{nav}-shards{shards}';raw=[json.loads(v) for v in (p/(name+'.stdout')).read_text().splitlines()];assert len(raw)==32
 for response in raw:
  assert m.decision_digest(response)==expected[arm+'-'+nav]['decisions'][response['request_id']];assert response['provider_cache_policy']==c['cache_policy'];verified+=1
 totals={k:sum(v['vector_io'][k] for v in c['measured']) for k in c['measured'][0]['vector_io']};blocks={}
 for v in raw[16:]:
  for block,a in v['vector_pin_observation']['block_access'].items():
   dst=blocks.setdefault(block,{k:0 for k in ['provider_obtains','provider_hits','provider_misses','storage_callbacks','encoded_bytes','decode_successes']})
   for k in dst:dst[k]+=a[k]
 cells.append(dict(arm=arm,navigation=nav,cache_policy=c['cache_policy'],retained_vector_totals=totals,retained_blocks=blocks,mean_counts={k:statistics.mean(v['counts'][k] for v in c['measured']) for k in c['measured'][0]['counts']},median_trace_elapsed_ms=c['median_warm_native_elapsed_ms'],reader_open_seconds=c['reader_open_seconds'],compressed_cache=c['compressed_navigation_cache']))
pairs=[]
for arm in ['reference','owner']:
 a=next(v for v in cells if v['arm']==arm and v['cache_policy']['shards']==64);b=next(v for v in cells if v['arm']==arm and v['cache_policy']['shards']==1);fa=a['retained_vector_totals']['storage_callbacks'];fb=b['retained_vector_totals']['storage_callbacks'];label='inconclusive-baseline-did-not-reproduce' if fa==0 else ('consistent-with-shard-fragmentation-mitigation' if fb==0 else 'single-shard-did-not-eliminate-fetches');assert label==next(v for v in r['attribution'] if v['arm']==arm)['classification'];pairs.append(dict(arm=arm,classification=label,baseline_reproduced=fa>0,retained_64_vs_1={k:[a['retained_vector_totals'][k],b['retained_vector_totals'][k]] for k in ['provider_obtains','provider_hits','provider_misses','storage_callbacks','encoded_bytes','decode_successes']},numerical_work_equal=a['mean_counts']==b['mean_counts'],no_retry=True))
result=dict(status='scientific-passed-terminal-cost-pending',evidence={str(f.relative_to(w)):d(f) for f in [e/'retrieval-manifest.json',p/'result.json',o/'fixed-decisions.json']},binary=r['binary'],controller_seconds=r['elapsed_seconds'],join_setup_seconds=r['join_setup_seconds'],raw_decisions_echoes_verified=verified,cells=cells,attribution=pairs,limits=['NewrandomhashassignmentdoesnotreconstructhistoricalUUIDbuckets/evictions.','SamecapacitytargetsbutBOTHblock/rootpartitioning changes; actualvectortrafficnotwholecache.','Encodedpayloadreturns/decodeevents notphysicalIO/network/latency/RSSproof.','Exactnumericalpins/counterssame; initialization/cachestateandfixedorder stilllimitcausaltiming.','Prespecifiednonreproductionisinconclusive,noretry/seed/size/policysweep.','Same8192/reused16querycohort, no qualitygeneralization or deeperpruningclaim.'])
(o/'ACTUAL-RESULT-AND-NEXT-DECISION.json').write_text(json.dumps(result,indent=2)+'\n');print('verified',verified,'pairs',pairs)
