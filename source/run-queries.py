"""Prespecified warm saved-reader recall/work comparison; no lifecycle actions."""
import argparse,hashlib,json,math,os,resource,statistics,subprocess,time,sys
from pathlib import Path
from harness_common import descriptor,verify,write,require
from stage_coverage import mapping,join_trace
HERE=Path(__file__).resolve().parent
SETTINGS=[dict(name='beam32',beam=32,k=100,rerank_factor=2)]

def validate(response,request,gt):
 require('error' not in response,'native query error')
 require(response['request_id']==request['request_id'] and response['generation']==1 and response['shard_id']=='global','query response pin')
 require(response['query_work_policy']=='ordinary-query-work-v1','counter policy echo')
 ids=response['ids'];require(len(ids)==len(set(ids)) and all(type(i) is int and 0<=i<8192 for i in ids),'returned IDs')
 require(len(response['distances'])==len(ids) and all(math.isfinite(v) for v in response['distances']),'returned distances')
 work=response['query_work'];c=work['counts'];require(all(type(v) is int and v>=0 for v in c.values()),'work integer counts')
 require(c['selected_leaves']==work['selected_leaves'] and c['nonempty_leaves_scored']<=c['selected_leaves'],'leaf counters')
 require(c['posting_rows_scored_before_dedup']>=c['distinct_candidates_before_trim']>=c['candidates_after_trim'],'pretrim counts')
 require(c['candidates_after_trim']==min(c['distinct_candidates_before_trim'],request['k']*request['rerank_factor']),'candidate trim count')
 require(c['exact_embedding_gets']==c['exact_vectors_reranked']==c['candidates_after_trim'],'successful exact rerank count')
 require(work['returned_results']==len(ids)==min(request['k'],c['exact_vectors_reranked']),'returned count')
 require(response['actual_candidate_count']==c['candidates_after_trim'],'legacy clamp compatibility')
 require(len(work['returned_distance_bits'])==len(ids),'returned bit count')
 import struct
 require(work['returned_distance_bits']==[struct.unpack('<I',struct.pack('<f',x))[0] for x in response['distances']],'returned f32 bits')
 obs=response['vector_pin_observation'];require(obs is not None and obs['access_overflow'] is False,'incomplete vector observation')
 io={key:sum(v[key] for v in obs['block_access'].values()) for key in ['reader_requests','pin_hits','pin_misses','provider_obtains','provider_hits','provider_misses','storage_callbacks','encoded_bytes','decode_attempts','decode_successes']}
 require(io['storage_callbacks']>=0,'IO counts')
 return dict(query_id=gt['query_id'],recall_at_100=len(set(ids)&set(gt['ids']))/100,gt_hits=len(set(ids)&set(gt['ids'])),returned_ids=ids,returned_f32_distance_bits=work['returned_distance_bits'],counts=c,elapsed_ms=response['elapsed_ms'],timings={k:work[k] for k in ['navigate_ns','quantize_ns','distance_ns','sort_dedup_ns','rerank_ns']},vector_io=io,scope='Warm native operation elapsed excludes readeropen/serialization/transport; includes clear/map/memory accounting. Vector-only observer excludes rootopens/posting/scalar/cache reconstruction/physicalIO. Selected leaves are navigation proxy, not distance-count work.')

def run(s):
 require(sys.platform=='linux','Linux runtime only')
 require(s['navigation_policies']==['one-bit','fp','four-bit'],'prespecified navigation policies')
 require(s['settings']==SETTINGS and s['warmup_passes']==s['measured_passes']==1,'prespecified settings/pass count')
 require((s['reader_seconds'],s['total_seconds'])==(10,60) and time.time()<s['deadline_unix'],'fixed deadline/budgets')
 require(s['bindings']==json.loads((HERE/'runtime-spec.template.json').read_text())['bindings'],'frozen saved-input bindings')
 binary=verify(s['binary']);b=s['bindings'];qpath=verify(b['queries']);gpath=verify(b['ground_truth']);queries=[json.loads(v) for v in qpath.read_text().splitlines()];gt=[json.loads(v) for v in gpath.read_text().splitlines()];require(len(queries)==len(gt)==16 and [q['query_id'] for q in queries]==[q['query_id'] for q in gt],'query/GT cohort');require(all(len(x['ids'])==len(set(x['ids']))==100 for x in gt),'expected exact GT')
 join_setup_started=time.monotonic();join_map_bytes=0;join_quality_bytes=0
 snapshots={};leaf_sets={};maps={};visible_node_counts={}
 for arm in ['reference','owner']:
  mf=verify(b[arm]['manifest']);mp=verify(b[arm]['map']);m=json.loads(mf.read_text());require(m['generation']==1 and m['dimension']==768 and m['metric']=='ip' and m['vector_count']==8192 and m['id_mapping']['sha256']==b[arm]['map']['sha256'],'saved checkpoint metadata');snapshots[arm]=(mf,mp);maps[arm]=mapping(mp,b[arm]['map']);quality=json.loads(verify(b[arm]['quality']).read_text());require(quality['owners'][0]['snapshot_sha256']==b[arm]['manifest']['sha256'],'saved leaf report binding');leaf_sets[arm]={leaf['leaf_id']:leaf['gt_ids'] for leaf in quality['owners'][0]['census']['leaves']};visible_node_counts[arm]=quality['owners'][0]['census']['reachable_nodes'];join_map_bytes+=b[arm]['map']['bytes'];join_quality_bytes+=b[arm]['quality']['bytes']
 join_setup_seconds=time.monotonic()-join_setup_started
 out=Path(s['output']);out.mkdir(exist_ok=False);started=time.monotonic();cells=[];error=None
 try:
  for navigation_policy in ['one-bit','fp','four-bit']:
   setting=SETTINGS[0]
   for arm in ['reference','owner']:
    name=arm+'-'+navigation_policy;requests=[dict(request_id=phase+':'+q['query_id'],generation=1,op='query',query=q['vector'],k=100,beam_min=setting['beam'],beam_max=setting['beam'],rerank_factor=2) for phase in ['warmup','measured'] for q in queries];path=out/(name+'-requests.jsonl');path.write_text(''.join(json.dumps(q)+'\n' for q in requests))
    command=[str(binary),'read','--checkpoint-dir',str(snapshots[arm][0].parent),'--profile-query-work','--profile-rerank-pins','--trace-query-stages','--query-navigation',navigation_policy]
    # Original publication has manifest.json rather than manifest-1.json; its
    # pinned contents enforce generation1. Do not create a downstream alias.
    remaining=min(10,60-(time.monotonic()-started),s['deadline_unix']-time.time());require(remaining>0,'fixed query deadline')
    t=time.monotonic();cpu=resource.getrusage(resource.RUSAGE_CHILDREN)
    with path.open('rb') as stdin,(out/(name+'.stdout')).open('xb') as stdout,(out/(name+'.stderr')).open('xb') as stderr:
     try:p=subprocess.run(command,stdin=stdin,stdout=stdout,stderr=stderr,timeout=remaining,env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1'))
     except subprocess.TimeoutExpired:write(out/(name+'-process.json'),dict(command=command,status='timeout',elapsed_seconds=time.monotonic()-t));raise
    after=resource.getrusage(resource.RUSAGE_CHILDREN);write(out/(name+'-process.json'),dict(command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-t,user_cpu_seconds=after.ru_utime-cpu.ru_utime,system_cpu_seconds=after.ru_stime-cpu.ru_stime,scope='Whole process includes rootopen/warmup/measured/serialization/exit, not warmqueryCPU.'));require(p.returncode==0,'reader failed')
    replies=[json.loads(v) for v in (out/(name+'.stdout')).read_text().splitlines()];require(len(replies)==32,'response count')
    cache_key='compressed_navigation_cache' if navigation_policy=='four-bit' else 'fp_navigation_cache';cache=replies[0][cache_key];require(all(v['query_navigation_policy']==navigation_policy and v[cache_key]==cache for v in replies),'navigation policy/cache echo')
    for v in replies:
     require(v['query_navigation_snapshot']=={k:json.loads(snapshots[arm][0].read_text())[k] for k in ['generation','source_id','dimension','metric','root_ref']},'navigation saved binding')
     require(v['query_work']['counts']['expanded_internal_nodes']>0 and v['query_work']['counts']['centroid_distance_evaluations']>0,'navigation work absent')
    if navigation_policy=='four-bit':
     require(cache is not None and cache['policy']=='four-bit' and cache['nodes']==visible_node_counts[arm] and cache['dimension']==768,'compressed cache binding')
     require(cache['code_bytes_per_node']==396 and cache['retained_code_payload_bytes']==cache['nodes']*396 and cache['temporary_owned_fp_payload_bytes']==cache['nodes']*3072,'compressed payload')
     require(cache['accounted_preparation_peak_payload_bytes']==cache['nodes']*(3072+396)+3072+396 and cache['per_distance_unpack_logical_payload_bytes']==6144,'temporary payload accounting')
     loaded=cache['fp_source_load'];require(loaded['root_id']==json.loads(snapshots[arm][0].read_text())['root_ref']['id'] and loaded['nodes']==cache['nodes'] and loaded['dimension']==768 and loaded['point_gets']==cache['nodes'] and loaded['vector_access']['access_overflow'] is False,'compressed FP source binding')
     require(all(v['fp_navigation_cache'] is None for v in replies),'compressed retained FP metadata ambiguity')
    elif navigation_policy=='fp':
     require(cache is not None and cache['nodes']==visible_node_counts[arm],'centroid visibility count')
     require(cache['point_gets']==cache['nodes'] and cache['root_id']==json.loads(snapshots[arm][0].read_text())['root_ref']['id'],'centroid root/read count')
     require(cache['dimension']==768 and cache['owned_f32_payload_bytes']==cache['logical_centroid_return_bytes']==cache['nodes']*768*4 and cache['owned_f32_payload_bytes']<=1048576 and cache['nodes']<=256,'centroid payload/cap')
     require(cache['vector_access']['access_overflow'] is False,'incomplete centroid IO observation')
    else:require(cache is None,'onebit unexpectedly loaded FPcache')
    observed=[validate(reply,request,gt[i%16]) for i,(reply,request) in enumerate(zip(replies,requests))];
    for i,(item,response) in enumerate(zip(observed,replies)):
     item['stage_coverage']=join_trace(response,gt[i%16]['ids'],leaf_sets[arm],maps[arm]);require(item['stage_coverage']['returned_gt_hits']==item['gt_hits'],'recall join differs')
    measured=observed[16:];cells.append(dict(arm=arm,setting=setting,navigation_policy=navigation_policy,fp_navigation_cache=cache if navigation_policy=='fp' else None,compressed_navigation_cache=cache if navigation_policy=='four-bit' else None,reader_open_seconds=replies[0]['query_reader_open_seconds'],warmup_queries=16,measured=measured,mean_recall_at_100=statistics.mean(v['recall_at_100'] for v in measured),median_warm_native_elapsed_ms=statistics.median(v['elapsed_ms'] for v in measured)))
    require(descriptor(snapshots[arm][0])==b[arm]['manifest'] and descriptor(snapshots[arm][1])==b[arm]['map'],'original saved input changed')
 except Exception as e:error=repr(e)
 finally:
  size=sum(p.stat().st_size for p in out.rglob('*') if p.is_file())
  if size>50*1024**2:error='observed metadata exceeds50MiB'
  result=dict(status='passed' if error is None else 'failed',error=error,cells=cells,join_setup_seconds=join_setup_seconds,join_setup_payload=dict(map_consumed_bytes=join_map_bytes,quality_hash_bytes=join_quality_bytes,quality_parse_bytes=join_quality_bytes,scope='NewGTjoin setup payload only; outside nativequery. Existingbindinghashes/native readeropens separate, not physicalIO.'),elapsed_seconds=time.monotonic()-started,metadata_bytes=size,binary=s['binary'],bindings=b,scope='ExistingCode4compressedcontrolvsFPandactualCode1centroidscoring atsamePER-LEVELbeam32; finalleafcounts/work can differ on mixeddepthtree. Reused16exploratoryqueries/disjointnessunknown. Same8192originalsnapshots/exactFP64prefixGT; notnewconstruction. Warmup+onewarm retainedpass/settings; trace copy overhead included, prior latency incomparable. GT joins outside native query timing. FP32 distances/cutoffties/order unchanged; recall uses frozen FP64top100/globalIDtie reference. Global savedreader is not actorfanout/network. No retries/scaleincrease/coldIO/physicalIO claim; externalguard/deadline required. Code4preparationfirstownsFPthenretainscodes; quantizer/unpack scratchandoriginalstoragearenot eliminated.')
  write(out/'result.json',result)
 return result
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--spec',required=True);a=p.parse_args();r=run(json.loads(Path(a.spec).read_text()));print(json.dumps(r));raise SystemExit(r['status']!='passed')
