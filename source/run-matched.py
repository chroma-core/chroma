"""One fixed16K three-arm experiment. No lifecycle, transfer, resizing or retry."""
import argparse,hashlib,importlib.util,json,math,os,resource,shutil,statistics,struct,subprocess,sys,time
from pathlib import Path
from harness_common import require,descriptor,verify,write
from construction_support import child,checkpoint,child_peak_kib
from navigation_work import validate_query_work_counts

HERE=Path(__file__).resolve().parent
ROWS=16384
DIM=768
ARMS=('reference','dynamic1','dynamic4')
QUERY_SHA='2a4ad3039af69f0d0f6fe9a536ad33aa6d9a25a2f4f51bb2c4a66fa3b343cb20'
CACHE={'block_capacity_units':256,'root_capacity_entries':4096,'shards':1,'clear_after_operation':False}


def check_queries(path, expected_ids, dimension=DIM, count=100, rows=ROWS):
    values=[json.loads(line) for line in Path(path).read_text().splitlines()]
    require(len(values)==count and [v['query_id'] for v in values]==expected_ids and len(set(expected_ids))==count,'frozen query order/count')
    for q in values:
        require(len(q['vector'])==dimension and all(type(v) in (int,float) and math.isfinite(v) for v in q['vector']),'query dimension/finite')
        struct.pack('<'+'f'*dimension,*q['vector']) # reject FP32 overflow
        if 'canonical_global_id' in q:
            require(type(q['canonical_global_id']) is int and q['canonical_global_id']>=rows,'query overlaps prefix')
        if q.get('id_namespace')=='canonical-global' and 'global_id' in q:
            require(type(q['global_id']) is int and q['global_id']>=rows,'query overlaps prefix')
    return values


def prepare(source,out,source_tree,query_path,expected_ids,rows=ROWS,dimension=DIM,k=100,query_count=100):
    started=time.monotonic();queries=check_queries(query_path,expected_ids,dimension,query_count,rows)
    row_bytes=dimension*4;query_bits={hashlib.sha256(struct.pack('<'+'f'*dimension,*q['vector'])).digest() for q in queries}
    target=out/'evaluation-prefix.f32';copied=0;hash_=hashlib.sha256();chunks=0
    with Path(source).open('rb') as src,target.open('xb') as dst:
        while copied<rows*row_bytes:
            chunk=src.read(min(8192*row_bytes,rows*row_bytes-copied));require(chunk and len(chunk)%row_bytes==0,'truncated/unaligned prefix')
            for offset in range(0,len(chunk),row_bytes):
                require(hashlib.sha256(memoryview(chunk)[offset:offset+row_bytes]).digest() not in query_bits,'query vector bits duplicate a training row')
            dst.write(chunk);hash_.update(chunk);copied+=len(chunk);chunks+=1
        dst.flush();os.fsync(dst.fileno())
    copy_seconds=time.monotonic()-started;sys.path.insert(0,str(source_tree/'tools'))
    from hierarchical_sharding.exact_blocked import exact_blocked
    import numpy as np
    gt_started=time.monotonic();gt=exact_blocked(target,queries,dimension,k=k,metric='ip',corpus_block=8192,query_block=16)
    require([x['query_id'] for x in gt]==expected_ids,'GT query order')
    for x in gt:
        require(len(x['ids'])==len(set(x['ids']))==k and all(type(i) is int and 0<=i<rows for i in x['ids']),'GT coverage')
        require(len(x['distances'])==k and all(math.isfinite(v) for v in x['distances']),'finite GT distances')
        require(list(zip(x['distances'],x['ids']))==sorted(zip(x['distances'],x['ids'])),'GT total ordering')
    gtpath=out/'prefix-gt.jsonl';gtpath.write_text(''.join(json.dumps(x)+'\n' for x in gt))
    result=dict(status='passed',prefix=descriptor(target),prefix_sha256=hash_.hexdigest(),prefix_copy_bytes=copied,prefix_read_chunks=chunks,prefix_copy_seconds=copy_seconds,gt_seconds=time.monotonic()-gt_started,setup_seconds=time.monotonic()-started,ground_truth=descriptor(gtpath),queries=descriptor(query_path),rows=rows,dimension=dimension,k=k,query_ids=expected_ids,engine='existing FP64 exact_blocked,8192corpus/16query blocks',engine_source=descriptor(source_tree/'tools/hierarchical_sharding/exact_blocked.py'),numpy_version=np.__version__,disjointness='ExactFP32 vector duplicates and declared commonnamespace ID overlaps rejected. No common training IDs in retained queries: disjointness remains unknown. All100queries reused/exploratory, not an untouched holdout.',scope='Only dense prefix GT; never filter2MGT. One fullprefix logical copy/hash/read charged; exact engine reads prefix separately. No physicalIO inference.')
    write(out/'preparation.json',result);return result


def shape(x,arm):
    c=x['census'];require(c['unique_live_rows']==ROWS and c['duplicate_current_entries']==c['unreachable_nodes']==0,'full saved census')
    require([v['rows'] for v in x['phases']]==[4096,8192,12288,16384],'common balance cuts')
    require(x['source_read_chunk_rows']==8192 and x['source_read_calls']==2 and x['source_prefix_read_bytes']==ROWS*DIM*4,'input read chunks')
    timing=x['timing_accounting'];parts=x['input_seconds']+timing['setup_seconds']+timing['phase_elapsed_seconds']+x['save_seconds']+x['audit_seconds']+timing['publication_seconds']+timing['other_before_serialize_seconds']
    require(abs(parts-timing['native_before_serialize_seconds'])<1e-6,'native phase accounting')
    if arm=='reference':
        s=x['reference_shape'];require(s['nodes']==s['reachable_nodes'] and s['pending_balance']==0 and s['depth']>=2,'reference shape')
    else:
        s=x['owner_report'];require(s['schedule_sample_overflow'] is False and sum(bool(v) for v in s['last_reference_subtree_assignments'])>=2 and s['peak_distinct_subtree_workers']>=2,'multiple scheduler regions absent')
        require(s['pending_balance']==s['central_current_versions']==s['central_mutable_posting_rows']==0,'unfinished balance/central mirror')
        require(s['predicate_batching'] is True and s['batched_scrub_validation'] is True and s['direct_current_policy'] is False and s['direct_current_reads']==0,'combined owner policy')
        require(s['predicate_batch_computed_rows']>0 and s['scrub_validation_rows']>0,'owner work absent')
        require(x['construction_policy']=='owner-batched-v1','owner echo')
        if arm=='dynamic1':require(s['placement_moves']==0 and sum(v['current']>0 for v in s['owners'])==1,'oneowner migration')
        else:require(s['placement_moves']>0 and sum(v['current']>0 for v in s['owners'])==4,'fourowner placement absent')


def diagnostic_spec(native,pinned,prep,ids):
    return dict(format_version=1,source_id=native['source_id'],dimension=DIM,metric='ip',global_rows=ROWS,max_map_rows=ROWS,ground_truth=prep['ground_truth'],query_vectors=prep['queries'],selected_queries=[dict(query_id=q,expected=100) for q in ids],owners=[dict(owner_id='global',generation=1,snapshot=pinned)])


def validate_quality(report,spec,spec_path,gt):
    require(report['spec_sha256']==descriptor(spec_path)['sha256'] and report['source_id']==spec['source_id'],'diagnostic response binding')
    require(report['policy']=='saved-gt-quality-v1' and report['declared_global_rows']==ROWS and report['dimension']==DIM and report['metric']=='ip','quality shape')
    require(report['ground_truth']==spec['ground_truth'] and report['query_vectors']==spec['query_vectors'] and report['raw_embedding_reads']==0 and len(report['owners'])==1,'quality inputs')
    owner=report['owners'][0];require(owner['owner_id']=='global' and owner['generation']==1 and owner['snapshot_sha256']==spec['owners'][0]['snapshot']['sha256'],'quality snapshot')
    ids=[v['query_id'] for v in spec['selected_queries']];require([v['query_id'] for v in report['queries']]==ids,'quality query order')
    union=set().union(*(set(gt[q]['ids']) for q in ids));require(report['gt_union_count']==len(union),'GT union')
    require(all(v['expected']==v['covered']==100 and len(v['oracles'])==3 for v in report['queries']),'GT coverage/oracles')
    c=owner['census'];require(c['reachable_leaves']<=4096 and c['reachable_nodes']>=c['reachable_leaves'],'saved diagnostic shape cap')
    return owner['census']


def join_stages(reply,gt_ids,leaf_sets,id_map):
    t=reply['query_stage_trace'];c=reply['query_work']['counts'];require(reply['query_stage_trace_policy']=='bounded-query-stages-v1' and t['complete'] is True and t['id_cap']==ROWS,'complete stage trace')
    selected=t['selected_leaf_ids'];pre=t['pretrim_candidate_ids'];kept=t['retained_candidate_ids']
    require([len(selected),len(pre),len(kept)]==[c['selected_leaves'],c['distinct_candidates_before_trim'],c['candidates_after_trim']],'stage counts')
    require(all(len(v)<=ROWS and len(v)==len(set(v)) and all(type(i) is int for i in v) for v in [selected,pre,kept]),'bounded unique stage IDs')
    require(all(i in leaf_sets for i in selected) and all(0<=i<len(id_map) for i in pre+kept),'stage IDs')
    require(set(kept)<=set(pre),'trim outside pretrim');gt=set(gt_ids);covered=set().union(*(leaf_sets[i]&gt for i in selected));pre_gt={id_map[i] for i in pre}&gt;kept_gt={id_map[i] for i in kept}&gt;returned=set(reply['ids'])&gt
    require(covered==pre_gt and returned<=kept_gt<=pre_gt and set(reply['ids'])<={id_map[i] for i in kept},'stage ownership/coverage join')
    return dict(selected_leaf_gt_hits=len(covered),pretrim_gt_hits=len(pre_gt),retained_gt_hits=len(kept_gt),returned_gt_hits=len(returned),navigation_loss=100-len(covered),trim_loss=len(pre_gt)-len(kept_gt),exact_loss=len(kept_gt)-len(returned))


def validate_reply(reply,request,gt,leaf_sets,id_map,saved_internals):
    require('error' not in reply and reply['request_id']==request['request_id'] and reply['generation']==1 and reply['shard_id']=='global','query response binding')
    require(reply['query_work_policy']=='ordinary-query-work-v1' and reply['query_navigation_policy']=='four-bit' and reply['provider_cache_policy']==CACHE,'query policy')
    w=reply['query_work'];summary=validate_query_work_counts(w['counts'],saved_internal_nodes=saved_internals,require_trace=True);c=summary['scalar_counts'];ids=reply['ids']
    require(len(ids)==len(set(ids)) and all(type(i) is int and 0<=i<ROWS for i in ids),'returned IDs')
    require(len(reply['distances'])==len(ids) and all(math.isfinite(v) for v in reply['distances']),'finite distances')
    require(w['returned_distance_bits']==[struct.unpack('<I',struct.pack('<f',v))[0] for v in reply['distances']],'returned bits')
    require(c['posting_rows_scored_before_dedup']>=c['distinct_candidates_before_trim']>=c['candidates_after_trim']==min(c['distinct_candidates_before_trim'],200),'score/trim counts')
    require(c['exact_embedding_gets']==c['exact_vectors_reranked']==c['candidates_after_trim'] and w['returned_results']==len(ids)==min(100,c['exact_vectors_reranked']),'rerank counts')
    require(w['selected_leaves']==c['selected_leaves'] and c['nonempty_leaves_scored']<=c['selected_leaves'],'leaf counts')
    obs=reply['vector_pin_observation'];require(obs is not None and obs['access_overflow'] is False,'vector observation incomplete')
    keys=['reader_requests','pin_hits','pin_misses','provider_obtains','provider_hits','provider_misses','storage_callbacks','encoded_bytes','decode_attempts','decode_successes'];io={key:sum(v[key] for v in obs['block_access'].values()) for key in keys}
    joined=join_stages(reply,gt['ids'],leaf_sets,id_map)
    return dict(query_id=gt['query_id'],recall_at_100=joined['returned_gt_hits']/100,stage_coverage=joined,structural=summary,counts=c,elapsed_ms=reply['elapsed_ms'],vector_io=io,returned_ids=ids,returned_distance_bits=w['returned_distance_bits'])


def query_arm(binary,arm,pinned,mp,reports,queries,gt,out,remaining,env):
    leaf_sets={};census=reports[0]['owners'][0]['census'];saved_internals=census['reachable_nodes']-census['reachable_leaves']
    for report in reports:
        require(report['owners'][0]['census']['reachable_nodes']==census['reachable_nodes'] and report['owners'][0]['census']['reachable_leaves']==census['reachable_leaves'],'saved census shape changed')
        for leaf in report['owners'][0]['census']['leaves']:leaf_sets.setdefault(leaf['leaf_id'],set()).update(leaf['gt_ids'])
    raw=verify(mp).read_bytes();id_map=struct.unpack('<'+str(ROWS)+'Q',raw);require(id_map==tuple(range(ROWS)),'dense identity map')
    requests=[dict(request_id=phase+':'+q['query_id'],generation=1,op='query',query=q['vector'],k=100,beam_min=32,beam_max=32,rerank_factor=2) for phase in ['warmup','measured'] for q in queries]
    path=out/(arm+'-requests.jsonl');path.write_text(''.join(json.dumps(v)+'\n' for v in requests))
    command=[str(binary),'read','--checkpoint-dir',str(Path(pinned['path']).parent),'--profile-query-work','--profile-rerank-pins','--trace-query-stages','--query-navigation','four-bit','--provider-cache-policy',json.dumps(CACHE)]
    started=time.monotonic();cpu=resource.getrusage(resource.RUSAGE_CHILDREN)
    with path.open('rb') as stdin,(out/(arm+'-query.stdout')).open('xb') as stdout,(out/(arm+'-query.stderr')).open('xb') as stderr:
        try:p=subprocess.run(command,stdin=stdin,stdout=stdout,stderr=stderr,env=env,timeout=remaining)
        except subprocess.TimeoutExpired:write(out/(arm+'-query-process.json'),dict(command=command,status='timeout'));raise
    after=resource.getrusage(resource.RUSAGE_CHILDREN);write(out/(arm+'-query-process.json'),dict(command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-started,user_cpu_seconds=after.ru_utime-cpu.ru_utime,system_cpu_seconds=after.ru_stime-cpu.ru_stime,voluntary_switches=after.ru_nvcsw-cpu.ru_nvcsw,involuntary_switches=after.ru_nivcsw-cpu.ru_nivcsw,observed_child_maxrss_kib=child_peak_kib(after)));require(p.returncode==0,'query reader failed');require(child_peak_kib(after)<=8*1024**2,'observed8GiB RSS ceiling')
    replies=[json.loads(v) for v in (out/(arm+'-query.stdout')).read_text().splitlines()];require(len(replies)==200,'query response count');cache=replies[0]['compressed_navigation_cache'];manifest=json.loads(verify(pinned).read_text())
    require(cache['nodes']==census['reachable_nodes'] and cache['nodes']<=256 and cache['policy']=='four-bit' and cache['dimension']==DIM and cache['retained_code_payload_bytes']==cache['nodes']*396,'Code4 payload/cap')
    require(cache['temporary_owned_fp_payload_bytes']==cache['nodes']*DIM*4 and cache['fp_source_load']['vector_access']['access_overflow'] is False,'FP preload coverage')
    snapshot={k:manifest[k] for k in ['generation','source_id','dimension','metric','root_ref']}
    require(all(v['compressed_navigation_cache']==cache and v['query_navigation_snapshot']==snapshot for v in replies),'cache generation binding')
    validated=[validate_reply(v,requests[i],gt[queries[i%100]['query_id']],leaf_sets,id_map,saved_internals) for i,v in enumerate(replies)]
    measured=validated[100:];passed=sum(v['structural']['internal_pruning'] for v in measured)
    return dict(arm=arm,structural_pass_queries=passed,structural_gate_pass=passed>=80,mean_recall_at_100=statistics.mean(v['recall_at_100'] for v in measured),median_trace_warm_elapsed_ms=statistics.median(v['elapsed_ms'] for v in measured),compressed_navigation_cache=cache,reader_open_seconds=replies[0]['query_reader_open_seconds'],measured=measured,scope='Exploratory reused100;trace overhead/cache warmth retained; equalperlevelbeam notmatchedrecall. Savedglobalreader is not actor/networkfanout.')


def run(s):
    require(sys.platform=='linux','Linux runtime only');template=json.loads((HERE/'runtime-spec.template.json').read_text())
    mutable={'binary','deadline_unix','output','source'};require({k:v for k,v in s.items() if k not in mutable}=={k:v for k,v in template.items() if k not in mutable},'fixed plan changed')
    require(type(s['deadline_unix']) in (int,float) and math.isfinite(s['deadline_unix']) and time.time()<s['deadline_unix'],'fixed external deadline')
    binary=verify(s['binary']);restore=json.loads(verify(s['restore_receipt']).read_text());require(restore['status']=='complete' and restore['canonical']==s['vectors'] and restore['rows']==2000000 and restore['dimension']==DIM,'restore binding')
    source=Path(s['vectors']['path']);require(source.stat().st_size==6144000000,'source extent');require(descriptor(HERE/'queries.jsonl')['sha256']==QUERY_SHA,'frozen queries')
    queries=check_queries(HERE/'queries.jsonl',s['query_ids']);tree=Path(s['source']);sp=importlib.util.spec_from_file_location('build_recipe',HERE/'compile-native.py');mod=importlib.util.module_from_spec(sp);sp.loader.exec_module(mod);manifest=json.loads((HERE/'source-manifest.json').read_text());mod.verify(tree,manifest)
    require(shutil.disk_usage(tree).free>=20*1024**3,'20GiB free floor');out=Path(s['output']);out.mkdir(exist_ok=False);started=time.monotonic();results={};quality_reports={};query_results={};bindings={};failure=None;structural_failure=False
    env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1')
    def remaining(cap,end):
        require(shutil.disk_usage(out).free>=20*1024**3,'20GiB free floor')
        value=min(cap,end-time.monotonic(),s['deadline_unix']-time.time(),780-(time.monotonic()-started));require(value>0,'fixed phase/scientific deadline');return value
    try:
        path=out/'setup-spec.json';write(path,dict(source=str(source),source_tree=str(tree),output=str(out),query_ids=s['query_ids']))
        child([sys.executable,str(Path(__file__).resolve()),'--prepare',str(path)],'setup',out,remaining(60,started+60),env)
        prep=json.loads((out/'preparation.json').read_text());require(prep['prefix_copy_bytes']==ROWS*DIM*4 and prep['rows']==ROWS and prep['queries']['sha256']==QUERY_SHA,'prefix/GT binding')
        gt={v['query_id']:v for v in [json.loads(line) for line in verify(prep['ground_truth']).read_text().splitlines()]};construction_end=time.monotonic()+540
        for arm in ARMS:
            owner=arm!='reference';native=dict(protocol='reference-owner-parallel-v1',source_id='retained-matched2m-first16384',vectors=s['vectors'],prefix_sha256=prep['prefix_sha256'],rows=ROWS,dimension=DIM,metric='ip',config=s['config'],treatment='owner' if owner else 'reference',predicate_batching=owner,construction_observation=True,skip_placement=arm=='dynamic1',direct_current_reads=False,batched_scrub_validation=owner,publish_checkpoint=True,threads=4,balance_every=4096,owner_slots=1 if arm=='dynamic1' else 4,move_above=4096,output=str(out/arm))
            path=out/(arm+'-spec.json');write(path,native);value=child([str(binary),'reference-owner-diagnostic','--spec',str(path)],arm,out,remaining(180,construction_end),env)
            require(value==json.loads((out/arm/'result.json').read_text()) and value['spec']==native and value['spec_sha256']==descriptor(path)['sha256'],'native output binding');shape(value,arm);results[arm]=value;bindings[arm]=checkpoint(value,native)
            require(child_peak_kib(resource.getrusage(resource.RUSAGE_CHILDREN))<=8*1024**2,'observed8GiB RSS ceiling')
        require(all(len(v['all_id_exact_fp_distance_bits'])==ROWS and v['all_id_exact_fp_distance_bits']==results['reference']['all_id_exact_fp_distance_bits'] for v in results.values()),'saved exact vector bits differ')
        diagnosis_end=time.monotonic()+90
        for arm in ARMS:
            pinned,mp=bindings[arm];reports=[]
            for start in range(0,100,16):
                ids=s['query_ids'][start:start+16];ds=diagnostic_spec(results[arm]['spec'],pinned,prep,ids);path=out/(arm+'-diagnostic-'+str(start)+'.json');write(path,ds)
                report=child([str(binary),'diagnose-ground-truth','--spec',str(path)],arm+'-diagnostic-'+str(start),out,remaining(30,diagnosis_end),env);validate_quality(report,ds,path,gt);reports.append(report)
            quality_reports[arm]=reports;require(descriptor(pinned['path'])==pinned and descriptor(mp['path'])==mp,'original checkpoint changed')
        query_end=time.monotonic()+90
        for arm in ARMS:
            pinned,mp=bindings[arm];query_results[arm]=query_arm(binary,arm,pinned,mp,quality_reports[arm],queries,gt,out,remaining(30,query_end),env)
            require(descriptor(pinned['path'])==pinned and descriptor(mp['path'])==mp,'original checkpoint changed')
        structural_failure=not all(v['structural_gate_pass'] for v in query_results.values())
        mod.verify(tree,manifest)
    except Exception as error:failure=repr(error)
    finally:
        try:mod.verify(tree,manifest)
        except Exception as source_error:failure='final source verification: '+repr(source_error)
        files=[p for p in out.rglob('*') if p.is_file()];prefix=out/'evaluation-prefix.f32';stores={arm:sum(p.stat().st_size for p in files if p.is_relative_to(out/arm/'store')) for arm in ARMS};metadata=sum(p.stat().st_size for p in files if p!=prefix and not any(p.is_relative_to(out/arm/'store') for arm in ARMS))
        if metadata>128*1024**2 or any(v>512*1024**2 for v in stores.values()):failure='observed output cap exceeded'
        result=dict(status='failed' if failure else 'structurally-insufficient' if structural_failure else 'passed',error=failure,effective_construction_config=dict(s['config'],max_replicas=1,write_level_taus=[],write_level_min_pcts=[]),query_policy=dict(centroid='four-bit',posting='one-bit',exact_rerank='FP32',k=100,rerank_factor=2,tau=2.0,per_level_min=32,per_level_max=32,final_leaf_truncation=False),elapsed_seconds=time.monotonic()-started,results=results,quality_reports=quality_reports,query_results=query_results,bindings=bindings,metadata_bytes=metadata,retained_store_bytes=stores,prefix_copy_bytes=prefix.stat().st_size if prefix.exists() else 0,scope='Independent stochastic trees/uncontrolled split RNG and thread scheduler; no causal speedup, no physicalIO/actorfanout/distributedmemory. All100retained queries reused/disjointnessunknown;knownID/exactvectoroverlapreject. One Code4perlevelbeam32/tau2.0 policy;samebeamnotmatchedrecall. Structural failure stops without automatic growth/retry/tuning. Directchild timeouts plus required external guard; cumulative observedRSSnotallocationbound. Full ownercache/joinedsave/globalmetadata and bootstrap/map/audit/oracle costs charged.')
        projected=metadata+len(json.dumps(result,indent=2).encode())+1
        if projected>128*1024**2:result.update(status='failed',error='metadata including final result exceeds128MiB')
        result['metadata_including_result_estimate_bytes']=projected
        write(out/'result.json',result)
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--spec');parser.add_argument('--prepare');a=parser.parse_args()
    if a.prepare:
        require(sys.platform=='linux','Linux setup only');s=json.loads(Path(a.prepare).read_text());v=prepare(s['source'],Path(s['output']),Path(s['source_tree']),HERE/'queries.jsonl',s['query_ids']);print(json.dumps(v))
    else:
        require(a.spec is not None,'spec required');v=run(json.loads(Path(a.spec).read_text()));print(json.dumps(v));raise SystemExit(v['status']!='passed')
