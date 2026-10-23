"""Calibration-only score/cutoff evidence. No lifecycle, evaluation or policy change."""
import argparse,hashlib,json,math,os,resource,shutil,struct,subprocess,sys,time
from pathlib import Path
from harness_common import require,descriptor,verify,write
from quality_analysis import CACHE,cohort_ids,validate_gt,validate_reply,vector_traffic
HERE=Path(__file__).resolve().parent

def bits(v):return struct.unpack('<I',struct.pack('<f',v))[0]
def number(v):return struct.unpack('<f',struct.pack('<I',v))[0]
def integer(v):return type(v) is int and 0<=v<=0xffffffff

def score_trace(reply):
    t=reply['query_stage_trace'];require(t['complete'] is True and t['id_cap']==16384,'complete trace binding')
    rows=t['pretrim_candidate_score_bits'];require(len(rows)<=16384,'score trace bound')
    require(all(type(v) is list and len(v)==2 and all(integer(z) for z in v) and math.isfinite(number(v[1])) for v in rows),'score bits shape')
    scores={i:number(v) for i,v in rows};require(len(scores)==len(rows) and set(scores)==set(t['pretrim_candidate_ids']),'complete unique pretrim scores')
    kept=t['retained_candidate_ids'];require(len(kept)==len(set(kept))==min(400,len(scores)) and set(kept)<=scores.keys(),'retained count')
    if len(scores)>400:
        require(integer(t['cutoff_score_bits']),'cutoff bits');cut=number(t['cutoff_score_bits']);require(math.isfinite(cut),'finite cutoff')
        require(cut==max(scores[i] for i in kept),'retained cutoff max')
        require(all(scores[i]>=cut for i in scores.keys()-set(kept)),'selection boundary')
        require(type(t['cutoff_tie_candidates']) is int and type(t['cutoff_tie_retained']) is int,'tie counts')
        require(t['cutoff_tie_candidates']==sum(v==cut for v in scores.values()) and t['cutoff_tie_retained']==sum(scores[i]==cut for i in kept),'ties partition')
    else:require(t['cutoff_score_bits'] is None,'no cutoff without trim')
    return scores

def inventory(store,deadline):
    files={};total=0
    for p in sorted(store.rglob('*')):
        require(time.time()<deadline,'inventory deadline')
        if not p.is_file():continue
        require(not p.is_symlink() and len(files)<10000,'inventory cap/symlink')
        total+=p.stat().st_size;require(total<=512*1024**2,'inventory byte cap')
        files[str(p.relative_to(store))]=descriptor(p)
    require(files,'empty store');return dict(files=files,logical_hash_read_bytes=total)

def run_reader(binary,store,requests,out,stem,deadline):
    require(0<len(requests)<=32,'request count');inp=out/(stem+'-requests.jsonl');inp.write_text(''.join(json.dumps(v)+'\n' for v in requests))
    cmd=[str(binary),'read','--checkpoint-dir',str(store),'--query-navigation','fp','--profile-query-work','--profile-rerank-pins','--provider-cache-policy',json.dumps(CACHE)]
    if stem=='query':cmd+=['--trace-query-stages']
    env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4');before=resource.getrusage(resource.RUSAGE_CHILDREN);start=time.monotonic()
    try:
        with inp.open('rb') as stdin,(out/(stem+'.stdout')).open('xb') as stdout,(out/(stem+'.stderr')).open('xb') as stderr:
            p=subprocess.run(cmd,stdin=stdin,stdout=stdout,stderr=stderr,env=env,timeout=min(20,deadline-time.time()))
    except BaseException as e:
        write(out/(stem+'-process.json'),dict(command=cmd,error=repr(e),elapsed_seconds=time.monotonic()-start));raise
    after=resource.getrusage(resource.RUSAGE_CHILDREN);write(out/(stem+'-process.json'),dict(command=cmd,returncode=p.returncode,elapsed_seconds=time.monotonic()-start,user_cpu_seconds=after.ru_utime-before.ru_utime,system_cpu_seconds=after.ru_stime-before.ru_stime,voluntary_switches=after.ru_nvcsw-before.ru_nvcsw,observed_cumulative_child_maxrss_kib=after.ru_maxrss))
    require(p.returncode==0 and after.ru_maxrss<=8*1024**2,'reader exit/RSS')
    raw=[json.loads(v) for v in (out/(stem+'.stdout')).read_text().splitlines()];require(len(raw)==len(requests),'reply count')
    require(all(not v.get('error') and v['request_id']==q['request_id'] and v['generation']==1 for v,q in zip(raw,requests)),'reader response binding')
    require(time.monotonic()-start<=20,'reader time cap');return raw

def run(spec):
    require(sys.platform=='linux','Linux runtime only');template=json.loads((HERE/'runtime-spec.template.json').read_text())
    require(set(spec)==set(template) and all(spec[k]==v for k,v in template.items() if k not in ['deadline_unix','output']),'fixed spec differs')
    require(type(spec['deadline_unix']) in (int,float) and 180<spec['deadline_unix']-time.time()<=1200,'fixed external deadline/reserve')
    for name,pin in json.loads((HERE/'package-manifest.json').read_text())['files'].items():require(descriptor(HERE/name)['sha256']==pin['sha256'] and (HERE/name).stat().st_size==pin['bytes'],'package changed')
    build=json.loads(Path(spec['build_receipt']).read_text());require(build['returncode']==0 and build['source_manifest_sha256']==descriptor(HERE/'source-manifest.json')['sha256'] and build['compiler_sha256']=='028fd60b0e0add5505c661cd3ccde91393d615c8729bd95d94ab1e91409b36a1','source/build binding');binary=verify(build['binary']);require(str(binary)==spec['binary'],'binary path')
    require(shutil.disk_usage(binary).free>=20*1024**3,'disk floor')
    out=Path(spec['output']);out.mkdir(exist_ok=False);start=time.monotonic();science_deadline=min(time.time()+120,spec['deadline_unix']-180)
    before=None;result=dict(status='failed',evaluation_accessed=False,policy_changed=False)
    try:
        snapshot=spec['snapshot'];store=verify(snapshot['manifest']).parent;verify(snapshot['mapping']);manifest=json.loads((store/'manifest.json').read_text());require(manifest['generation']==1 and manifest['vector_count']==16384 and manifest['dimension']==768 and manifest['metric']=='ip','snapshot metric/dimension')
        queries=[json.loads(v) for v in verify(spec['queries']).read_text().splitlines()];gt=[json.loads(v) for v in verify(spec['ground_truth']).read_text().splitlines()];expected=[f'canonical-doc:{i}' for i in cohort_ids()[::9]];require([v['query_id'] for v in queries]==expected,'calibration-only IDs');validate_gt(gt,expected,16384,100)
        prior=[json.loads(v) for v in verify(spec['prior_raw']).read_text().splitlines()];require(len(prior)==64,'prior warm+retained count');prior=prior[32:];old=json.loads(verify(spec['prior_result']).read_text());require(old['outcome']=='amended-calibration-target-or-pruning-not-met' and old['evaluation']=={},'negative lineage')
        before=inventory(store,min(time.time()+20,science_deadline));write(out/'snapshot-before.json',before);snapshot=dict(manifest,**snapshot,_store=str(store))
        requests=[dict(request_id='fidelity:'+v['query_id'],generation=1,op='query',query=v['vector'],k=100,beam_min=38,beam_max=38,rerank_factor=4) for v in queries];raw=run_reader(binary,store,requests,out,'query',science_deadline)
        exact_requests=[];rows=[]
        for q,request,g,reply,oldreply in zip(queries,requests,gt,raw,prior):
            measured=validate_reply(reply,request,g,snapshot,snapshot['census']);scores=score_trace(reply);t=reply['query_stage_trace'];oldtrace=oldreply['query_stage_trace'];require(t['selected_leaf_ids']==oldtrace['selected_leaf_ids'] and set(t['pretrim_candidate_ids'])==set(oldtrace['pretrim_candidate_ids']),'navigation/current candidate decisions changed')
            kept=set(t['retained_candidate_ids']);changed=kept^set(oldtrace['retained_candidate_ids']);require(not changed or all(scores[i]==number(t['cutoff_score_bits']) for i in changed),'prior kept change outside equal-score cutoff');lost=sorted((set(g['ids'])&scores.keys())-kept)
            rows.append(dict(query_id=q['query_id'],lost_gt_ids=lost,prior_kept_equal=kept==set(oldtrace['retained_candidate_ids']),prior_returned_equal=(reply['ids']==oldreply['ids'] and reply['query_work']['returned_distance_bits']==oldreply['query_work']['returned_distance_bits']),cutoff_bits=t['cutoff_score_bits'],cutoff_tie_candidates=t['cutoff_tie_candidates'],cutoff_tie_retained=t['cutoff_tie_retained'],metrics=measured))
            if lost:
                ids=sorted(kept|set(lost));require(len(ids)<=500,'targeted exact cap');exact_requests.append(dict(request_id='exact:'+q['query_id'],generation=1,op='rerank_selected',query=q['vector'],local_ids=ids))
        exact=run_reader(binary,store,exact_requests,out,'targeted-exact',science_deadline) if exact_requests else [];by_id={v['query_id']:v for v in rows};raw_by_id={q['query_id']:v for q,v in zip(queries,raw)};gt_by_id={v['query_id']:v for v in gt}
        for request,reply in zip(exact_requests,exact):
            qid=request['request_id'][6:];r=by_id[qid];scores=score_trace(raw_by_id[qid]);hits=reply['results'];require(reply['exact_vectors_scored']==len(hits)==len(request['local_ids']) and {v['id'] for v in hits}==set(request['local_ids']),'targeted exact completeness');require(all(math.isfinite(v['distance']) for v in hits),'finite exact distances');ordered=sorted(hits,key=lambda v:((~bits(v['distance'])&0xffffffff) if bits(v['distance'])&0x80000000 else bits(v['distance'])^0x80000000,v['id']));rank={v['id']:i+1 for i,v in enumerate(ordered)};cut=number(r['cutoff_bits']);r['lost_score_fidelity']=[dict(id=i,approx_bits=bits(scores[i]),approx_distance=scores[i],cutoff_numeric_tie=scores[i]==cut,approx_strictly_better_count=sum(v<scores[i] for v in scores.values()),exact_distance=next(v['distance'] for v in hits if v['id']==i),exact_bits=bits(next(v['distance'] for v in hits if v['id']==i)),exact_rank_in_kept_plus_lost=rank[i]) for i in r['lost_gt_ids']];exact_dist={v['id']:v['distance'] for v in hits};oldhits=raw_by_id[qid];require(all(bits(exact_dist[i])==v for i,v in zip(oldhits['ids'],oldhits['query_work']['returned_distance_bits'])),'exact distance consistency');r['targeted_exact_vector_io']=vector_traffic(reply['vector_pin_observation'],len(hits));r['targeted_exact_top100_gt_hits']=len({v['id'] for v in ordered[:100]}&set(gt_by_id[qid]['ids']))
        write(out/'calibration-gt.jsonl.json',gt);result.update(status='completed',rows=rows,logical_targeted_vector_bytes=sum(len(v['local_ids'])*768*4 for v in exact_requests),targeted_exact_requests=len(exact_requests),targeted_exact_rows=sum(len(v['local_ids']) for v in exact_requests),scope='Calibration-only fidelity evidence; exact requests occur in a separate process outside query timing. Kept-plus-lost rank is not an all-candidate exact ranking or policy change. Timing perturbed; encoded returns not physical IO/network.')
    except BaseException as e:result['error']=repr(e)
    finally:
        if before is not None:
            try:
                after=inventory(store,min(time.time()+30,spec['deadline_unix']-10));write(out/'snapshot-after.json',after);require(before==after,'old saved bytes changed');verify(build['binary'])
            except BaseException as e:result.update(status='failed',preservation_error=repr(e))
        result['elapsed_seconds']=time.monotonic()-start
        if result['elapsed_seconds']>150:result.update(status='failed',deadline_error='120 scientific +30 preservation exceeded')
        # Native/request outputs are retained remotely, but vector-bearing requests are never retrieved.
        size=sum(p.stat().st_size for p in out.rglob('*') if p.is_file());payload=json.dumps(result,indent=2)+'\n';require(size+len(payload.encode())<=64*1024**2,'metadata cap');(out/'result.json').write_text(payload)
    return 0 if result['status']=='completed' else 1

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--spec',required=True);a=parser.parse_args();raise SystemExit(run(json.loads(Path(a.spec).read_text())))
