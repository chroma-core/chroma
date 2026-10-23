"""Fixed observed-quality cost description; no build, inference tuning or lifecycle."""
import argparse,hashlib,json,math,os,resource,statistics,sys,time,signal
from pathlib import Path
from harness_common import require,verify,descriptor,write
from persistent_reader import Reader
from quality_analysis import ARMS,CACHE,cohort_ids,vector_traffic
from navigation_work import validate_query_work_counts
from admission import input_snapshot,overlay_closure,write_final_result
from construction_support import child_peak_kib
HERE=Path(__file__).resolve().parent
PASSES=('warmup','retained1','retained2','retained3')
BEAMS={'reference':36,'dynamic1':38,'dynamic4':36}
THREAD_ENV={'TOKIO_WORKER_THREADS':'4','RAYON_NUM_THREADS':'4','OPENBLAS_NUM_THREADS':'1','OMP_NUM_THREADS':'1','MKL_NUM_THREADS':'1'}

def freeze_threads():
    os.environ.update(THREAD_ENV)
    return {k:os.environ[k] for k in THREAD_ENV}



def load(path):return json.loads(Path(path).read_text())


def seal(path,value):
    with Path(path).open('x') as f:
        json.dump(value,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    fd=os.open(Path(path).parent,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)
    return descriptor(path)


def verify_package():
    for name,pin in load(HERE/'package-manifest.json')['files'].items():
        p=HERE/name;require(p.is_file() and not p.is_symlink() and p.stat().st_size==pin['bytes'] and hashlib.sha256(p.read_bytes()).hexdigest()==pin['sha256'],'package '+name)


def check_inference(value):
    require(value['outcome']=='target-not-met' and value['queries']==256,'failed absolute target remains explicit')
    for arm in ARMS:require(value['intervals'][arm]['mean']<.95,'frozen observed recall')
    for arm in ARMS[1:]:
        interval=value['intervals'][arm+'-reference'];require(-.01<interval['lower']<=interval['upper']<.01,'conditional relative component')


def admit(spec,deadline):
    """Hash and validate real immutable artifacts before any reader starts."""
    binary=verify(spec['binary']);build=load(verify(spec['build']))
    require(build['source_manifest_sha256']==spec['source_manifest_sha256'] and build['binary']==spec['binary'],'build source/binary')
    selection=load(verify(spec['selection']));lock=load(verify(spec['selection_lock']))
    require(lock==spec['selection'],'exact original policy lock')
    require(selection['snapshots']==spec['snapshots'] and selection['overlays']==spec['overlays'],'original snapshots/overlays')
    inference=HERE/'recovered-inference.json';require(hashlib.sha256(inference.read_bytes()).hexdigest()==spec['inference_sha256'],'recovered inference hash');check_inference(load(inference))
    manifests={};before={};baselines={}
    for arm in ARMS:
        manifest,before[arm]=input_snapshot(spec['snapshots'][arm],deadline);manifests[arm]=manifest
        overlay=spec['overlays'][arm];require(overlay_closure(Path(overlay['manifest']['path']).parent,spec['snapshots'][arm])==overlay,'locked overlay closure')
        prior=load(verify(spec['baseline'][arm]));require(prior['arm']==arm and len(prior['measured'])==256,'prior evaluation identity/count');baselines[arm]=prior
    queries=[json.loads(line) for line in verify(spec['queries']).read_text().splitlines()]
    ids=['canonical-doc:'+str(v) for j,v in enumerate(cohort_ids()) if j%9!=0]
    require([q['query_id'] for q in queries]==ids,'frozen 256-query order')
    for q in queries:require(len(q['vector'])==768 and all(type(x) in (int,float) and math.isfinite(x) for x in q['vector']),'finite 768D query')
    for arm in ARMS:require([q['query_id'] for q in baselines[arm]['measured']]==ids,'baseline query order')
    return binary,manifests,before,baselines,queries


def command(binary,snapshot,overlay):
    return [str(binary),'read','--checkpoint-dir',str(Path(snapshot['manifest']['path']).parent),'--profile-query-work','--profile-rerank-pins','--query-navigation','fp','--provider-cache-policy',json.dumps(CACHE),'--posting-code4-overlay-dir',str(Path(overlay['manifest']['path']).parent)]


def validate_response(reply,request,prior,snapshot,overlay):
    require('error' not in reply and reply['request_id']==request['request_id'] and reply['generation']==1 and reply['shard_id']=='global','response identity')
    require(reply['query_navigation_policy']=='fp' and reply['query_work_policy']=='ordinary-query-work-v1' and reply['provider_cache_policy']==CACHE,'unchanged query policies')
    require(reply['posting_score_policy']=='code4-residual-postings-v1' and reply['candidate_budget']==400 and reply['rerank_factor']==4,'400 exact reranks then 100 returns')
    identity={k:snapshot[k] for k in ['generation','source_id','dimension','metric','root_ref']};require(reply['query_navigation_snapshot']==identity,'saved generation')
    require(reply.get('query_stage_trace') is None,'no stage/score tracing in timed cost run')
    summary=validate_query_work_counts(reply['query_work']['counts'],saved_internal_nodes=snapshot['integrity_audit']['reachable_nodes']-snapshot['integrity_audit']['reachable_leaves'],require_trace=False)
    require(summary['scalar_counts']==prior['counts'],'same scalar algorithm work')
    require(reply['ids']==prior['returned_ids'] and reply['query_work']['returned_distance_bits']==prior['returned_distance_bits'],'same IDs and FP32 distance bits')
    report=reply['posting_overlay_report'];require(report['created'] is False and report['embedding_point_gets']==0 and report['manifest_sha256']==overlay['manifest']['sha256'],'load original immutable overlay')
    io=vector_traffic(reply['vector_pin_observation'],prior['counts']['exact_embedding_gets'])
    require(type(reply['elapsed_ms']) in (int,float) and math.isfinite(reply['elapsed_ms']) and reply['elapsed_ms']>=0,'native elapsed')
    return dict(native_ms=reply['elapsed_ms'],counts=summary['scalar_counts'],vector_io=io)


def distribution(values):
    require(values and all(type(x) in (int,float) and math.isfinite(x) and x>=0 for x in values),'finite cost values')
    ordered=sorted(values)
    def quantile(p):
        t=(len(ordered)-1)*p;i=int(t);return ordered[i]+(ordered[min(i+1,len(ordered)-1)]-ordered[i])*(t-i)
    return dict(count=len(values),mean=statistics.mean(values),median=statistics.median(values),p90=quantile(.90),p95=quantile(.95))


def summarize(rows,process):
    """Summarize three within-query means; repeats are not independent queries."""
    require(len(rows)==1024 and [r['phase'] for r in rows]==[p for p in PASSES for _ in range(256)],'fixed warmup and three retained passes')
    require(all([r['query_id'] for r in rows[j*256:(j+1)*256]]==[r['query_id'] for r in rows[:256]] for j in range(4)),'same order each pass')
    byquery=[]
    for i in range(256):
        group=[rows[j*256+i] for j in (1,2,3)]
        byquery.append(dict(query_id=group[0]['query_id'],native_ms=statistics.mean(r['native_ms'] for r in group),client_ms=statistics.mean(r['client_ms'] for r in group)))
    cpu=process['user_cpu_seconds']+process['system_cpu_seconds'];elapsed=process['elapsed_seconds'];require(elapsed>0,'whole-child elapsed denominator')
    retained=rows[256:];available=all(r['vector_io']['available'] for r in retained)
    traffic=dict(available=available)
    if available:traffic['aggregate']={k:sum(r['vector_io']['aggregate'][k] for r in retained) for k in retained[0]['vector_io']['aggregate']}
    return dict(native_query_means_ms=distribution([r['native_ms'] for r in byquery]),client_query_means_ms=distribution([r['client_ms'] for r in byquery]),per_query=byquery,first_warmup_client_ms=rows[0]['client_ms'],whole_child_cpu_seconds=cpu,whole_child_elapsed_seconds=elapsed,whole_child_cpu_per_elapsed=cpu/elapsed,retained_provider_traffic=traffic,scope='Three repeated measurements are averaged within each fixed query. Whole-child CPU includes startup, warmup and all retained passes; it is not per-query CPU. Encoded provider returns are not physical IO or network.')


def run_arm(cmd,arm,snapshot,overlay,queries,baseline,out,timeout):
    started=time.monotonic();end=started+timeout;before=resource.getrusage(resource.RUSAGE_CHILDREN);reader=Reader(cmd,timeout);rows=[];startup=None;failure=None
    try:
        with (out/(arm+'-derived-replies.jsonl')).open('x') as raw:
            for phase in PASSES:
                for i,q in enumerate(queries):
                    require(time.monotonic()<end,'30-second arm deadline')
                    request=dict(request_id=phase+':'+q['query_id'],op='query',generation=1,query=q['vector'],k=100,beam_min=BEAMS[arm],beam_max=BEAMS[arm],rerank_factor=4)
                    metrics={};t=time.monotonic();reply=reader.query(request,timeout=end-time.monotonic(),metrics=metrics);client_ms=(time.monotonic()-t)*1000
                    row=validate_response(reply,request,baseline['measured'][i],snapshot,overlay);row.update(phase=phase,query_id=q['query_id'],client_ms=client_ms,pipe=metrics);rows.append(row)
                    if startup is None:startup={k:reply[k] for k in ['query_reader_open_seconds','fp_navigation_cache','posting_overlay_report']}
                    else:require(startup=={k:reply[k] for k in startup},'one stable startup report')
                    raw.write(json.dumps(reply,separators=(',',':'))+'\n')
                    require(raw.tell()<=150*1024**2,'per-arm derived reply cap')
        reader.process.stdin.close();reader.process.wait(timeout=max(.001,end-time.monotonic()));require(reader.process.returncode==0,'reader nonzero exit')
    except Exception as error:
        failure=repr(error);raise
    finally:
        reader.close()
        if failure is not None:
            usage=resource.getrusage(resource.RUSAGE_CHILDREN)
            write(out/(arm+'-failure.json'),dict(error=failure,command=cmd,completed_requests=len(rows),elapsed_seconds=time.monotonic()-started,returncode=reader.process.returncode,user_cpu_seconds=usage.ru_utime-before.ru_utime,system_cpu_seconds=usage.ru_stime-before.ru_stime))
    elapsed=time.monotonic()-started;after=resource.getrusage(resource.RUSAGE_CHILDREN)
    process=dict(command=cmd,returncode=reader.process.returncode,elapsed_seconds=elapsed,user_cpu_seconds=after.ru_utime-before.ru_utime,system_cpu_seconds=after.ru_stime-before.ru_stime,voluntary_switches=after.ru_nvcsw-before.ru_nvcsw,involuntary_switches=after.ru_nivcsw-before.ru_nivcsw,observed_cumulative_child_maxrss_kib=child_peak_kib(after))
    require(elapsed<=timeout and process['observed_cumulative_child_maxrss_kib']<=8*1024**2,'arm time/observed RSS bound')
    result=dict(arm_construction_provenance=arm,rows=rows,startup=startup,process=process,summary=summarize(rows,process),derived_replies=descriptor(out/(arm+'-derived-replies.jsonl')))
    write(out/(arm+'-result.json'),result);return result


def run(spec):
    require(sys.platform.startswith('linux'),'actual runtime is Linux only');verify_package()
    template=load(HERE/'runtime-spec.template.json');require({k:v for k,v in spec.items() if k not in ('deadline_unix','output')}=={k:v for k,v in template.items() if k not in ('deadline_unix','output')},'frozen spec')
    require(type(spec['deadline_unix']) in (int,float) and 180<spec['deadline_unix']-time.time()<=900,'fixed external window with shutdown reserve')
    out=Path(spec['output']);require(out.is_absolute() and str(out).startswith('/mnt/spann/code4-fixed-policy-query-cost-v1/') and not out.exists(),'fresh bounded output');out.mkdir(parents=True)
    started=time.monotonic();end=min(started+120,started+spec['deadline_unix']-time.time()-180);result=dict(status='failed',protocol=spec['protocol'])
    try:
        signal.signal(signal.SIGALRM,lambda *_: (_ for _ in ()).throw(TimeoutError('120-second scientific alarm')));signal.alarm(max(1,math.ceil(end-time.monotonic())))
        scientific_deadline=time.time()+end-time.monotonic()
        thread_environment=freeze_threads()
        binary,manifests,before,baselines,queries=admit(spec,scientific_deadline)
        plan=dict(spec=spec,arm_order=list(ARMS),passes=list(PASSES),query_ids=[q['query_id'] for q in queries],stage_tracing=False,thread_environment=thread_environment,logical_cpu_count=os.cpu_count(),inference='No new inference. Original 95% target fails; conditional relative component only.',cache='Fresh provider cache per process; overlay/setup warm it. OS page cache uncontrolled and affected by verification; fixed arm order is confounded.')
        result['plan']=seal(out/'cost-plan.lock.json',plan);values={};reader_elapsed=0
        for arm in ARMS:
            require(time.monotonic()<end,'120-second scientific bound');value=run_arm(command(binary,spec['snapshots'][arm],spec['overlays'][arm]),arm,manifests[arm],spec['overlays'][arm],queries,baselines[arm],out,min(30,end-time.monotonic()));values[arm]=value;reader_elapsed+=value['process']['elapsed_seconds']
        require(reader_elapsed<=90,'90-second shared reader bound')
        for arm in ARMS:
            _,after=input_snapshot(spec['snapshots'][arm],scientific_deadline);require(before[arm]==after,'original saved store unchanged')
            require(overlay_closure(Path(spec['overlays'][arm]['manifest']['path']).parent,spec['snapshots'][arm])==spec['overlays'][arm],'original overlay unchanged')
        verify(spec['selection']);verify(spec['selection_lock']);verify(spec['queries'])
        result.update(status='completed',arms={a:descriptor(out/(a+'-result.json')) for a in ARMS},scientific_seconds=time.monotonic()-started,reader_seconds=reader_elapsed)
        require(result['scientific_seconds']<=120,'scientific bound including verification')
    except Exception as error:result['error']=repr(error)
    finally:
        signal.alarm(0)
        size=sum(p.stat().st_size for p in out.rglob('*') if p.is_file());write_final_result(out,result,size)
    require(result['status']=='completed','cost run failed');return result


def main():
    parser=argparse.ArgumentParser();group=parser.add_mutually_exclusive_group(required=True);group.add_argument('--spec');group.add_argument('--summarize');args=parser.parse_args()
    if args.summarize:
        request=load(args.summarize);value=load(verify(request['input']));result=summarize(value['rows'],value['process']);write(Path(request['output']),result)
    else:result=run(load(args.spec))
    print(json.dumps(result))
if __name__=='__main__':main()
