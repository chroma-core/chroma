"""One reserved-cohort comparison. No build, lifecycle, transfer or retry."""
import argparse,hashlib,json,math,os,resource,shutil,statistics,struct,subprocess,sys,time
from pathlib import Path
from harness_common import require,descriptor,verify,write
from construction_support import child,child_peak_kib
from quality_analysis import ARMS,POLICIES,BOOTSTRAP,CACHE,ROWS,DIM,cohort_ids,validate_gt,validate_reply,summarize,choose,infer
from prepare_cohort import prepare
HERE=Path(__file__).resolve().parent


def verify_package():
    for name,pin in json.loads((HERE/'package-manifest.json').read_text())['files'].items():
        path=HERE/name;require(path.is_file() and path.stat().st_size==pin['bytes'] and hashlib.sha256(path.read_bytes()).hexdigest()==pin['sha256'],'package member: '+name)


def inventory(store,deadline):
    result={};total=0
    for path in sorted(store.rglob('*')):
        require(time.time()<deadline,'inventory deadline');require(not path.is_symlink(),'saved store symlink')
        if not path.is_file():continue
        require(len(result)<10000,'saved object cap');size=path.stat().st_size;total+=size;require(total<=512*1024**2,'saved store payload cap');h=hashlib.sha256()
        with path.open('rb') as f:
            for data in iter(lambda:f.read(1024*1024),b''):require(time.time()<deadline,'inventory deadline');h.update(data)
        result[str(path.relative_to(store))]=dict(bytes=size,sha256=h.hexdigest())
    require(result,'empty saved store');return dict(files=result,logical_hash_read_bytes=total,scope='Whole saved-store before/after byte identity. Original manifest/map hashes are previously admitted; this new inventory is not a backdated independent object-hash receipt.')


def input_snapshot(spec,deadline):
    manifest=json.loads(verify(spec['manifest']).read_text());mapping=verify(spec['mapping'])
    require(manifest['generation']==1 and manifest['dimension']==DIM and manifest['metric']=='ip' and manifest['vector_count']==ROWS and manifest['shard_id']=='global','saved manifest shape')
    require(manifest['block_ids']==spec['roots'] and manifest['integrity_audit']==spec['census'],'original roots/census')
    require(manifest['id_mapping']['sha256']==spec['mapping']['sha256'] and manifest['id_mapping']['count']==ROWS and manifest['id_mapping']['bytes']==ROWS*8 and manifest['id_mapping']['path']==mapping.name,'original map descriptor')
    require(struct.unpack('<'+str(ROWS)+'Q',mapping.read_bytes())==tuple(range(ROWS)),'dense identity map')
    census=spec['census'];require(census['unique_live_rows']==ROWS and census['duplicate_current_entries']==census['unreachable_nodes']==0,'admitted complete census')
    return manifest,inventory(Path(spec['manifest']['path']).parent,deadline)


def cell(binary,arm,policy,split,snapshot,census,queries,gt,out,remaining,env):
    stem=split+'-'+arm+'-beam'+str(policy['beam']);requests=[dict(request_id=phase+':'+q['query_id'],generation=1,op='query',query=q['vector'],k=100,beam_min=policy['beam'],beam_max=policy['beam'],rerank_factor=4) for phase in ['warmup','measured'] for q in queries]
    path=out/(stem+'-requests.jsonl');path.write_text(''.join(json.dumps(v)+'\n' for v in requests));command=[str(binary),'read','--checkpoint-dir',snapshot['_store'],'--profile-query-work','--profile-rerank-pins','--trace-query-stages','--query-navigation','fp','--provider-cache-policy',json.dumps(CACHE)]
    started=time.monotonic();cpu=resource.getrusage(resource.RUSAGE_CHILDREN)
    try:
        with path.open('rb') as stdin,(out/(stem+'.stdout')).open('xb') as stdout,(out/(stem+'.stderr')).open('xb') as stderr:p=subprocess.run(command,stdin=stdin,stdout=stdout,stderr=stderr,env=env,timeout=remaining)
    except subprocess.TimeoutExpired:
        write(out/(stem+'-process.json'),dict(command=command,status='timeout',elapsed_seconds=time.monotonic()-started));raise
    after=resource.getrusage(resource.RUSAGE_CHILDREN);process=dict(command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-started,user_cpu_seconds=after.ru_utime-cpu.ru_utime,system_cpu_seconds=after.ru_stime-cpu.ru_stime,voluntary_switches=after.ru_nvcsw-cpu.ru_nvcsw,involuntary_switches=after.ru_nivcsw-cpu.ru_nivcsw,observed_cumulative_child_maxrss_kib=child_peak_kib(after));write(out/(stem+'-process.json'),process);require(p.returncode==0,'reader failed');require(child_peak_kib(after)<=8*1024**2,'observed child RSS cap')
    raw=[json.loads(v) for v in (out/(stem+'.stdout')).read_text().splitlines()];require(len(raw)==len(requests),'reply count');require(all(v['fp_navigation_cache']==raw[0]['fp_navigation_cache'] for v in raw),'one immutable preload per cell')
    values=[validate_reply(v,request,gt[queries[i%len(queries)]['query_id']],snapshot,census) for i,(v,request) in enumerate(zip(raw,requests))];measured=values[len(queries):]
    # Warm-up/result decisions must match within this immutable reader, independent
    # of cache traffic. Keep all raw replies; this does not make timing unperturbed.
    for warm,retained in zip(values[:len(queries)],measured):require((warm['returned_ids'],warm['returned_distance_bits'],warm['counts'])==(retained['returned_ids'],retained['returned_distance_bits'],retained['counts']),'warm/retained decisions changed')
    value=dict(arm=arm,policy=policy,split=split,summary=summarize(measured),measured=measured,fp_navigation_cache=raw[0]['fp_navigation_cache'],reader_open_seconds=raw[0]['query_reader_open_seconds'],process=process,raw_stdout=descriptor(out/(stem+'.stdout')),requests=descriptor(path),coverage_scope='New prefix GT directly joined to complete pretrim/retained/returned IDs. Reached-current-candidate loss is not an independent selected-leaf membership audit.');write(out/(stem+'-result.json'),value);return value


def load_split(preparation,split,expected):
    queries=[json.loads(v) for v in verify(preparation['queries'][split]).read_text().splitlines()];gt=[json.loads(v) for v in verify(preparation['ground_truth'][split]).read_text().splitlines()]
    require([q['canonical_global_id'] for q in queries]==expected and all(q['split']==split for q in queries),'reserved cohort binding');validate_gt(gt,[q['query_id'] for q in queries]);return queries,{v['query_id']:v for v in gt}


def policy_cycle(out,preparation,snapshots,censuses,run_cell,budget):
    selection_ids=cohort_ids();cal_ids=[v for j,v in enumerate(selection_ids) if j%9==0];eval_ids=[v for j,v in enumerate(selection_ids) if j%9!=0]
    queries,gt=load_split(preparation,'calibration',cal_ids);calibration={};cal_end=time.monotonic()+60
    for arm in ARMS:
        calibration[arm]=[run_cell(arm,p,'calibration',snapshots[arm],censuses[arm],queries,gt,budget(10,cal_end)) for p in POLICIES]
    decision=choose(calibration)
    lock_value=dict(cohort_selection=descriptor(HERE/'cohort-selection.json'),protocol='reserved-quality-policy-lock-v1',decision=decision,bootstrap=BOOTSTRAP,preparation=descriptor(out/'preparation.json'),calibration_queries=preparation['queries']['calibration'],calibration_gt=preparation['ground_truth']['calibration'],evaluation_queries=preparation['queries']['evaluation'],evaluation_gt=preparation['ground_truth']['evaluation'],calibration_evidence={a:[descriptor(out/('calibration-'+a+'-beam'+str(p['beam'])+'-result.json')) for p in POLICIES] for a in ARMS},scope='Only calibration query outcomes select policies. Evaluation outcomes do not exist when this artifact is sealed.')
    path=out/'selection.json'
    with path.open('x') as f:json.dump(lock_value,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    locked=descriptor(path);write(out/'selection.lock.json',locked)
    if not decision['eligible_for_evaluation']:return dict(outcome='calibration-target-or-pruning-not-met',selection=locked,calibration=calibration,evaluation={})
    # Only after sealing all choices do we load evaluation labels or start its reader.
    require(descriptor(path)==locked,'selection changed before evaluation');queries,gt=load_split(preparation,'evaluation',eval_ids);evaluation={};eval_end=time.monotonic()+90
    for arm in ARMS:
        require(descriptor(path)==locked,'selection changed during evaluation');evaluation[arm]=run_cell(arm,decision['chosen'][arm],'evaluation',snapshots[arm],censuses[arm],queries,gt,budget(30,eval_end))
    require(descriptor(path)==locked,'selection changed after evaluation')
    return dict(outcome='evaluation-complete-awaiting-analysis',selection=locked,calibration=calibration,evaluation=evaluation)


def serialized_result(result,metadata_before):
    # Include the reporting field's own bytes. Its decimal width reaches a fixed
    # point after serialization, so the reported total equals the retained bytes.
    result['metadata_including_result_bytes']=0
    for _ in range(16):
        payload=(json.dumps(result,indent=2)+'\n').encode('utf-8')
        total=metadata_before+len(payload)
        if total==result['metadata_including_result_bytes']:return payload
        result['metadata_including_result_bytes']=total
    raise ValueError('metadata serialization did not converge')


def write_final_result(out,result,metadata_before,cap=512*1024**2):
    payload=serialized_result(result,metadata_before)
    if metadata_before+len(payload)>cap:
        result.update(status='failed',error='metadata cap exceeded')
        payload=serialized_result(result,metadata_before)
    with (out/'result.json').open('xb') as stream:stream.write(payload)
    require(result['metadata_including_result_bytes']==metadata_before+(out/'result.json').stat().st_size,'actual metadata size')
    return result


def run(spec):
    require(sys.platform=='linux','Linux runtime only');verify_package();template=json.loads((HERE/'runtime-spec.template.json').read_text());mutable={'output','deadline_unix'};require({k:v for k,v in spec.items() if k not in mutable}=={k:v for k,v in template.items() if k not in mutable},'fixed protocol changed')
    require(type(spec['deadline_unix']) in (float,int) and math.isfinite(spec['deadline_unix']) and 180<spec['deadline_unix']-time.time()<=900,'fresh deadline/preservation reserve')
    selection=json.loads((HERE/'cohort-selection.json').read_text());require(selection['ids']==cohort_ids() and selection['id_list_sha256']==hashlib.sha256(json.dumps(cohort_ids(),separators=(',',':')).encode()).hexdigest(),'frozen selection');binary=verify(spec['binary']);restore=json.loads(verify(spec['restore_receipt']).read_text());require(restore['status']=='complete' and restore['canonical']==spec['vectors'] and restore['rows']==2000000 and restore['dimension']==DIM,'restore binding');require(Path(spec['vectors']['path']).stat().st_size==6144000000,'canonical extent')
    out=Path(spec['output']);require(not any(out.resolve().is_relative_to(Path(v['manifest']['path']).parent.resolve()) for v in spec['snapshots'].values()),'output overlaps saved store');out.mkdir(exist_ok=False);started=time.monotonic();snapshots={};before={};result={};failure=None
    env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1')
    def budget(cap,end):
        require(shutil.disk_usage(out).free>=20*1024**3,'20GiB free floor');remaining=min(cap,end-time.monotonic(),240-(time.monotonic()-started),spec['deadline_unix']-180-time.time());require(remaining>0,'fixed scientific deadline');return remaining
    try:
        admission_started=time.monotonic();admission_deadline=min(time.time()+20,spec['deadline_unix']-180)
        for arm in ARMS:
            snapshot,pins=input_snapshot(spec['snapshots'][arm],admission_deadline);snapshot['_store']=str(Path(spec['snapshots'][arm]['manifest']['path']).parent);snapshots[arm]=snapshot;before[arm]=pins
        write(out/'snapshot-before.json',before);admission_seconds=time.monotonic()-admission_started
        setup_spec=dict(source=spec['vectors']['path'],output=str(out));write(out/'setup-spec.json',setup_spec);child([sys.executable,str(HERE/'run-quality.py'),'--prepare',str(out/'setup-spec.json')],'setup',out,budget(60,time.monotonic()+60),env)
        preparation=json.loads((out/'preparation.json').read_text());require(preparation['prefix_sha256']==spec['prefix_sha256'] and preparation['prefix_copy_bytes']==50331648 and preparation['query_logical_read_bytes']==884736 and preparation['numpy_version']=='2.5.4','new cohort/GT binding');require(len(preparation['row_provenance'])==288 and [v['canonical_global_id'] for v in preparation['row_provenance']]==cohort_ids(),'frozen selected IDs')
        write(out/'frozen-cohort-descriptors.json',dict(queries=preparation['queries'],ground_truth=preparation['ground_truth'],selection_sha256=hashlib.sha256(json.dumps(cohort_ids(),separators=(',',':')).encode()).hexdigest(),engine_manifest=json.loads((HERE/'engine-manifest.json').read_text())))
        censuses={a:spec['snapshots'][a]['census'] for a in ARMS}
        def run_cell(arm,policy,split,snapshot,census,queries,gt,remaining):return cell(binary,arm,policy,split,snapshot,census,queries,gt,out,remaining,env)
        result=policy_cycle(out,preparation,snapshots,censuses,run_cell,budget);result['admission_inventory_seconds']=admission_seconds
        if result['evaluation']:
            analysis_spec=dict(selection=result['selection'],evaluations={a:descriptor(out/('evaluation-'+a+'-beam'+str(v['policy']['beam'])+'-result.json')) for a,v in result['evaluation'].items()},output=str(out/'inference.json'));write(out/'analysis-spec.json',analysis_spec);child([sys.executable,str(HERE/'run-quality.py'),'--analyze',str(out/'analysis-spec.json')],'analysis',out,budget(10,time.monotonic()+10),env);result['inference']=json.loads((out/'inference.json').read_text());result['outcome']=result['inference']['outcome']
        result['scientific_seconds']=time.monotonic()-started;require(result['scientific_seconds']<=240,'scientific cap')
    except Exception as error:failure=repr(error)
    finally:
        try:
            preservation_started=time.monotonic();preservation_deadline=min(time.time()+30,spec['deadline_unix']-10);after={a:inventory(Path(spec['snapshots'][a]['manifest']['path']).parent,preservation_deadline) for a in before};write(out/'snapshot-after.json',after);require(before==after,'saved object bytes changed');verify(spec['binary']);verify_package();result['preservation_seconds']=time.monotonic()-preservation_started
        except Exception as error:failure='preservation: '+repr(error)+'; prior='+str(failure)
        files=[p for p in out.rglob('*') if p.is_file()];metadata=sum(p.stat().st_size for p in files if p!=out/'evaluation-prefix.f32');result.update(status='failed' if failure else 'completed',error=failure,elapsed_seconds=time.monotonic()-started,metadata_bytes=metadata,binary=spec['binary'],old_construction_epoch='c810fb55c2db4ac5aefc55bf7dc839e096906f119021d58a7940ec5909804f22',scope='Reserved document cohort; conditional fixed-tree resampling only. Saved trees predate live-prefix/split filter. Negative/inconclusive outcomes are retained without retry. No physical network/IO or actor-fanout claim. Traced warm latency is descriptive.')
        write_final_result(out,result,metadata)
    return result


def main():
    p=argparse.ArgumentParser();p.add_argument('--spec');p.add_argument('--prepare');p.add_argument('--analyze');a=p.parse_args();require(sum(v is not None for v in [a.spec,a.prepare,a.analyze])==1,'one mode')
    if a.prepare:
        require(sys.platform=='linux','Linux preparation only');verify_package();s=json.loads(Path(a.prepare).read_text());template=json.loads((HERE/'runtime-spec.template.json').read_text());require(s['source']==template['vectors']['path'],'fixed canonical source');v=prepare(s['source'],Path(s['output']));print(json.dumps(v));return
    if a.analyze:
        require(sys.platform=='linux','Linux analysis only');verify_package();s=json.loads(Path(a.analyze).read_text());lock=json.loads(verify(s['selection']).read_text());require(lock['decision']['eligible_for_evaluation'] and lock['bootstrap']==BOOTSTRAP,'sealed analysis policy');evaluation={}
        for arm in ARMS:
            v=json.loads(verify(s['evaluations'][arm]).read_text());require(v['split']=='evaluation' and v['arm']==arm and v['policy']==lock['decision']['chosen'][arm] and len(v['measured'])==256,'evaluation policy/count');evaluation[arm]=v['measured']
        require([v['query_id'] for v in evaluation['reference']]==['canonical-doc:'+str(v) for j,v in enumerate(cohort_ids()) if j%9!=0],'reserved inference cohort');v=infer(evaluation);write(s['output'],v);print(json.dumps(v));return
    v=run(json.loads(Path(a.spec).read_text()));print(json.dumps(v));raise SystemExit(1 if v['status']=='failed' else 2 if v['outcome']=='calibration-target-or-pruning-not-met' else 0)

if __name__=='__main__':main()
