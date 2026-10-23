"""One Code4 posting policy, calibration first; no evaluation tuning or lifecycle."""
import argparse,hashlib,json,math,os,shutil,sys,time
from pathlib import Path
from harness_common import require,descriptor,verify,write
import query_runtime as previous
from quality_analysis import ARMS,BOOTSTRAP,cohort_ids,summarize,vector_traffic
HERE=Path(__file__).resolve().parent
OVERLAY_VERIFY={'calls':0,'code_hash_read_bytes':0,'manifest_read_bytes':0,'elapsed_seconds':0.}
POLICIES={a:dict(name='code4-residual-FP-navigation-rerank4',beam=38 if a=='dynamic1' else 36) for a in ARMS}

def overlay_closure(path,snapshot):
    started=time.monotonic();path=Path(path);manifest=path/'overlay.json';require(manifest.stat().st_size<=512*1024,'overlay metadata cap');m=json.loads(manifest.read_text())
    require(m['policy']=='code4-residual-postings-v1' and m['snapshot']==snapshot['manifest']['sha256'] and m['dimension']==768 and m['metric']=='InnerProduct' and m['root']==snapshot['root_ref']['id'],'overlay snapshot binding')
    require(len(m['leaves'])==snapshot['census']['reachable_leaves']<=256,'overlay leaf closure')
    files={};total=0
    for leaf in m['leaves']:
        name=leaf['file'];require(name==f"leaf-{leaf['id']}.code4" and name not in files,'overlay local path/uniqueness');require(type(leaf['rows']) is int and type(leaf['unique_rows']) is int and 0<=leaf['unique_rows']<=leaf['rows']<=4096,'overlay row bounds');p=path/name;require(not p.is_symlink(),'overlay symlink');pin=descriptor(p);require(pin['bytes']==leaf['bytes']==leaf['unique_rows']*400 and pin['sha256']==leaf['sha256'],'overlay leaf digest/extent');files[name]=pin;total+=pin['bytes']
    require(total<=16*1024**2+32768*4,'overlay payload bound');OVERLAY_VERIFY['calls']+=1;OVERLAY_VERIFY['code_hash_read_bytes']+=total;OVERLAY_VERIFY['manifest_read_bytes']+=manifest.stat().st_size*2;OVERLAY_VERIFY['elapsed_seconds']+=time.monotonic()-started;return dict(manifest=descriptor(manifest),codes=files,serialized_code_bytes=total)

def locked(selection,overlay_pins,spec):
    require(descriptor(selection['path'])==selection,'policy lock changed')
    for arm,pin in overlay_pins.items():
        require(overlay_closure(Path(pin['manifest']['path']).parent,spec['snapshots'][arm])==pin,'immutable overlay changed')

def cycle(out,spec,prep,snapshots,censuses,run_cell,budget,baseline_raw):
    cal_ids=[v for j,v in enumerate(cohort_ids()) if j%9==0];eval_ids=[v for j,v in enumerate(cohort_ids()) if j%9!=0]
    queries,gt=previous.load_split(prep,'calibration',cal_ids);shutil.copyfile(verify(prep['ground_truth']['calibration']),out/'calibration-gt.jsonl');calibration={};overlay_pins={};failures={};end=time.monotonic()+45
    for arm in ARMS:
        value=run_cell(arm,POLICIES[arm],'calibration',snapshots[arm],censuses[arm],queries,gt,budget(15,end));calibration[arm]=value
        # Pure scoring changes must leave the offered live rows and global navigation fixed.
        raw=[json.loads(v) for v in verify(value['raw_stdout']).read_text().splitlines()][32:];old=baseline_raw[arm]
        for a,b in zip(raw,old):
            require(a['query_stage_trace']['selected_leaf_ids']==b['query_stage_trace']['selected_leaf_ids'] and set(a['query_stage_trace']['pretrim_candidate_ids'])==set(b['query_stage_trace']['pretrim_candidate_ids']),'baseline navigation/current-ID work changed')
            for key in ['selected_leaves','nonempty_leaves_scored','posting_rows_scored_before_dedup','distinct_candidates_before_trim','centroid_distance_evaluations','expanded_internal_nodes']:
                require(a['query_work']['counts'][key]==b['query_work']['counts'][key],'baseline work changed: '+key)
        report=value['posting_overlay_report'];require(report['created'] is True and report['snapshot']==spec['snapshots'][arm]['manifest']['sha256'],'new overlay preparation')
        overlay_pins[arm]=overlay_closure(out/('overlay-'+arm),spec['snapshots'][arm]);require(report['manifest_sha256']==overlay_pins[arm]['manifest']['sha256'],'prepared overlay digest')
        expected=report['embedding_point_gets']+report['explicit_centroid_gets']+report['loader_centroid_gets'];value['preparation_vector_traffic']=vector_traffic(report['vector_observation'],expected)
        summary=summarize(value['measured'])
        if summary['mean_recall']<.95 or summary['pruning_queries']<26:failures[arm]='Code4 posting policy misses fixed95% mean recall or26/32 pruning.'
    eligible=not failures;decision=dict(chosen=POLICIES,failures=failures,eligible_for_evaluation=eligible)
    selection=dict(protocol='reserved-quality-policy-lock-v1',decision=decision,bootstrap=BOOTSTRAP,posting_policy='code4-residual-postings-v1',build=spec['build_binding'],configuration=dict(k=100,rerank_factor=4,navigation='fp',tau=2.0,cache=previous.CACHE),snapshots=spec['snapshots'],overlays=overlay_pins,calibration_evidence={a:descriptor(out/f"calibration-{a}-beam{POLICIES[a]['beam']}-result.json") for a in ARMS},preparation=spec['original']['preparation'],calibration_queries=prep['queries']['calibration'],calibration_gt=prep['ground_truth']['calibration'],evaluation_queries=prep['queries']['evaluation'],evaluation_gt=prep['ground_truth']['evaluation'],lineage='One new common posting policy learned from calibration. Old failures retained; no evaluation-informed selection, sweep, target reduction or unchanged-policy equivalence claim.')
    path=out/'selection.json'
    with path.open('x') as f:json.dump(selection,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    pin=descriptor(path)
    with (out/'selection.lock.json').open('x') as f:json.dump(pin,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    fd=os.open(out,os.O_RDONLY);os.fsync(fd);os.close(fd)
    result=dict(outcome='calibration-target-or-pruning-not-met' if not eligible else 'evaluation-complete-awaiting-analysis',selection=pin,calibration=calibration,overlays=overlay_pins,evaluation={})
    if not eligible:return result
    # No evaluation labels or query payload are read until all policies/config/overlays are sealed.
    locked(pin,overlay_pins,spec);queries,gt=previous.load_split(prep,'evaluation',eval_ids);shutil.copyfile(verify(prep['ground_truth']['evaluation']),out/'evaluation-gt.jsonl');end=time.monotonic()+90
    for arm in ARMS:
        locked(pin,overlay_pins,spec);value=run_cell(arm,POLICIES[arm],'evaluation',snapshots[arm],censuses[arm],queries,gt,budget(30,end));require(value['posting_overlay_report']['created'] is False and value['posting_overlay_report']['embedding_point_gets']==0 and value['posting_overlay_report']['manifest_sha256']==overlay_pins[arm]['manifest']['sha256'],'reuse exact locked overlay');result['evaluation'][arm]=value
    locked(pin,overlay_pins,spec);return result

def run(spec):
    require(sys.platform=='linux','Linux runtime only');previous.verify_package();template=json.loads((HERE/'runtime-spec.template.json').read_text());require(set(spec)==set(template) and all(spec[k]==v for k,v in template.items() if k not in ['deadline_unix','output']),'fixed Code4 protocol changed');require(type(spec['deadline_unix']) in (int,float) and math.isfinite(spec['deadline_unix']) and 180<spec['deadline_unix']-time.time()<=1200,'fixed window/reserve')
    build=json.loads(Path(spec['build_receipt']).read_text());require(build['returncode']==0 and build['compiler_sha256']=='028fd60b0e0add5505c661cd3ccde91393d615c8729bd95d94ab1e91409b36a1' and build['source_manifest_sha256']==descriptor(HERE/'source-manifest.json')['sha256'],'exact new Linux build binding');binary=verify(build['binary']);spec=dict(spec,build_binding=descriptor(spec['build_receipt']));out=Path(spec['output']);protected=[Path(v['manifest']['path']).parent.resolve() for v in spec['snapshots'].values()]+[Path(spec['original']['preparation']['path']).parent.resolve()];require(not any(out.resolve().is_relative_to(p) for p in protected),'output overlaps old artifacts');out.mkdir(exist_ok=False);started=time.monotonic();before={};result={};failure=None;env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1')
    def budget(cap,end):
        require(shutil.disk_usage(out).free>=20*1024**3,'20GiB floor');remaining=min(cap,end-time.monotonic(),180-(time.monotonic()-started),spec['deadline_unix']-time.time()-180);require(remaining>0,'scientific deadline exhausted');return remaining
    try:
        original={k:json.loads(verify(v).read_text()) for k,v in spec['original'].items()};prep=original['preparation'];require(prep['status']=='passed' and prep['prefix_sha256']==spec['prefix_sha256'] and [v['canonical_global_id'] for v in prep['row_provenance']]==cohort_ids(),'same reserved cohort/prefix');require(len({v['sha256'] for v in prep['row_provenance']})==288 and all(v['split']==('calibration' if j%9==0 else 'evaluation') for j,v in enumerate(prep['row_provenance'])),'cohort duplicate/split provenance');admitted=original['admitted_spec'];require(all(spec[k]==admitted[k] for k in ['vectors','restore_receipt','prefix_sha256','snapshots']),'old source/tree identity');verify(admitted['binary']);require(original['selection']['decision']['eligible_for_evaluation'] is False and original['result']['evaluation']=={},'original negative/evaluation untouched')
        baseline_raw={a:[json.loads(v) for v in verify(pin).read_text().splitlines()][32:] for a,pin in spec['baseline_raw'].items()};require(all(len(v)==32 for v in baseline_raw.values()),'prior baseline cells')
        snapshots={};censuses={};end=time.monotonic()+20
        for arm in ARMS:
            m,before[arm]=previous.inspect_snapshot(spec['snapshots'][arm],time.time()+budget(20,end));snapshots[arm]=dict(m,_store=str(Path(spec['snapshots'][arm]['manifest']['path']).parent));censuses[arm]=spec['snapshots'][arm]['census'];spec['snapshots'][arm].update({k:m[k] for k in ['root_ref','generation']})
        write(out/'snapshot-before.json',before)
        runner=lambda a,p,split,s,c,q,g,remaining:previous.cell(binary,a,p,split,s,c,q,g,out,remaining,env)
        result=cycle(out,spec,prep,snapshots,censuses,runner,budget,baseline_raw)
        if result['evaluation']:
            request=dict(selection=result['selection'],evaluations={a:descriptor(out/f"evaluation-{a}-beam{POLICIES[a]['beam']}-result.json") for a in ARMS},output=str(out/'inference.json'));write(out/'analysis-input.json',request);previous.child([sys.executable,str(HERE/'query_runtime.py'),'--analyze',str(out/'analysis-input.json')],'analysis',out,budget(10,time.monotonic()+10),env);result['inference']=json.loads((out/'inference.json').read_text());result['outcome']=result['inference']['outcome']
        result['scientific_seconds']=time.monotonic()-started;require(result['scientific_seconds']<=180,'science cap')
    except Exception as e:failure=repr(e)
    finally:
        try:
            start=time.monotonic();end=min(time.time()+30,spec['deadline_unix']-10);after={a:previous.inventory(Path(spec['snapshots'][a]['manifest']['path']).parent,end) for a in before};write(out/'snapshot-after.json',after);require(before==after,'old saved objects changed');locked(result['selection'],result['overlays'],spec) if result.get('overlays') else None;verify(build['binary']);[verify(v) for v in spec['original'].values()];[verify(v) for v in spec['baseline_raw'].values()];previous.verify_package();result['preservation_seconds']=time.monotonic()-start
        except Exception as e:failure='preservation: '+repr(e)+'; prior='+str(failure)
        metadata=sum(p.stat().st_size for p in out.rglob('*') if p.is_file());result.update(status='failed' if failure else 'completed',error=failure,elapsed_seconds=time.monotonic()-started,metadata_bytes=metadata,overlay_integrity_verification=OVERLAY_VERIFY,binary=build['binary'],scope='Same32calibration/256reserved document queries, old16K saved tree epoch; common Code4 residual policy, exact400trim/100rerank, FP navigation unchanged. Preparation/overlay cache costs charged; no latency or bitwidth-only claim. No automatic retry/target reduction.');previous.write_final_result(out,result,metadata)
    return result
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--spec',required=True);s=run(json.loads(Path(p.parse_args().spec).read_text()));print(json.dumps(s));raise SystemExit(1 if s['status']=='failed' else 2 if s['outcome']=='calibration-target-or-pruning-not-met' else 0)
