"""One declared calibration amendment, never an original-window retry."""
import argparse,hashlib,json,math,os,shutil,sys,time
from pathlib import Path
from harness_common import require,descriptor,verify,write
import query_runtime as previous
from quality_analysis import ARMS,BOOTSTRAP,cohort_ids,summarize
HERE=Path(__file__).resolve().parent
CANDIDATE={'name':'fp32-beam38-rerank4','beam':38}
CONTROL={'name':'fp32-beam36-rerank4','beam':36}


def lineage(spec):
    pins=spec['original'];values={name:json.loads(verify(pin).read_text()) for name,pin in pins.items()}
    prep=values['preparation'];selection=values['selection'];original=values['result'];admitted=values['admitted_spec'];setup=values['setup_spec']
    require(values['lock']==pins['selection'] and selection['preparation']==pins['preparation'],'original selection/preparation lock')
    require(original['status']=='completed' and original['outcome']=='calibration-target-or-pruning-not-met' and not original['evaluation'] and 'inference' not in original,'original negative/no evaluation')
    require(selection['decision']['eligible_for_evaluation'] is False and set(selection['decision']['failures'])=={'dynamic1'} and selection['bootstrap']==BOOTSTRAP,'original calibration failure')
    require(admitted['protocol']=='reserved-cohort-quality-v1' and all(admitted[k]==spec[k] for k in ['binary','vectors','restore_receipt','prefix_sha256','snapshots']),'original admitted identity')
    require(setup==dict(source=spec['vectors']['path'],output=admitted['output']),'original setup source')
    require(prep['status']=='passed' and prep['prefix_sha256']==spec['prefix_sha256'] and prep['prefix_copy_bytes']==50331648 and prep['query_logical_read_bytes']==884736,'new cohort/prefix provenance')
    require([v['canonical_global_id'] for v in prep['row_provenance']]==cohort_ids(),'same288 selected IDs')
    require(len({v['sha256'] for v in prep['row_provenance']})==288 and all(v['split']==('calibration' if j%9==0 else 'evaluation') for j,v in enumerate(prep['row_provenance'])),'cohort split/duplicate provenance')
    for split in ['calibration','evaluation']:
        require(selection[split+'_queries']==prep['queries'][split] and selection[split+'_gt']==prep['ground_truth'][split],'original cohort/GT descriptors')
    for arm in ['reference','dynamic4']:
        cell=values[arm];require(selection['decision']['chosen'][arm]==CONTROL and pins[arm] in selection['calibration_evidence'][arm],'original chosen control evidence')
        require(cell==original['calibration'][arm][1] and cell['arm']==arm and cell['split']=='calibration' and cell['policy']==CONTROL,'old control identity')
        require([v['query_id'] for v in cell['measured']]==['canonical-doc:'+str(v) for j,v in enumerate(cohort_ids()) if j%9==0],'old control calibration cohort')
        summary=summarize(cell['measured']);require(len(cell['measured'])==32 and summary['mean_recall']>=.95 and summary['pruning_queries']>=26,'old control eligibility')
    return prep,values


def amended_cycle(out,spec,prep,values,snapshots,censuses,run_cell,budget):
    cal_ids=[v for j,v in enumerate(cohort_ids()) if j%9==0];eval_ids=[v for j,v in enumerate(cohort_ids()) if j%9!=0]
    queries,gt=previous.load_split(prep,'calibration',cal_ids)
    shutil.copyfile(verify(prep['ground_truth']['calibration']),out/'calibration-gt.jsonl')
    candidate=run_cell('dynamic1',CANDIDATE,'calibration',snapshots['dynamic1'],censuses['dynamic1'],queries,gt,budget(10,time.monotonic()+10));summary=summarize(candidate['measured'])
    eligible=summary['mean_recall']>=.95 and summary['pruning_queries']>=26
    chosen={'reference':CONTROL,'dynamic4':CONTROL};failures={}
    if eligible:chosen['dynamic1']=CANDIDATE
    else:failures['dynamic1']='The single amended beam38 candidate misses fixed95% mean recall or26/32 pruning; no evaluation or further candidate.'
    decision=dict(chosen=chosen,failures=failures,eligible_for_evaluation=eligible)
    selection=dict(protocol='reserved-quality-policy-lock-v1',decision=decision,bootstrap=BOOTSTRAP,preparation=spec['original']['preparation'],calibration_queries=prep['queries']['calibration'],calibration_gt=prep['ground_truth']['calibration'],evaluation_queries=prep['queries']['evaluation'],evaluation_gt=prep['ground_truth']['evaluation'],lineage=dict(original_negative=spec['original']['result'],original_selection=spec['original']['selection'],original_lock=spec['original']['lock'],reference_control=spec['original']['reference'],dynamic4_control=spec['original']['dynamic4'],candidate=descriptor(out/'calibration-dynamic1-beam38-result.json'),amendment='Beam38 is chosen from prior calibration evidence, not evaluation. Original two-policy failure remains unchanged. No32/36 reruns, additional beam, sweep or target reduction.'))
    path=out/'selection.json'
    with path.open('x') as f:json.dump(selection,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    locked=descriptor(path);write(out/'selection.lock.json',locked)
    result=dict(outcome='amended-calibration-target-or-pruning-not-met' if not eligible else 'evaluation-complete-awaiting-analysis',selection=locked,calibration_candidate=candidate,reused_controls={a:spec['original'][a] for a in ['reference','dynamic4']},evaluation={})
    if not eligible:return result
    # This is the first evaluation read in either protocol. Hash/parse only after
    # sealing all three policies, and never use these labels to alter selection.
    require(descriptor(path)==locked,'amended selection changed');queries,gt=previous.load_split(prep,'evaluation',eval_ids);shutil.copyfile(verify(prep['ground_truth']['evaluation']),out/'evaluation-gt.jsonl');end=time.monotonic()+90
    for arm in ARMS:
        require(descriptor(path)==locked,'selection changed during evaluation');result['evaluation'][arm]=run_cell(arm,chosen[arm],'evaluation',snapshots[arm],censuses[arm],queries,gt,budget(30,end))
    require(descriptor(path)==locked,'selection changed after evaluation');return result


def run(spec):
    require(sys.platform=='linux','Linux runtime only');previous.verify_package();template=json.loads((HERE/'runtime-spec.template.json').read_text());mutable={'output','deadline_unix'};require({k:v for k,v in spec.items() if k not in mutable}=={k:v for k,v in template.items() if k not in mutable},'fixed amendment changed')
    require(type(spec['deadline_unix']) in (int,float) and math.isfinite(spec['deadline_unix']) and 180<spec['deadline_unix']-time.time()<=900,'fresh deadline/preservation reserve');binary=verify(spec['binary']);restore=json.loads(verify(spec['restore_receipt']).read_text());require(restore['status']=='complete' and restore['canonical']==spec['vectors'],'restore provenance')
    out=Path(spec['output']);protected=[Path(v['manifest']['path']).parent.resolve() for v in spec['snapshots'].values()]+[Path(spec['original']['preparation']['path']).parent.resolve()];require(not any(out.resolve().is_relative_to(p) for p in protected),'output overlaps immutable inputs');out.mkdir(exist_ok=False);started=time.monotonic();before={};snapshots={};result={};failure=None;env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1')
    def budget(cap,end):
        require(shutil.disk_usage(out).free>=20*1024**3,'20GiB free floor');remaining=min(cap,end-time.monotonic(),150-(time.monotonic()-started),spec['deadline_unix']-180-time.time());require(remaining>0,'fixed amendment deadline');return remaining
    try:
        prep,values=lineage(spec);admission=time.monotonic();deadline=min(time.time()+20,spec['deadline_unix']-180)
        for arm in ARMS:
            snapshot,pins=previous.input_snapshot(spec['snapshots'][arm],deadline);snapshot['_store']=str(Path(spec['snapshots'][arm]['manifest']['path']).parent);snapshots[arm]=snapshot;before[arm]=pins
        write(out/'snapshot-before.json',before);admission_seconds=time.monotonic()-admission;censuses={a:spec['snapshots'][a]['census'] for a in ARMS}
        def run_cell(arm,policy,split,snapshot,census,queries,gt,remaining):return previous.cell(binary,arm,policy,split,snapshot,census,queries,gt,out,remaining,env)
        result=amended_cycle(out,spec,prep,values,snapshots,censuses,run_cell,budget);result['admission_inventory_seconds']=admission_seconds;result['gt_evidence_copy_bytes']=sum((out/(split+'-gt.jsonl')).stat().st_size for split in ['calibration','evaluation'] if (out/(split+'-gt.jsonl')).exists())
        if result['evaluation']:
            analysis=dict(selection=result['selection'],evaluations={a:descriptor(out/('evaluation-'+a+'-beam'+str(v['policy']['beam'])+'-result.json')) for a,v in result['evaluation'].items()},output=str(out/'inference.json'));write(out/'analysis-spec.json',analysis);previous.child([sys.executable,str(HERE/'query_runtime.py'),'--analyze',str(out/'analysis-spec.json')],'analysis',out,budget(10,time.monotonic()+10),env);result['inference']=json.loads((out/'inference.json').read_text());result['outcome']=result['inference']['outcome']
        result['scientific_seconds']=time.monotonic()-started;require(result['scientific_seconds']<=150,'scientific cap')
    except Exception as error:failure=repr(error)
    finally:
        try:
            preservation=time.monotonic();deadline=min(time.time()+30,spec['deadline_unix']-10);after={a:previous.inventory(Path(spec['snapshots'][a]['manifest']['path']).parent,deadline) for a in before};write(out/'snapshot-after.json',after);require(before==after,'saved objects changed');[verify(v) for v in spec['original'].values()];[verify(prep[k]['calibration']) for k in ['queries','ground_truth']] if 'prep' in locals() else None;[verify(prep[k]['evaluation']) for k in ['queries','ground_truth']] if result.get('evaluation') else None;verify(spec['binary']);previous.verify_package();result['preservation_seconds']=time.monotonic()-preservation
        except Exception as error:failure='preservation: '+repr(error)+'; prior='+str(failure)
        metadata=sum(p.stat().st_size for p in out.rglob('*') if p.is_file());result.update(status='failed' if failure else 'completed',error=failure,elapsed_seconds=time.monotonic()-started,metadata_bytes=metadata,binary=spec['binary'],scope='Separate calibration amendment learned from calibration only. Original negative preserved; old16K trees, conditional fixed-tree/cohort inference. No source/build/corpus/GT generation or automatic retry.');previous.write_final_result(out,result,metadata)
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--spec',required=True);s=run(json.loads(Path(p.parse_args().spec).read_text()));print(json.dumps(s));raise SystemExit(1 if s['status']=='failed' else 2 if s['outcome']=='amended-calibration-target-or-pruning-not-met' else 0)
