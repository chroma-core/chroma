"""One same-input16K owner-policy ablation; no lifecycle or retry authority."""
import argparse,importlib.util,json,math,os,resource,shutil,sys,time,hashlib
from pathlib import Path
from harness_common import require,descriptor,verify,write
from construction_support import child,checkpoint,child_peak_kib
from shape import shape
HERE=Path(__file__).resolve().parent
ARMS=('baseline','split-input')


def prefix_hash(path,bytes_,deadline):
    h=hashlib.sha256();count=0;reads=0
    with Path(path).open('rb') as stream:
        while count<bytes_:
            require(time.time()<deadline,'prefix deadline')
            value=stream.read(min(1024*1024,bytes_-count));require(value,'truncated prefix');h.update(value);count+=len(value);reads+=1
    require(time.time()<deadline,'prefix deadline');return dict(bytes=count,sha256=h.hexdigest(),read_requests=reads,scope='High-level logical file read requests and returned bytes, not physical I/O. No prefix copy.')


def validate(value,native,arm):
    require(value['spec']==native,'native policy/config echo');shape(value,'dynamic4');report=value['owner_report'];work=report['split_input_validation'];enabled=arm=='split-input';require(work['enabled'] is enabled,'split-input echo')
    require(report['live_prefix_validation']['enabled'] is True,'live-prefix fixed policy')
    for k in ['physical_rows','retained_rows','scalar_current_calls','batches','owner_calls','owner_requested_rows']:require(type(work[k]) is int and work[k]>=0,'split counter type')
    require(0<=work['retained_rows']<=work['physical_rows'] and work['owner_requested_rows']<=work['physical_rows'],'split row accounting')
    hist=work['batch_lengths'];require(len(hist)==65 and all(type(v) is int and v>=0 for v in hist) and hist[0]==0,'bounded histogram')
    require(sum(hist)==work['batches'],'batch count')
    require(work['physical_rows']>0,'split filtering absent')
    if enabled:require(work['scalar_current_calls']==0 and work['owner_calls']>0 and sum(i*n for i,n in enumerate(hist))==work['physical_rows'],'grouped work absent/inconsistent')
    else:require(work['scalar_current_calls']==work['physical_rows'] and work['batches']==work['owner_calls']==work['owner_requested_rows']==0,'baseline scalar work')
    return dict(split_input=work,logical_requested_id_bytes=work['owner_requested_rows']*4,logical_reply_presence_version_bytes=work['owner_requested_rows']*2,prefix=report['live_prefix_validation'],predicate_computed_rows=report['predicate_batch_computed_rows'],predicate_callbacks=report['predicate_batch_callbacks'],scope='Only tagged split-input work; other current callbacks are not assigned to this site. Logical request/reply estimates exclude slots/envelopes/transport. Retained flags preserve missing-to-zero comparison, not a new deletion rule. Prefix suffix/predicate work remains charged.')


def run(s):
    require(sys.platform=='linux','Linux runtime only');template=json.loads((HERE/'runtime-spec.template.json').read_text());mutable={'source','output','binary','deadline_unix'};require({k:v for k,v in s.items() if k not in mutable}=={k:v for k,v in template.items() if k not in mutable},'fixed pair plan changed');require(type(s['deadline_unix']) in (int,float) and math.isfinite(s['deadline_unix']) and time.time()<s['deadline_unix'],'external deadline')
    binary=verify(s['binary']);receipt=json.loads(verify(s['restore_receipt']).read_text());require(receipt['status']=='complete' and receipt['canonical']==s['vectors'] and receipt['rows']==2000000 and receipt['dimension']==768,'immutable restore binding');source=Path(s['vectors']['path']);require(source.stat().st_size==6144000000,'source extent')
    tree=Path(s['source']);manifest=json.loads((HERE/'source-manifest.json').read_text());sp=importlib.util.spec_from_file_location('build',HERE/'compile-native.py');mod=importlib.util.module_from_spec(sp);sp.loader.exec_module(mod);mod.verify(tree,manifest);require(shutil.disk_usage(tree).free>=20*1024**3,'20GiB free floor')
    out=Path(s['output']);out.mkdir(exist_ok=False);start=time.monotonic();results={};summaries={};bindings={};error=None;env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
    try:
        prep=prefix_hash(source,16384*768*4,min(s['deadline_unix'],time.time()+10));require(prep['sha256']==s['prefix_sha256'],'retained16K prefix differs');prep['elapsed_seconds']=time.monotonic()-start;write(out/'preparation.json',prep);end=time.monotonic()+60
        for arm in ARMS:
            require(shutil.disk_usage(out).free>=20*1024**3,'20GiB free floor');remaining=min(30,end-time.monotonic(),70-(time.monotonic()-start),s['deadline_unix']-time.time());require(remaining>0,'pair deadline')
            native=dict(protocol='reference-owner-parallel-v1',source_id='retained-matched2m-first16384',vectors=s['vectors'],prefix_sha256=s['prefix_sha256'],rows=16384,dimension=768,metric='ip',config=s['config'],treatment='owner',predicate_batching=True,construction_observation=True,skip_placement=False,direct_current_reads=False,batched_scrub_validation=True,live_prefix_validation=True,batched_split_input_validation=arm=='split-input',publish_checkpoint=True,threads=4,balance_every=4096,owner_slots=4,move_above=4096,output=str(out/arm))
            path=out/(arm+'-spec.json');write(path,native);value=child([str(binary),'reference-owner-diagnostic','--spec',str(path)],arm,out,remaining,env);require(value==json.loads((out/arm/'result.json').read_text()) and value['spec_sha256']==descriptor(path)['sha256'],'native raw binding');summaries[arm]=validate(value,native,arm);bindings[arm]=checkpoint(value,native);results[arm]=value
        require(len(results['baseline']['all_id_exact_fp_distance_bits'])==16384 and results['baseline']['all_id_exact_fp_distance_bits']==results['split-input']['all_id_exact_fp_distance_bits'],'saved all-ID exact bits')
        for mf,mp in bindings.values():require(descriptor(mf['path'])==mf and descriptor(mp['path'])==mp,'original saved checkpoint changed')
    except Exception as failure:error=repr(failure)
    finally:
        try:mod.verify(tree,manifest)
        except Exception as failure:error='source verification: '+repr(failure)
        files=[p for p in out.rglob('*') if p.is_file()];stores={a:sum(p.stat().st_size for p in files if p.is_relative_to(out/a/'store')) for a in ARMS};metadata=sum(p.stat().st_size for p in files if not any(p.is_relative_to(out/a/'store') for a in ARMS));result=dict(status='failed' if error else 'passed',error=error,elapsed_seconds=time.monotonic()-start,results=results,work_summaries=summaries,bindings=bindings,binary=s['binary'],metadata_bytes=metadata,retained_store_bytes=stores,scope='Same16K input/config/cuts/four threads; independent stochastic constructions. Fixed tape supplies causal dispatch evidence, runtime differences are descriptive. Sorted split-read locking changes allowed scheduling; source split rows and remaining prefix suffix/predicate work charged. Existing reassign/current/register checks unchanged. Local actor POC, no physical network/distributed claim. Full cached vectors/joined save/audit/publication remain. External deadline/guard required; no retry/scalechange/queryretuning.')
        projected=metadata+len(json.dumps(result,indent=2).encode())+1
        if projected>50*1024**2 or any(v>512*1024**2 for v in stores.values()):result.update(status='failed',error='observed evidence/store cap')
        write(out/'result.json',result)
    return result
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--spec',required=True);a=p.parse_args();v=run(json.loads(Path(a.spec).read_text()));print(json.dumps(v));raise SystemExit(v['status']!='passed')
