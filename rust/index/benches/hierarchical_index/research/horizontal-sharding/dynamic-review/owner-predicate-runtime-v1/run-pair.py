"""Paired reference-kernel construction; no lifecycle authority or automatic retries."""
import argparse, hashlib, json, os, subprocess, time
from pathlib import Path
from harness_common import verify,write,require,descriptor

def shape(report):
    require(report['schedule_sample_overflow'] is False,'incomplete schedule sample')
    assignments=report['last_reference_subtree_assignments']
    require(sum(bool(x) for x in assignments)>=2,'fewer than two independent assigned regions')
    require(report['peak_distinct_subtree_workers']>=2,'no actual subtree context overlap')
    require(report['placement_moves']>0 and sum(x['current']>0 for x in report['owners'])>=2,'ownership movement absent')
    require(report['pending_balance']==0,'unfinished balance')
    require(report['central_current_versions']==report['central_mutable_posting_rows']==0,'central mutation mirror')

def run(s):
    require(s['rows']==8192 and s['dimension']==768 and s['threads']==4,'fixed shape')
    require(s['arm_seconds']==30 and s['total_seconds']==60,'fixed limits')
    require(s['balance_every']==4096 and s['owner_slots']==4 and s['move_above']==1024,'fixed phase settings')
    require(s['config']['write_navigation']=='fp' and s['config']['fp_npa'] is True,'explicit FP both arms')
    require(time.time()<s['deadline_unix'],'expired external deadline')
    binary=verify(s['binary']);restore=json.loads(verify(s['restore_receipt']).read_text())
    require(restore['status']=='complete' and restore['canonical']==s['vectors'],'restore binding')
    source=Path(s['vectors']['path']);require(source.stat().st_size==s['vectors']['bytes']==6144000000,'source extent')
    out=Path(s['output']);out.mkdir(exist_ok=False)
    with source.open('rb') as f:prefix=f.read(8192*768*4)
    require(len(prefix)==8192*768*4,'truncated prefix');sha=hashlib.sha256(prefix).hexdigest();del prefix
    write(out/'preparation.json',dict(prefix_bytes=8192*768*4,prefix_sha256=sha,source=s['vectors'],restore_receipt=s['restore_receipt']))
    started=time.monotonic();results={};failure=None
    try:
        for arm in ['disabled','enabled']:
            native=dict(protocol='reference-owner-parallel-v1',source_id='retained-matched2m-first8192',vectors=s['vectors'],prefix_sha256=sha,rows=8192,dimension=768,metric='ip',config=s['config'],treatment='owner',predicate_batching=(arm=='enabled'),threads=4,balance_every=4096,owner_slots=4,move_above=1024,output=str(out/arm))
            p=out/(arm+'-spec.json');write(p,native)
            remaining=min(30,60-(time.monotonic()-started),s['deadline_unix']-time.time());require(remaining>0,'fixed deadline exhausted')
            command=[str(binary),'reference-owner-diagnostic','--spec',str(p)];t=time.monotonic()
            env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
            with (out/(arm+'.stdout')).open('xb') as stdout,(out/(arm+'.stderr')).open('xb') as stderr:r=subprocess.run(command,stdout=stdout,stderr=stderr,env=env,timeout=remaining)
            write(out/(arm+'-process.json'),dict(command=command,returncode=r.returncode,elapsed_seconds=time.monotonic()-t));require(r.returncode==0,'native arm failed')
            x=json.loads((out/arm/'result.json').read_text());require(x==json.loads((out/(arm+'.stdout')).read_text()),'stdout mismatch')
            require(x['spec']==native and x['spec_sha256']==descriptor(p)['sha256'],'spec binding')
            c=x['census'];require(c['unique_live_rows']==8192 and c['duplicate_current_entries']==c['unreachable_nodes']==0,'full census')
            require(len(x['phases'])==2,'common cuts missing');results[arm]=x
        require(results['disabled']['all_id_exact_fp_distance_bits']==results['enabled']['all_id_exact_fp_distance_bits'],'saved vector exact distances differ')
        for arm in ['disabled','enabled']:
            report=results[arm]['owner_report'];shape(report)
            require(report['predicate_batching']==(arm=='enabled'),'policy echo')
            require(report['live_version_requests']>0,'missing live version work')
            if arm=='disabled':require(report['predicate_batch_callbacks']==0 and report['owner_predicates']>0,'baseline dispatch work')
            else:require(report['predicate_batch_callbacks']>0 and report['predicate_batch_computed_rows']>0 and report['owner_predicates']==0,'batched dispatch work')
    except Exception as e:failure=repr(e)
    finally:
        files=[p for p in out.rglob('*') if p.is_file()];stores=[p for p in files if any(p.is_relative_to(out/a/'store') for a in ['disabled','enabled'])]
        metadata=sum(p.stat().st_size for p in files if p not in stores)
        if metadata>50*1024**2:failure='metadata exceeds50MiB'
        result=dict(status='passed' if failure is None else 'failed',error=failure,binary=s['binary'],prefix_sha256=sha,elapsed_seconds=time.monotonic()-started,metadata_bytes=metadata,retained_store_bytes=sum(p.stat().st_size for p in stores),results=results,scope='Independent stochastic schedules, fixed order disabled then enabled; no causal speedup/ANN quality claim. Context overlap includes waits; existing structural lock may serialize critical work. Bootstrap/input and full publication/audit separately reported. Successful encoded IO not physical IO; this entry has no new provider counters. Child timeout is not descendant/resource enforcement. External deadline/guard required; no retries.')
        write(out/'result.json',result)
    return result
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--spec',required=True);a=p.parse_args();r=run(json.loads(Path(a.spec).read_text()));print(json.dumps(r));raise SystemExit(r['status']!='passed')
