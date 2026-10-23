"""One opt-in validation-preserving lazy before-scrub diagnostic."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
from harness_common import descriptor, verify, write, require

POLICIES=(('lazy',True),)

def run(spec):
    require(time.time()<spec['deadline_unix'],'expired external deadline')
    require(spec['rows']==8192 and spec['dimension']==768 and spec['balance_every']==4096,'fixed tiny extent')
    require(type(spec['threads']) is int and 1<=spec['threads']<=8,'thread cap')
    require(spec['arm_seconds']==30 and spec['total_seconds']==30,'fixed single-arm compute cap')
    binary=verify(spec['binary']);receipt=json.loads(verify(spec['restore_receipt']).read_text())
    require(receipt['status']=='complete' and receipt['canonical']==spec['vectors'],'source binding')
    require(receipt['rows']==2000000 and receipt['dimension']==768 and receipt['expected_bytes']==6144000000,'source cohort')
    source=Path(spec['vectors']['path']);require(source.is_file() and source.stat().st_size==6144000000,'source extent')
    output=Path(spec['output']);output.mkdir(exist_ok=False)
    before=time.monotonic()
    with source.open('rb') as f:prefix=f.read(8192*768*4)
    require(len(prefix)==8192*768*4,'truncated prefix');sha=__import__('hashlib').sha256(prefix).hexdigest();del prefix
    write(output/'preparation.json',dict(prefix_bytes=8192*768*4,prefix_sha256=sha,elapsed_seconds=time.monotonic()-before,source_receipt=spec['restore_receipt'],scope='Immutable restored input assumed; only prefix read and hashed here.'))
    env=dict(os.environ,TOKIO_WORKER_THREADS=str(spec['threads']),RAYON_NUM_THREADS=str(spec['threads']),OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
    started=time.monotonic();results=[];failure=None
    try:
        for label,lazy in POLICIES:
            remaining=min(30-(time.monotonic()-started),spec['deadline_unix']-time.time());require(remaining>0,'diagnostic deadline')
            native=dict(vectors=spec['vectors'],prefix_sha256=sha,rows=8192,dimension=768,metric='ip',config=spec['config'],cell='authoritative',threads=spec['threads'],balance_every=4096,output=str(output/label),lazy_disabled_evaluation_trace=lazy,profile_before_scrub=True,lazy_disabled_before_scrub_trace=True)
            path=output/(label+'-spec.json');write(path,native);command=[str(binary),'build-speed-diagnostic','--spec',str(path)];start=time.monotonic()
            try:
                with (output/(label+'.stdout')).open('xb') as out,(output/(label+'.stderr')).open('xb') as err:process=subprocess.run(command,env=env,stdout=out,stderr=err,timeout=min(30,remaining))
            except subprocess.TimeoutExpired:
                write(output/(label+'-process.json'),dict(command=command,status='timeout',elapsed_seconds=time.monotonic()-start));raise
            proc=dict(command=command,returncode=process.returncode,elapsed_seconds=time.monotonic()-start);write(output/(label+'-process.json'),proc);require(process.returncode==0,'native failed: '+label)
            value=json.loads((output/label/'result.json').read_text());require(json.loads((output/(label+'.stdout')).read_text())==value,'stdout/result differ')
            require(value['spec']==native and value['spec_sha256']==descriptor(path)['sha256'],'consumed spec binding')
            require(value['census']['unique_live_rows']==8192 and value['census']['duplicate_current_entries']==0 and value['census']['unreachable_nodes']==0,'saved membership')
            attr=value['cuts'][-1]['trace_attribution'];e=attr['evaluation_events']
            require(attr['lazy']==lazy and attr['trace_enabled'] is False and type(e) is int and e>0,'trace policy/count')
            require(attr['elided_reference_checks']==(e if lazy else 0) and attr['retained_reference_checks']==(0 if lazy else e),'single-owner trace command accounting')
            profile=value['cuts'][-1]['before_scrub_profile'];require(profile['enabled'] is True and profile['lazy_trace'] is True and profile['trace_enabled'] is False,'before-scrub policy echo')
            counts=profile['cumulative']
            require(all(type(v) is int and v>=0 for v in counts.values()),'invalid counts/spans')
            require(counts['trace_preparations']>0 and counts['physical_requests']==counts['semantic_scrub_calls'] and counts['physical_requests']<=counts['trace_preparations'],'span call accounting')
            require(counts['reference_queries']==counts['reference_successes']==0 and counts['physical_rows']==counts['batch_validated_rows'] and counts['batch_validation_calls']==counts['batch_validation_successes'] and 0<counts['batch_validation_calls']<=counts['physical_requests'] and counts['semantic_live_rows']<=counts['physical_rows'],'row/query accounting')
            results.append(dict(policy=label,result=descriptor(output/label/'result.json'),process=proc,trace_attribution=attr,before_scrub_profile=profile,actor_commands=value['cuts'][-1]['owner_counters']['0']['actor_commands']))
    except Exception as error:failure=str(error)
    finally:
        files=[p for p in output.rglob('*') if p.is_file()]
        stores=[p for p in files if any(p.is_relative_to(output/name/'store') for name,_ in POLICIES)]
        store_bytes=sum(p.stat().st_size for p in stores);metadata_bytes=sum(p.stat().st_size for p in files if p not in stores)
        if metadata_bytes>50*1024**2:failure='metadata exceeds50MiB'
        result=dict(status='passed' if failure is None else 'failed',error=failure,results=results,compute_elapsed_seconds=time.monotonic()-started,metadata_bytes=metadata_bytes,retained_store_bytes=store_bytes,binary=spec['binary'],prefix_sha256=sha,scope='One opt-in execution. Each physical ID retains location/current presence validation. Consecutive same-owner runs replace per-ID Reference requests; trace body is omitted only when disabled. Spans include waits/overhead, not CPU fractions. No causal speedup against independent stochastic baselines. Saved stores are separate from metadata.')
        write(output/'result.json',result)
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--spec',required=True);a=p.parse_args();result=run(json.loads(Path(a.spec).read_text()));print(json.dumps(result));raise SystemExit(result['status']!='passed')
