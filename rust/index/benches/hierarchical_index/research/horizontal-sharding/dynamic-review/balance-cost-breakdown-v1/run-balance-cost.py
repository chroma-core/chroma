"""One bounded balance-attribution arm, using the retained restored prefix."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
from harness_common import descriptor, verify, write, require

def validate_cost(cost):
    categories=cost['categories'];total=sum(v['exclusive_ns'] for v in categories.values())
    require(cost['valid'] is True and total>0 and total==cost['exclusive_sum_ns']==cost['observed_window_ns']==cost['cli_balance_wall_ns']+cost['observation_boundary_extra_ns'],'exclusive clock reconciliation')
    for stage in ['split_input_preparation','clustering','neighbor_discovery','eligibility_scrub_trace','structural_install_merge']:require(categories[stage]['entries']>0,'missing category work: '+stage)
    for kind in ['split','discovery','owner-evaluation','neighbor-begin']:require(cost['work_event_deltas'].get(kind,0)>0,'missing construction work: '+kind)
    require(cost['shape_after']['nodes']>1 and cost['shape_after']['leaves']>1,'unrepresentative no-split shape')
    return cost,categories,total

def run(spec):
    require(time.time()<spec['deadline_unix'],'expired external deadline')
    require(spec['rows']==8192 and spec['dimension']==768 and spec['threads']==2,'fixed diagnostic shape')
    require(spec['arm_seconds']==30,'fixed child timeout')
    binary=verify(spec['binary']);receipt=json.loads(verify(spec['restore_receipt']).read_text())
    require(receipt['status']=='complete' and receipt['canonical']==spec['vectors'],'restored source binding')
    source=Path(spec['vectors']['path']);require(source.is_file() and source.stat().st_size==spec['vectors']['bytes']==6144000000,'source extent')
    output=Path(spec['output']);output.mkdir(exist_ok=False)
    start=time.monotonic()
    with source.open('rb') as f:prefix=f.read(8192*768*4)
    require(len(prefix)==8192*768*4,'truncated prefix');sha=hashlib.sha256(prefix).hexdigest();del prefix
    write(output/'preparation.json',dict(prefix_bytes=8192*768*4,prefix_sha256=sha,elapsed_seconds=time.monotonic()-start,scope='Exact restored immutable input assumed; prefix hashed, no corpus growth.'))
    native=dict(vectors=spec['vectors'],prefix_sha256=sha,rows=8192,dimension=768,metric='ip',config=spec['config'],cell='authoritative',threads=2,balance_every=8192,output=str(output/'profiled'),lazy_disabled_evaluation_trace=True,profile_before_scrub=True,lazy_disabled_before_scrub_trace=True,current_embedding_view=True,storage_chunk_rows=256,vector_probe_rounds=0,phased_owner_workers=2,split_preparation_workers=1,balance_cost_observation=True)
    path=output/'profiled-spec.json';write(path,native);command=[str(binary),'build-speed-diagnostic','--spec',str(path)]
    env=dict(os.environ,TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
    started=time.monotonic();failure=None;report=None
    try:
        remaining=min(30,spec['deadline_unix']-time.time());require(remaining>0,'external deadline expired')
        with (output/'profiled.stdout').open('xb') as out,(output/'profiled.stderr').open('xb') as err:process=subprocess.run(command,env=env,stdout=out,stderr=err,timeout=remaining)
        write(output/'profiled-process.json',dict(command=command,returncode=process.returncode,elapsed_seconds=time.monotonic()-started));require(process.returncode==0,'native diagnostic failed')
        value=json.loads((output/'profiled/result.json').read_text());require(value==json.loads((output/'profiled.stdout').read_text()),'stdout/result mismatch')
        require(value['spec']==native and value['spec_sha256']==descriptor(path)['sha256'],'consumed spec mismatch')
        census=value['census'];require(census['unique_live_rows']==8192 and census['duplicate_current_entries']==0 and census['unreachable_nodes']==0,'saved census')
        require(len(value['cuts'])==1,'one balance cut required');cut=value['cuts'][0];cost,categories,total=validate_cost(cut['balance_cost'])
        require(cut['construction_phase']['status']=='Ready' and cut['construction_phase']['valid'] is True,'phase not complete')
        require(cut['split_preparation']['calls']>0 and cut['split_preparation']['provider_obtains']==cut['split_preparation']['rows'],'preparation counters')
        for state in [cut['storage_after_add'],cut['storage_after_balance']]:require(state['overflowed'] is False,'storage observation invalid')
        for owner in cut['vector_reads_after_balance'].values():
            require(owner['valid'] is True,'vector observation invalid')
            for reader in owner['readers']:require(reader['access_overflow'] is False,'vector block observation overflow')
        report=dict(balance_cost=cost,category_elapsed_fractions={k:v['exclusive_ns']/total for k,v in categories.items()},split_preparation=cut['split_preparation'],owner_counters=cut['owner_counters'],before_scrub=cut['before_scrub_profile'],trace=cut['trace_attribution'],construction_phase=cut['construction_phase'],storage_after_add=cut['storage_after_add'],storage_after_balance=cut['storage_after_balance'],vector_reads_after_balance=cut['vector_reads_after_balance'],census=census)
    except Exception as error:failure=repr(error)
    finally:
        files=[p for p in output.rglob('*') if p.is_file()];stores=[p for p in files if p.is_relative_to(output/'profiled/store')]
        metadata=sum(p.stat().st_size for p in files if p not in stores);store_bytes=sum(p.stat().st_size for p in stores)
        if metadata>50*1024**2:failure='metadata exceeds50MiB'
        result=dict(status='passed' if failure is None else 'failed',error=failure,binary=spec['binary'],prefix_sha256=sha,compute_elapsed_seconds=time.monotonic()-started,metadata_bytes=metadata,retained_store_bytes=store_bytes,report=report,scope='One stochastic8192-row arm for elapsed attribution, not causal speedup. Nested exclusive categories include actor waits/observer overhead and sum to observed window; signed boundary offset reconciles CLI balance wall. Input/setup/add/publication/reshard/save/audit/root metadata remain separate in raw native result. Storage counters are successful encoded returns, not physical IO. Saved stores excluded from metadata50MiB cap; direct child timeout does not imply process-group/resource control.')
        write(output/'result.json',result)
    return result
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--spec',required=True);a=parser.parse_args();result=run(json.loads(Path(a.spec).read_text()));print(json.dumps(result));raise SystemExit(result['status']!='passed')
