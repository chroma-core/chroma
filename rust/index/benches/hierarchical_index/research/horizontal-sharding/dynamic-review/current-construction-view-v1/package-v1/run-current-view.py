"""Paired fixed-access construction embedding-view diagnostic."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
from harness_common import descriptor, verify, write, require

POLICIES=(('historical',False),('current',True))

def run(spec):
    require(time.time()<spec['deadline_unix'],'expired external deadline')
    require(spec['rows']==8192 and spec['dimension']==768 and spec['balance_every']==8192,'fixed tiny extent')
    require(type(spec['threads']) is int and 1<=spec['threads']<=8,'thread cap')
    require(spec['arm_seconds']==30 and spec['total_seconds']==60,'fixed paired compute cap')
    binary=verify(spec['binary']);receipt=json.loads(verify(spec['restore_receipt']).read_text())
    require(receipt['status']=='complete' and receipt['canonical']==spec['vectors'],'source binding')
    require(receipt['rows']==2000000 and receipt['dimension']==768 and receipt['expected_bytes']==6144000000,'source cohort')
    source=Path(spec['vectors']['path']);require(source.is_file() and source.stat().st_size==6144000000,'source extent')
    output=Path(spec['output']);output.mkdir(exist_ok=False)
    before=time.monotonic()
    with source.open('rb') as f:prefix=f.read(8192*768*4)
    require(len(prefix)==8192*768*4,'truncated prefix');sha=__import__('hashlib').sha256(prefix).hexdigest();expected=__import__('hashlib').sha256()
    for _ in range(2):
        for id in range(8192):expected.update(id.to_bytes(4,'little'));expected.update(prefix[id*3072:(id+1)*3072])
    expected_probe_sha=expected.hexdigest();del prefix
    write(output/'preparation.json',dict(prefix_bytes=8192*768*4,prefix_sha256=sha,elapsed_seconds=time.monotonic()-before,source_receipt=spec['restore_receipt'],scope='Immutable restored input assumed; only prefix read and hashed here.'))
    env=dict(os.environ,TOKIO_WORKER_THREADS=str(spec['threads']),RAYON_NUM_THREADS=str(spec['threads']),OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
    started=time.monotonic();results=[];failure=None
    try:
        for label,lazy in POLICIES:
            remaining=min(60-(time.monotonic()-started),spec['deadline_unix']-time.time());require(remaining>0,'diagnostic deadline')
            native=dict(vectors=spec['vectors'],prefix_sha256=sha,rows=8192,dimension=768,metric='ip',config=spec['config'],cell='authoritative',threads=spec['threads'],balance_every=8192,output=str(output/label),lazy_disabled_evaluation_trace=True,profile_before_scrub=True,lazy_disabled_before_scrub_trace=True,current_embedding_view=lazy,storage_chunk_rows=256,vector_probe_rounds=2)
            path=output/(label+'-spec.json');write(path,native);command=[str(binary),'build-speed-diagnostic','--spec',str(path)];start=time.monotonic()
            try:
                with (output/(label+'.stdout')).open('xb') as out,(output/(label+'.stderr')).open('xb') as err:process=subprocess.run(command,env=env,stdout=out,stderr=err,timeout=min(30,remaining))
            except subprocess.TimeoutExpired:
                write(output/(label+'-process.json'),dict(command=command,status='timeout',elapsed_seconds=time.monotonic()-start));raise
            proc=dict(command=command,returncode=process.returncode,elapsed_seconds=time.monotonic()-start);write(output/(label+'-process.json'),proc);require(process.returncode==0,'native failed: '+label)
            value=json.loads((output/label/'result.json').read_text());require(json.loads((output/(label+'.stdout')).read_text())==value,'stdout/result differ')
            require(value['spec']==native and value['spec_sha256']==descriptor(path)['sha256'],'consumed spec binding')
            require(value['census']['unique_live_rows']==8192 and value['census']['duplicate_current_entries']==0 and value['census']['unreachable_nodes']==0,'saved membership')
            cut=value['cuts'][-1];probe=cut['probe'];require(len(value['cuts'])==1 and probe['rounds']==2 and probe['ids_per_round']==8192,'fixed access shape')
            require(cut['vector_reads_after_balance'] is None,'probe interval must end before balance')
            for side in ['reads_before','reads_after']:
                for owner in probe[side].values():
                    for reader in owner['readers']:require(reader['access_overflow'] is False,'invalid block observation')
            before=probe['counters_before']['0'];after=probe['counters_after']['0']
            opens=after['vector_root_opens']-before['vector_root_opens'];clears=after['vector_reader_cache_clears']-before['vector_reader_cache_clears']
            require(after['vector_gets']-before['vector_gets']==16384,'access count')
            require(after['embedding_view_publications']==int(lazy),'publication count')
            results.append(dict(policy=label,result=descriptor(output/label/'result.json'),process=proc,probe=probe,probe_root_opens=opens,probe_reader_cache_clears=clears,publication_seconds=cut['publication_seconds']))
        require(len(results)==2 and results[0]['probe']['result_sha256']==results[1]['probe']['result_sha256']==expected_probe_sha,'fixed access bits differ')
        require(results[0]['probe_root_opens']>32 and results[0]['probe_reader_cache_clears']>0,'historical churn not reproduced')
        require(results[1]['probe_root_opens']<=1 and results[1]['probe_reader_cache_clears']==0,'published view not reused')
    except Exception as error:failure=str(error)
    finally:
        files=[p for p in output.rglob('*') if p.is_file()]
        stores=[p for p in files if any(p.is_relative_to(output/name/'store') for name,_ in POLICIES)]
        store_bytes=sum(p.stat().st_size for p in stores);metadata_bytes=sum(p.stat().st_size for p in files if p not in stores)
        if metadata_bytes>50*1024**2:failure='metadata exceeds50MiB'
        result=dict(status='passed' if failure is None else 'failed',error=failure,results=results,compute_elapsed_seconds=time.monotonic()-started,metadata_bytes=metadata_bytes,retained_store_bytes=store_bytes,binary=spec['binary'],prefix_sha256=sha,expected_probe_sha256=expected_probe_sha,scope='Paired identical pre-balance access sequence. Independent stochastic balancing is correctness-only, not a causal time comparison. Probe observer ends before balance. One opt-in execution. Embedding view publication changes only root selection for admitted existing embedding keys; centroid roots/membership versions stay separate. Spans include waits/overhead, not CPU fractions. No causal speedup against independent stochastic baselines. Saved stores are separate from metadata.')
        write(output/'result.json',result)
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--spec',required=True);a=p.parse_args();result=run(json.loads(Path(a.spec).read_text()));print(json.dumps(result));raise SystemExit(result['status']!='passed')
