"""Optimized synthetic selection kernel and separate real saved-query parity."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import statistics
import struct
import subprocess
import time
from .dynamic_runtime import descriptor
from .measure_rerank_pins import inventory
from .subtree_transport import LocalBackend,TreeCoordinator
from .test_dynamic_trie_query import logical_trace
from .tree_manifest import checked_artifact,load_model


def main():
    parser=argparse.ArgumentParser()
    for key in ('fixture','binary','reference-binary','kernel','manifest','expected-inventory','distribution','output'):parser.add_argument('--'+key,type=Path,required=True)
    for key in ('binary','reference-binary','kernel','distribution'):parser.add_argument('--expected-'+key+'-sha256',required=True)
    a=parser.parse_args();repo=Path(__file__).resolve().parents[2];root=a.fixture.resolve();out=a.output.resolve();sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();pins=json.loads(a.manifest.read_bytes())['files'];before=inventory(root)
    if root==out or root in out.parents or out in root.parents:raise ValueError('overlapping output')
    if any(sha(getattr(a,key.replace('-','_')))!=getattr(a,'expected_'+key.replace('-','_')+'_sha256') for key in ('binary','reference-binary','kernel','distribution')) or before!=json.loads(a.expected_inventory.read_bytes()) or {p:sha(repo/p) for p in pins}!=pins:raise ValueError('admission identity differs')
    distribution=json.loads(a.distribution.read_bytes());values=sorted(row['unique_candidates_scanned'] for row in distribution['rows'])
    if len(values)!=1689 or set(distribution['candidate_budget_values'])!={1000}:raise ValueError('exact recorded U/M distribution required')
    quantiles={name:values[index] for name,index in [('min',0),('p25',(len(values)-1)//4),('p50',(len(values)-1)//2),('p75',3*(len(values)-1)//4),('p95',95*(len(values)-1)//100),('max',len(values)-1)]};sizes=sorted(set(quantiles.values()))
    receipt=json.loads((root/'receipt.json').read_bytes());deployment=json.loads(checked_artifact(receipt['append_bundle'],'.').read_bytes())['deployment'];tree=deployment['tree']
    if tree['dimension']!=32 or tree['source']['row_count']!=98 or len(tree['subtrees'])!=2:raise ValueError('retained98x32 two-owner fixture required')
    mappings={s['subtree_id']:[r[0] for r in struct.iter_unpack('<Q',checked_artifact(s['local_to_global'],'.').read_bytes())] for s in tree['subtrees']}
    out.mkdir(parents=True,exist_ok=False);(out/'before.json').write_text(json.dumps(before,indent=2)+'\n');raw=[];proof=[];backends={};children=[];cleanup=[];started=time.monotonic();result=dict(status='failed',kernel_scope='generated finite score arrays at recorded U quantiles, not retained full candidate scores; optimized helper only, no workload-weighted query speedup',native_scope='unoptimized actual saved-index full-query parity and stage diagnostics, not latency benchmark',quantiles=quantiles,distribution_sha256=sha(a.distribution))
    def stop(*_):raise TimeoutError('five-minute selection measurement deadline')
    previous=signal.signal(signal.SIGALRM,stop);signal.alarm(300)
    previous_env={k:os.environ.get(k) for k in ('TOKIO_WORKER_THREADS','RAYON_NUM_THREADS')};os.environ.update(TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2')
    try:
        command=[str(a.kernel.resolve()),','.join(map(str,sizes))]
        with (out/'kernel.stdout').open('xb') as stdout,(out/'kernel.stderr').open('xb') as stderr:process=subprocess.run(command,stdout=stdout,stderr=stderr,timeout=120)
        if process.returncode or (out/'kernel.stdout').stat().st_size>8*1024*1024:raise ValueError('optimized paired kernel failed')
        kernel=[json.loads(line) for line in (out/'kernel.stdout').read_text().splitlines()]
        if len(kernel)!=len(sizes)*3*7*2:raise ValueError('kernel repetition coverage differs')
        summaries=[]
        for size in sizes:
            for shape in ('generated-spread','generated-tie-heavy','all-ties'):
                modes={str(mode):[row for row in kernel if row['size']==size and row['shape']==shape and row['partition']==mode] for mode in (False,True)}
                summaries.append(dict(size=size,shape=shape,full_sort_median_ns=statistics.median(r['kernel_ns'] for r in modes['False']),partition_median_ns=statistics.median(r['kernel_ns'] for r in modes['True']),full_sorted_rows=modes['False'][0]['sorted_rows'],partition_sorted_rows=modes['True'][0]['sorted_rows'],retained_rows=modes['True'][0]['retained_rows'],scope='seven paired AB/BA samples, clones/generation/dedup excluded; raw clone_ns retained'))
        for mode in ('reference','full-sort-inclusive','partition-inclusive'):
            descriptors={s['subtree_id']:descriptor(a.reference_binary if mode=='reference' else a.binary,deployment['owners'][s['subtree_id']]['checkpoint_dir'],s) for s in tree['subtrees']}
            if mode!='reference':
                for value in descriptors.values():value['posting_selection_policy']=mode
            backend=LocalBackend(descriptors,timeout=5);backends[mode]=backend
            for sid,pinned in backend.readers.items():
                children.append((mode,sid,pinned.reader.process));original=pinned.reader.query
                def observed(payload,*args,_mode=mode,_sid=sid,_call=original,**kwargs):
                    response=_call(payload,*args,**kwargs);raw.append(dict(mode=_mode,subtree_id=_sid,request=payload,response=response));return response
                pinned.reader.query=observed
        coordinators={mode:TreeCoordinator(tree,load_model(tree),backend,mappings) for mode,backend in backends.items()}
        for index,(query,budget) in enumerate((([47.4,1.,-0.]+[0.]*29,1),([48.,4097.,-0.]+[0.]*29,16),([48.,4097.,-0.]+[0.]*29,16),([47.4,1.,-0.]+[0.]*29,98))):
            order=('reference','full-sort-inclusive','partition-inclusive') if index%2==0 else ('reference','partition-inclusive','full-sort-inclusive');rows={mode:coordinators[mode].query(query,min(4,budget),beam_width=None,candidate_budget=budget,timeout=5) for mode in order}
            if any(logical_trace(rows[mode])!=logical_trace(rows['reference']) for mode in rows):raise ValueError('full saved-query trace differs')
            proof.append(dict(query=query,budget=budget,order=order,results=rows))
        for mode in list(backends):
            backend=backends.pop(mode);backend.close();cleanup.extend(dict(mode=m,subtree_id=sid,returncode=p.returncode) for m,sid,p in children if m==mode)
        if any(row['returncode'] is None for row in cleanup):raise ValueError('child terminality unknown')
        rss=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss;rss_bytes=rss if os.uname().sysname=='Darwin' else rss*1024
        if rss_bytes>2*1024**3:raise ValueError('observed child highwater exceeded')
        result.update(status='passed',kernel_summaries=summaries,kernel_exitcode=process.returncode,queries=proof,terminal_children_maxrss_bytes=rss_bytes,remaining_costs=['same code-distance scoring for every current copy','same minimum map and all-U Vec capacity','same load/version checks and quantization','cutoff ties may retain U; no unconditional M bound','globalID mapping/order and exact refinement unchanged'])
    except BaseException as error:result['error']=repr(error);raise
    finally:
        for mode in list(backends):
            backend=backends.pop(mode)
            try:backend.close();cleanup.extend(dict(mode=m,subtree_id=sid,returncode=p.returncode) for m,sid,p in children if m==mode)
            except BaseException as error:result.update(status='failed-cleanup',cleanup_error=repr(error))
        signal.alarm(0);signal.signal(signal.SIGALRM,previous)
        for key,value in previous_env.items():
            if value is None:os.environ.pop(key,None)
            else:os.environ[key]=value
        try:
            after=inventory(root);(out/'after.json').write_text(json.dumps(after,indent=2)+'\n');result['inventory_unchanged']=after==before;result['end_source_hashes']={p:sha(repo/p) for p in pins}
            if after!=before or result['end_source_hashes']!=pins:result['status']='failed-reconciliation'
        except BaseException as error:result.update(status='failed-reconciliation',reconciliation_error=repr(error))
        result.update(seconds=time.monotonic()-started,cleanup=cleanup);(out/'native.json').write_text(json.dumps(raw,indent=2)+'\n');data=json.dumps(result,indent=2)+'\n';files=[p for p in out.rglob('*') if p.is_file()]
        if len(data.encode())+sum(p.stat().st_size for p in files)>50*1024*1024:raise ValueError('observed output budget')
        (out/'result.json').write_text(data)
    if result['status']!='passed':raise ValueError('measurement did not reconcile')

if __name__=='__main__':main()
