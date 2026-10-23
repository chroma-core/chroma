"""Independent admission and completion gate for new full-corpus reference queries.

This module never treats its own retained candidates as independent ground truth.
It first checks fresh integration receipts and independently scans three complete
query/corpus pairs before atomically publishing new reference output.
"""
import json
import time
from pathlib import Path
import numpy as np
from .tree_manifest import artifact,checked_artifact,canonical_bytes,_atomic_write,file_hash
from .reference_device_engine import screened_reference,preflight_chunks

RUNTIME_KEYS=('torch','cuda','device','capability','tf32_matmul','tf32_cudnn','matmul_precision','workspace')


def independent_oracle(chunks,queries,dimension,k,block_rows=4096):
    """Score every source row in float64, then globally sort without screening."""
    total=sum(c for _,_,c in chunks);preflight_chunks(chunks,dimension,total)
    if k>total:raise ValueError('corpus smaller than k')
    answers=[]
    for query in queries:
        q=np.asarray(query['vector'],dtype=np.float64)
        if q.shape!=(dimension,) or not np.isfinite(q).all():raise ValueError('invalid independent query')
        scores=np.empty(total,dtype=np.float64)
        for path,start,count in chunks:
            x=np.memmap(path,dtype='<f4',mode='r',shape=(count,dimension))
            try:
                for offset in range(0,count,block_rows):
                    values=np.array(x[offset:offset+block_rows],dtype=np.float64)
                    if not np.isfinite(values).all():raise ValueError('invalid independent source')
                    scores[start+offset:start+offset+len(values)]=1-np.sum(values*q,axis=1,dtype=np.float64)
            finally:x._mmap.close()
        ids=np.arange(total,dtype=np.uint64);order=np.lexsort((ids,scores))[:k]
        answers.append(dict(query_id=query['query_id'],ids=ids[order].tolist(),distances=scores[order].tolist()))
    return answers


def compare(rows,expected,k):
    index={r['query_id']:r for r in expected}
    if len(index)!=len(expected) or len({r['query_id'] for r in rows})!=len(rows):raise ValueError('duplicate reference identities')
    for row in rows:
        raw=index[row['query_id']]
        other=dict(ids=raw['ids'][:k],distances=raw['distances'][:k])
        for value in (row,other):
            ids=value['ids'];distances=value['distances']
            if len(ids)!=k or len(distances)!=k or any(type(i) is not int or not 0<=i<2**64 for i in ids) or len(set(ids))!=k:
                raise ValueError('exact k distinct u64 IDs and distances required')
            if any(type(d) not in (int,float) for d in distances) or not np.isfinite(distances).all():raise ValueError('finite numeric distances required')
        if row['ids']!=other['ids'][:k] or not np.allclose(row['distances'],other['distances'][:k],rtol=0,atol=1e-12):
            raise ValueError('independent full-corpus reference mismatch')


def integration_gate(receipt_paths,chunks,runtime,expected_hashes,k=100):
    """Verify actual completed 1M/10M CPU comparisons from this exact code/runtime."""
    if len(receipt_paths)!=2 or len(expected_hashes)!=2:raise ValueError('both integration corpus receipts required')
    result=[]
    for path,rows,expected_hash in zip(receipt_paths,(1000000,10000000),expected_hashes):
        path=Path(path);report=json.loads(path.read_text())
        if report.get('status')!='passed' or report['metrics']['corpus_rows']!=rows:
            raise ValueError('missing passed full-corpus integration')
        if report['metrics']['full_score_checks']<3*rows:raise ValueError('integration did not check three complete queries')
        if any(report['runtime'].get(key)!=runtime.get(key) for key in RUNTIME_KEYS):raise ValueError('integration runtime differs')
        if runtime.get('tf32_matmul') is not False or runtime.get('tf32_cudnn') is not False or runtime.get('matmul_precision')!='highest':
            raise ValueError('strict FP32 runtime required')
        declared=report['source_chunks'];selected=[];covered=0
        for source in chunks:
            if covered==rows:break
            selected.append(artifact(Path(source[0])));covered+=source[2]
        if covered!=rows or [(x['sha256'],x['bytes']) for x in selected]!=[(x['sha256'],x['bytes']) for x in declared]:raise ValueError('integration source prefix differs')
        for producer in report['producer_sources']:
            current=Path(__file__).with_name(Path(producer['path']).name)
            if file_hash(current)!=producer['sha256']:raise ValueError('integration engine epoch differs')
        required={'reference_device_engine.py','reference_device_filter.py','reference_bounds.py','reference_gpu.py'}
        if {Path(p['path']).name for p in report['producer_sources']}!=required:raise ValueError('incomplete engine source binding')
        expected_path=checked_artifact(report['expected'],path.parent)
        if file_hash(expected_path)!=expected_hash:raise ValueError('CPU reference pin differs')
        queries=checked_artifact(report['queries'],path.parent)
        query_rows=[json.loads(line) for line in queries.read_text().splitlines()]
        expected=[json.loads(line) for line in expected_path.read_text().splitlines()]
        actual_path=checked_artifact(report['output'],path.parent)
        actual=[json.loads(line) for line in actual_path.read_text().splitlines()]
        if len(query_rows)!=200 or len(expected)!=200 or len(actual)!=200 or {r['query_id'] for r in query_rows}!={r['query_id'] for r in expected}:raise ValueError('complete200 integration identities required')
        compare(actual,expected,k);result.append(dict(receipt=artifact(path),query_ids=[r['query_id'] for r in query_rows]))
    return result


def certified_scan(chunks,queries,dimension,k,scorer,output,receipts,expected_hashes,
                   backend=None,to_numpy=None):
    """Publish only after all-source numeric checks and independent new-query oracles."""
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    started=time.monotonic();report=dict(status='running');runtime=None
    try:
        total=sum(count for _,_,count in chunks);preflight_chunks(chunks,dimension,total)
        if total!=10000000 or len(queries)<3 or len({q['query_id'] for q in queries})!=len(queries):
            raise ValueError('full10M source and distinct query identities required')
        runtime=scorer.metadata()
        report['integrations']=integration_gate(receipts,chunks,runtime,expected_hashes,k)
        old_ids={qid for receipt in report['integrations'] for qid in receipt['query_ids']}
        if any(q['query_id'] in old_ids for q in queries):raise ValueError('publication queries overlap the200 integration pool')
        from .reference_gpu import pathology_checks
        class FullScores:
            def begin_block(self,x):scorer.begin_block(x)
            def score(self,q):return (to_numpy or np.asarray)(scorer.score(q))
        report['pathologies']=pathology_checks(FullScores())
        # Independent CPU work happens before the device result exists.
        began=time.monotonic();oracle=independent_oracle(chunks,queries[:3],dimension,k)
        report['independent_oracle_wall_s']=time.monotonic()-began
        _atomic_write(output/'independent-oracle.json',canonical_bytes(dict(method='full-corpus float64 elementwise products/sum and global distance-ID lexsort',answers=oracle)))
        values,metrics=screened_reference(chunks,queries,dimension,k,scorer,backend=backend,to_numpy=to_numpy,full_score_queries=3)
        if metrics['corpus_rows']!=total or metrics['full_score_checks']!=3*total:raise ValueError('full corpus or numeric check coverage differs')
        compare(values[:3],oracle,k)
        if len(values)!=len(queries) or [r['query_id'] for r in values]!=[r['query_id'] for r in queries]:raise ValueError('query output coverage differs')
        path=output/'exact.jsonl';_atomic_write(path,''.join(json.dumps(v)+'\n' for v in values).encode())
        report.update(status='passed',metrics=metrics,queries=len(queries),output=artifact(path,output),runtime=runtime,wall_s=time.monotonic()-started,
            oracle=artifact(output/'independent-oracle.json',output),scope='certified conservative retention with full-source FP32 envelope checks on three queries and independent full-corpus new-query CPU checks; no published full-corpus parity claim')
        _atomic_write(output/'complete.json',canonical_bytes(report));return report
    except BaseException as error:
        report.update(status='failed',error=repr(error),wall_s=time.monotonic()-started);raise
    finally:_atomic_write(output/'report.json',canonical_bytes(report))


def main():
    import argparse
    from .benchmark import read_jsonl
    from .publication_gpu_job import admit_work
    from .reference_device_engine import CudaDeviceScorer
    from .reference_device_filter import memory_projection,admit_memory
    from .reference_gpu import pathology_checks
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('prepared','queries','receipt1m','receipt10m','cpu1m','cpu10m','output'):p.add_argument('--'+key,required=True)
    p.add_argument('--stop-epoch',required=True,type=float);a=p.parse_args()
    root=Path(a.output);root.mkdir(parents=True,exist_ok=False)
    started=time.monotonic();report=dict(status='running');scorer=None
    try:
        if a.stop_epoch-time.time()<600:raise TimeoutError('insufficient remaining numeric-pilot budget')
        prepared=Path(a.prepared);tree=json.loads(prepared.read_text())
        if tree.get('metric')!='ip' or tree.get('dimension')!=768 or tree.get('global_row_count')!=10000000:raise ValueError('full10M preserved FineWeb IP source required')
        chunks=[(checked_artifact(c['vectors'],prepared.parent),c['start_id'],c['row_count']) for c in tree['source']['chunks']]
        preflight_chunks(chunks,768,10000000)
        queries=read_jsonl(a.queries)
        if len(queries)!=11000 or len({q['query_id'] for q in queries})!=11000:raise ValueError('complete distinct11000 query pool required')
        report.update(prepared=artifact(prepared),queries=artifact(Path(a.queries)),producer_sources=[artifact(Path(__file__)),artifact(Path(__file__).with_name('publication_gpu_job.py'))])
        scorer=CudaDeviceScorer();runtime=scorer.metadata()
        receipts=[a.receipt1m,a.receipt10m];pins=[file_hash(a.cpu1m),file_hash(a.cpu10m)]
        report['integrations']=integration_gate(receipts,chunks,runtime,pins)
        report['memory_admission']=admit_memory(memory_projection(768,32768,32,len(queries),100),int(scorer.torch.cuda.mem_get_info()[0]))
        class FullScores:
            def begin_block(self,x):scorer.begin_block(x)
            def score(self,q):return scorer.to_numpy(scorer.score(q))
        report['pathologies']=pathology_checks(FullScores())
        begin=time.monotonic();oracle=independent_oracle(chunks,queries[:3],768,100)
        report['pilot_oracle_s']=time.monotonic()-begin
        scorer.torch.cuda.reset_peak_memory_stats();begin=time.monotonic()
        values,metrics=screened_reference(chunks,queries[:32],768,100,scorer,backend=scorer.backend,to_numpy=scorer.to_numpy)
        elapsed=time.monotonic()-begin;compare(values[:3],oracle,100)
        report['pilot']=dict(metrics=metrics,wall_s=elapsed,runtime=scorer.metadata(),independent_oracle=oracle)
        _atomic_write(root/'pilot.json',canonical_bytes(report))
        # This deliberately overestimates fixed full-score-check overhead instead of
        # extrapolating matrix kernel time. A failed admission preserves the pilot.
        report['admission']=admit_work(elapsed/32,len(queries),a.stop_epoch-time.time(),reserve_s=300+report['pilot_oracle_s']*1.5)
        scorer.torch.cuda.reset_peak_memory_stats()
        completed=certified_scan(chunks,queries,768,100,scorer,root/'certified',receipts,pins,backend=scorer.backend,to_numpy=scorer.to_numpy)
        report.update(status='passed',completion=artifact(root/'certified/complete.json',root),wall_s=time.monotonic()-started)
        _atomic_write(root/'complete.json',canonical_bytes(report))
    except BaseException as error:
        report.update(status='failed',error=repr(error),wall_s=time.monotonic()-started);raise
    finally:
        try:report['runtime']=scorer.metadata() if scorer else dict(initialization_failed=True)
        except BaseException as error:report['runtime']=dict(metadata_failed=True,error=repr(error))
        _atomic_write(root/'report.json',canonical_bytes(report))

if __name__=='__main__':main()
