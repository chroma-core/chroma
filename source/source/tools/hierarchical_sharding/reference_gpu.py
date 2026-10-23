"""Full-prefix FP32 screening plus uncapped float64 candidate reranking.

CUDA execution is optional; tests inject a CPU scorer. A GPU run is valid only
with the recorded strict FP32 contract and passing full-score/reference checks.
"""
import argparse,json,os,platform,time,tempfile
from pathlib import Path
import numpy as np
from .benchmark import read_jsonl,write_jsonl
from .reference_bounds import fp32_ip_distance_error
from .tree_manifest import artifact,checked_artifact,canonical_bytes


class CudaScorer:
    def __init__(self):
        if os.environ.get('CUBLAS_WORKSPACE_CONFIG') not in (':4096:8',':16:8'):raise ValueError('pinned deterministic cuBLAS workspace required')
        import torch
        self.torch=torch
        if not torch.cuda.is_available():raise ValueError('CUDA device required')
        torch.backends.cuda.matmul.allow_tf32=False;torch.backends.cudnn.allow_tf32=False
        torch.set_float32_matmul_precision('highest')
        for key in ['allow_fp16_reduced_precision_reduction','allow_bf16_reduced_precision_reduction','allow_fp32_reduced_precision_reduction']:
            if hasattr(torch.backends.cuda.matmul,key):setattr(torch.backends.cuda.matmul,key,False)
        torch.use_deterministic_algorithms(True)
        if torch.backends.cuda.matmul.allow_tf32 or torch.backends.cudnn.allow_tf32:raise ValueError('TF32 disable failed')
        self.kernel_s=0.;self.transfer_s=0.
    def begin_block(self,x):
        t=self.torch;began=time.monotonic();self.x=t.tensor(x,device='cuda',dtype=t.float32);t.cuda.synchronize();self.transfer_s+=time.monotonic()-began
    def score(self,q):
        t=self.torch;began=time.monotonic();tensor=t.tensor(q,device='cuda',dtype=t.float32);t.cuda.synchronize();self.transfer_s+=time.monotonic()-began
        began=time.monotonic()
        with t.no_grad(),t.autocast(device_type='cuda',enabled=False):value=1-tensor@self.x.T
        t.cuda.synchronize();self.kernel_s+=time.monotonic()-began
        if value.dtype!=t.float32:raise ValueError('kernel output must be FP32')
        return value.cpu().numpy()
    def metadata(self):
        t=self.torch;p=t.cuda.get_device_properties(0)
        return dict(torch=t.__version__,cuda=t.version.cuda,device=p.name,capability=list(t.cuda.get_device_capability(0)),device_memory=p.total_memory,tf32_matmul=t.backends.cuda.matmul.allow_tf32,tf32_cudnn=t.backends.cudnn.allow_tf32,matmul_precision=t.get_float32_matmul_precision(),workspace=os.environ.get('CUBLAS_WORKSPACE_CONFIG'),kernel_s=self.kernel_s,host_to_device_s=self.transfer_s,gpu_peak_bytes=t.cuda.max_memory_allocated())


def screened_reference(chunks,queries,dimension,k,scorer,corpus_block=32768,query_block=32,full_score_queries=3):
    if min(dimension,k,corpus_block,query_block)<1 or not queries:raise ValueError('positive scan configuration required')
    q=np.asarray([r['vector'] for r in queries],dtype=np.float32)
    if q.shape!=(len(queries),dimension) or not np.isfinite(q).all():raise ValueError('invalid queries')
    tiny=np.finfo(np.float32).tiny
    if np.any((np.abs(q)>0)&(np.abs(q)<tiny)):raise ValueError('subnormal query input')
    q64=q.astype(np.float64);qnorm=np.linalg.norm(q64,axis=1)
    best=[(np.empty(0,dtype=np.uint64),np.empty(0,dtype=np.float64)) for _ in queries];upper_best=[np.empty(0,dtype=np.float64) for _ in queries]
    expected=0;retained=np.zeros(len(queries),dtype=np.int64);maximum_error=0.;full_checks=0;rerank_s=0.;started=time.monotonic()
    for path,start,count in chunks:
        if start!=expected or count<1 or Path(path).stat().st_size!=count*dimension*4:raise ValueError('chunks must cover a dense prefix')
        source=np.memmap(path,dtype='<f4',mode='r',shape=(count,dimension))
        for offset in range(0,count,corpus_block):
            x=np.array(source[offset:offset+corpus_block],dtype=np.float32)
            if not np.isfinite(x).all() or np.any((np.abs(x)>0)&(np.abs(x)<tiny)):raise ValueError('nonfinite or subnormal corpus input')
            x64=x.astype(np.float64);norms=np.linalg.norm(x64,axis=1);ids=np.arange(start+offset,start+offset+len(x),dtype=np.uint64)
            # Gate every norm pair BEFORE issuing this block's FP32 kernels.
            fp32_ip_distance_error(float(qnorm.max()),norms,dimension)
            scorer.begin_block(x)
            for qs in range(0,len(q),query_block):
                approximate=scorer.score(q[qs:qs+query_block])
                if approximate.dtype!=np.float32 or approximate.shape!=(min(query_block,len(q)-qs),len(x)) or not np.isfinite(approximate).all():raise ValueError('invalid strict-FP32 kernel output')
                for local,d in enumerate(approximate):
                    index=qs+local;errors=fp32_ip_distance_error(qnorm[index],norms,dimension);lower=np.nextafter(d.astype(np.float64)-errors,-np.inf);upper=np.nextafter(d.astype(np.float64)+errors,np.inf)
                    if index<full_score_queries:
                        independent=1-np.einsum('ij,j->i',x64,q64[index],optimize=False);difference=np.abs(independent-d)
                        if np.any(difference>errors):raise ValueError('FP32 error envelope violated')
                        maximum_error=max(maximum_error,float(difference.max()));full_checks+=len(x)
                    merged=np.concatenate((upper_best[index],upper));upper_best[index]=np.partition(merged,min(k,len(merged))-1)[:k];threshold=float(upper_best[index].max())
                    selected=np.flatnonzero(lower<=threshold);retained[index]+=len(selected)
                    began=time.monotonic();exact=1-np.einsum('ij,j->i',x64[selected],q64[index],optimize=False);oldids,oldd=best[index]
                    allids=np.concatenate((oldids,ids[selected]));alld=np.concatenate((oldd,exact));order=np.lexsort((allids,alld))[:k];best[index]=(allids[order],alld[order]);rerank_s+=time.monotonic()-began
        expected+=count
    if expected<k:raise ValueError('corpus smaller than k')
    rows=[dict(query_id=row['query_id'],ids=ids.tolist(),distances=d.tolist()) for row,(ids,d) in zip(queries,best)]
    return rows,dict(corpus_rows=expected,queries=len(queries),full_score_checks=full_checks,max_fp32_distance_error=maximum_error,retained_candidates_per_query=retained.tolist(),rerank_s=rerank_s,wall_s=time.monotonic()-started,tie_policy='ascending_float64_einsum_distance_then_global_id',retention_policy='all_lower_bounds_at_or_below_running_global_kth_upper; no candidate cap')


def pathology_checks(scorer):
    rng=np.random.default_rng(81);signed=rng.normal(size=(64,768)).astype('<f4');near=np.ones((64,768),dtype='<f4');near[::2,0]=np.nextafter(np.float32(1),np.float32(2))
    cases=[('signed_nonunit',signed),('all_cutoff_ties',np.ones((64,768),dtype='<f4')),('near_cutoff_ties',near),('internal_precision_probe',np.full((64,768),np.float32(1+2**-12),dtype='<f4'))];reports=[]
    with tempfile.TemporaryDirectory() as tmp:
        for name,x in cases:
            path=Path(tmp)/name;x.tofile(path);queries=[dict(query_id=str(i),vector=row.tolist()) for i,row in enumerate(x[:3])]
            rows,metrics=screened_reference([(path,0,len(x))],queries,768,16,scorer,corpus_block=17,query_block=2,full_score_queries=3)
            for row,q in zip(rows,queries):
                d=1-np.einsum('ij,j->i',x.astype(np.float64),np.asarray(q['vector'],dtype=np.float64),optimize=False);order=np.lexsort((np.arange(len(x)),d))[:16]
                if row['ids']!=order.tolist():raise ValueError('pathological-case ordering mismatch')
            reports.append(dict(case=name,status='passed',full_score_checks=metrics['full_score_checks']))
    return reports


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['prepared','queries','output','expected']:p.add_argument('--'+name,required=True)
    p.add_argument('--k',type=int,default=100);p.add_argument('--corpus-block',type=int,default=32768);p.add_argument('--query-block',type=int,default=32);a=p.parse_args()
    out=Path(a.output);out.mkdir(parents=True,exist_ok=False);prepared=Path(a.prepared);tree=json.loads(prepared.read_text());base=prepared.parent
    if tree.get('metric')!='ip' or tree.get('dimension')!=768:raise ValueError('this pilot requires preserved768D FineWeb IP input')
    chunks=[(checked_artifact(c['vectors'],base),c['start_id'],c['row_count']) for c in tree['source']['chunks']]
    expected_rows=tree.get('global_row_count')
    if type(expected_rows) is not int or expected_rows<1 or sum(c[2] for c in chunks)!=expected_rows:raise ValueError('source chunks do not cover declared prepared corpus count')
    queries=read_jsonl(a.queries);expected={r['query_id']:r for r in read_jsonl(a.expected)}
    if set(expected)!={q['query_id'] for q in queries} or len(expected)!=len(queries):raise ValueError('query/reference identities differ')
    scorer=None;report=dict(status='running',prepared=artifact(prepared),queries=artifact(Path(a.queries)),expected=artifact(Path(a.expected)),source_chunks=[artifact(Path(c[0])) for c in chunks],python=platform.python_version(),numpy=np.__version__)
    try:
        scorer=CudaScorer()
        report['pathological_cases']=pathology_checks(scorer)
        scorer.kernel_s=0.;scorer.transfer_s=0.;scorer.torch.cuda.reset_peak_memory_stats()
        rows,metrics=screened_reference(chunks,queries,tree['dimension'],a.k,scorer,a.corpus_block,a.query_block)
        maximum=0.
        for row in rows:
            reference=expected[row['query_id']]
            if row['ids']!=reference['ids'][:a.k]:raise ValueError('global top-k IDs differ from independent float64 reference')
            error=float(np.max(np.abs(np.asarray(row['distances'])-reference['distances'][:a.k])));maximum=max(maximum,error)
            if error>1e-12:raise ValueError('float64 rerank distances differ from reference')
        write_jsonl(out/'exact.jsonl',rows);report.update(status='passed',metrics=metrics,max_reference_distance_error=maximum,output=artifact(out/'exact.jsonl'))
    except Exception as e:report.update(status='failed',error_type=type(e).__name__,error=str(e));raise
    finally:
        report['runtime']=scorer.metadata() if scorer is not None else dict(initialization_failed=True,workspace=os.environ.get('CUBLAS_WORKSPACE_CONFIG'));(out/'report.json').write_bytes(canonical_bytes(report))
        print(json.dumps(dict(status=report['status'],report=str(out/'report.json'))),flush=True)

if __name__=='__main__':main()
