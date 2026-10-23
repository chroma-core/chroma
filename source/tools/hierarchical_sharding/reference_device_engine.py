"""Optional device-filter scan; never selected by the tested reference engine."""
import time
from pathlib import Path
import numpy as np
from .reference_bounds import fp32_ip_distance_error
from .reference_device_filter import NumpyIntervals,RunningFilter,memory_projection,admit_memory


def screened_reference(chunks,queries,dimension,k,scorer,backend=None,to_numpy=None,
                       corpus_block=32768,query_block=32,full_score_queries=3):
    """Score every row, filter on its device, rerank uncapped candidates in f64.

    scorer.score returns a device matrix. to_numpy copies only selected indices,
    except complete designated check queries, which always bypass the filter.
    The deployment wrapper must run strict arithmetic and endpoint probes first.
    """
    if min(dimension,k,corpus_block,query_block)<1 or not queries or full_score_queries<1:
        raise ValueError('positive configuration and full-score checks required')
    backend=backend or NumpyIntervals();to_numpy=to_numpy or np.asarray
    q=np.asarray([r['vector'] for r in queries],dtype=np.float32)
    tiny=np.finfo(np.float32).tiny
    if q.shape!=(len(queries),dimension) or not np.isfinite(q).all() or np.any((np.abs(q)>0)&(np.abs(q)<tiny)):
        raise ValueError('finite normal-or-zero float32 queries required')
    q64=q.astype(np.float64);normq=np.linalg.norm(q64,axis=1)
    filters=[RunningFilter(k,backend) for _ in queries]
    best=[(np.empty(0,dtype=np.uint64),np.empty(0,dtype=np.float64)) for _ in queries]
    retained=np.zeros(len(q),dtype=np.int64);rows=0;checks=0;maximum_error=0.;started=time.monotonic();rerank_s=0.
    for path,start,count in chunks:
        if start!=rows or count<1 or Path(path).stat().st_size!=count*dimension*4:raise ValueError('dense exact-sized chunks required')
        source=np.memmap(path,dtype='<f4',mode='r',shape=(count,dimension))
        for offset in range(0,count,corpus_block):
            x=np.array(source[offset:offset+corpus_block],dtype=np.float32)
            if not np.isfinite(x).all() or np.any((np.abs(x)>0)&(np.abs(x)<tiny)):raise ValueError('invalid source arithmetic range')
            x64=x.astype(np.float64)
            # Upward inflation is applied inside the certified bound helper.
            max_norm=float(np.linalg.norm(x64,axis=1).max())
            fp32_ip_distance_error(float(normq.max()),np.array([max_norm]),dimension)
            scorer.begin_block(x)
            for qs in range(0,len(q),query_block):
                approximate=scorer.score(q[qs:qs+query_block])
                if tuple(approximate.shape)!=(min(query_block,len(q)-qs),len(x)):raise ValueError('score shape mismatch')
                for local in range(len(approximate)):
                    index=qs+local
                    error=float(fp32_ip_distance_error(normq[index],np.array([max_norm]),dimension)[0])
                    # Validate all rows, including rejected rows, independently.
                    if index<full_score_queries:
                        all_scores=np.asarray(to_numpy(approximate[local]))
                        if all_scores.dtype!=np.float32 or not np.isfinite(all_scores).all():raise ValueError('invalid full-check scores')
                        exact_all=1-np.einsum('ij,j->i',x64,q64[index],optimize=False)
                        difference=np.abs(exact_all-all_scores)
                        if np.any(difference>error):raise ValueError('full-score envelope violated')
                        checks+=len(x);maximum_error=max(maximum_error,float(difference.max()))
                    selected=np.asarray(to_numpy(filters[index].select(approximate[local],error)))
                    if selected.dtype.kind not in ('i','u') or selected.ndim!=1 or np.any(selected<0) or np.any(selected>=len(x)) or len(np.unique(selected))!=len(selected):raise ValueError('invalid selected indices')
                    retained[index]+=len(selected);began=time.monotonic()
                    ids=np.asarray(start+offset+selected,dtype=np.uint64)
                    exact=1-np.einsum('ij,j->i',x64[selected],q64[index],optimize=False)
                    old_ids,old_d=best[index];ids=np.concatenate((old_ids,ids));distances=np.concatenate((old_d,exact))
                    order=np.lexsort((ids,distances))[:k];best[index]=(ids[order],distances[order]);rerank_s+=time.monotonic()-began
        rows+=count
    if rows<k:raise ValueError('corpus smaller than k')
    return [dict(query_id=q['query_id'],ids=ids.tolist(),distances=d.tolist()) for q,(ids,d) in zip(queries,best)],dict(corpus_rows=rows,full_score_checks=checks,max_error=maximum_error,retained_candidates_per_query=retained.tolist(),wall_s=time.monotonic()-started,rerank_s=rerank_s)


class CudaDeviceScorer:
    """Strict existing runtime setup with device-resident score return values."""
    def __init__(self):
        from .reference_gpu import CudaScorer
        from .reference_device_filter import TorchIntervals,check_endpoint_conformance
        self.runtime=CudaScorer();self.torch=self.runtime.torch
        self.backend=TorchIntervals()
        # Includes top-k and complete streaming threshold histories.
        check_endpoint_conformance(self.backend,
            lambda x:self.torch.tensor(x,device='cuda',dtype=self.torch.float32),
            lambda x:x.detach().cpu().numpy())

    def begin_block(self,x):self.runtime.begin_block(x)

    def score(self,q):
        t=self.torch;began=time.monotonic()
        query=t.tensor(q,device='cuda',dtype=t.float32);t.cuda.synchronize()
        self.runtime.transfer_s+=time.monotonic()-began
        began=time.monotonic();start=t.cuda.Event(enable_timing=True);end=t.cuda.Event(enable_timing=True);start.record()
        with t.no_grad(),t.autocast(device_type='cuda',enabled=False):value=1-query@self.runtime.x.T
        end.record();t.cuda.synchronize();self.runtime.kernel_s+=time.monotonic()-began
        self.runtime.event_s+=start.elapsed_time(end)/1000
        if value.dtype!=t.float32 or not bool(t.isfinite(value).all()):raise ValueError('invalid strict FP32 output')
        return value

    def to_numpy(self,value):
        began=time.monotonic();result=value.detach().cpu().numpy()
        self.runtime.return_s+=time.monotonic()-began;return result

    def metadata(self):return self.runtime.metadata()


def preflight_chunks(chunks,dimension,expected_rows):
    """Validate declared full-prefix coverage before constructing a CUDA runtime."""
    if type(expected_rows) is not int or expected_rows<1 or dimension<1:raise ValueError('positive declared corpus required')
    end=0
    for path,start,count in chunks:
        if start!=end or count<1 or Path(path).stat().st_size!=count*dimension*4:raise ValueError('declared corpus coverage mismatch')
        end+=count
    if end!=expected_rows:raise ValueError('declared corpus count mismatch')
    return end


def main():
    import argparse,json,platform,resource
    from .benchmark import read_jsonl,write_jsonl
    from .tree_manifest import artifact,checked_artifact,canonical_bytes
    from .reference_gpu import pathology_checks
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('prepared','queries','expected','output'):parser.add_argument('--'+key,required=True)
    parser.add_argument('--k',type=int,default=100);a=parser.parse_args()
    out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
    report=dict(status='running',python=platform.python_version(),numpy=np.__version__);scorer=None
    try:
        began=time.monotonic();path=Path(a.prepared);tree=json.loads(path.read_text())
        if tree.get('metric')!='ip' or tree.get('dimension')!=768:raise ValueError('preserved FineWeb IP source required')
        chunks=[(checked_artifact(c['vectors'],path.parent),c['start_id'],c['row_count']) for c in tree['source']['chunks']]
        preflight_chunks(chunks,tree['dimension'],tree.get('global_row_count'))
        queries=read_jsonl(a.queries);references=read_jsonl(a.expected);expected={r['query_id']:r for r in references}
        if len(expected)!=len(references) or len(queries)!=len(expected) or {q['query_id'] for q in queries}!=set(expected):raise ValueError('query/reference identity mismatch')
        report.update(prepared=artifact(path),queries=artifact(Path(a.queries)),expected=artifact(Path(a.expected)),source_chunks=[artifact(Path(c[0])) for c in chunks],input_validation_wall_s=time.monotonic()-began)
        report['producer_sources']=[artifact(Path(__file__)),artifact(Path(__file__).with_name('reference_device_filter.py')),artifact(Path(__file__).with_name('reference_bounds.py')),artifact(Path(__file__).with_name('reference_gpu.py'))]
        projection=memory_projection(768,32768,32,len(queries),a.k)
        began=time.monotonic();scorer=CudaDeviceScorer();report['device_memory_admission']=admit_memory(projection,int(scorer.torch.cuda.mem_get_info()[0]));report['initialization_and_endpoint_probe_wall_s']=time.monotonic()-began
        # Existing adversarial scorer probes inspect the complete device scores.
        class FullScores:
            def begin_block(self,x):scorer.begin_block(x)
            def score(self,q):return scorer.to_numpy(scorer.score(q))
        report['pathologies']=pathology_checks(FullScores())
        scorer.runtime.kernel_s=0.;scorer.runtime.transfer_s=0.;scorer.runtime.return_s=0.;scorer.runtime.event_s=0.
        scorer.torch.cuda.reset_peak_memory_stats()
        usage=resource.getrusage(resource.RUSAGE_SELF)
        rows,metrics=screened_reference(chunks,queries,768,a.k,scorer,backend=scorer.backend,to_numpy=scorer.to_numpy)
        after=resource.getrusage(resource.RUSAGE_SELF)
        for row in rows:
            reference=expected[row['query_id']]
            if row['ids']!=reference['ids'][:a.k] or not np.allclose(row['distances'],reference['distances'][:a.k],rtol=0,atol=1e-12):raise ValueError('independent reference mismatch')
        write_jsonl(out/'exact.jsonl',rows)
        report.update(status='passed',metrics=metrics,output=artifact(out/'exact.jsonl'),scan_cpu_user_s=after.ru_utime-usage.ru_utime,scan_cpu_system_s=after.ru_stime-usage.ru_stime,lifetime_peak_rss_kib=after.ru_maxrss,cache_condition='hashes and probes warm OS/runtime; device cache uncontrolled')
    except Exception as error:
        report.update(status='failed',error_type=type(error).__name__,error=str(error));raise
    finally:
        try:report['runtime']=scorer.metadata() if scorer is not None else dict(initialization_failed=True)
        except Exception as metadata_error:report['runtime']=dict(metadata_failed=True,error_type=type(metadata_error).__name__,error=str(metadata_error))
        report['timing_scope']='wall includes endpoint/topk/selection; those phases are not independently isolated'
        (out/'report.json').write_bytes(canonical_bytes(report))

if __name__=='__main__':main()
