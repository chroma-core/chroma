"""Separate batched conservative reference epoch; no automatic GPU launch or CLI.

The deployment caller pins input bytes, validates strict FP32 arithmetic and
compares independent full-corpus oracle queries before publishing any output.
"""
import time
from pathlib import Path
import numpy as np
from .reference_bounds import fp32_ip_distance_error
from .reference_batched_filter import BatchedRunningFilter,NumpyBatchedIntervals


def batched_reference(chunks,queries,dimension,k,scorer,*,expected_rows,backend=None,to_numpy=None,
                      corpus_block=32768,query_block=32,full_score_queries=3):
    """Scan every corpus row; transfer uncapped coordinates once per query batch.

    Only endpoints and kth-upper selection are batched. Exact CPU reranking stays
    one query at a time so all-tie candidates cannot multiply vector workspace.
    """
    for value in (dimension,k,expected_rows,corpus_block,query_block,full_score_queries):
        if type(value) is not int or value<1:raise ValueError('positive integer scan configuration required')
    if query_block>32 or not queries or expected_rows<k:raise ValueError('invalid bounded batch or corpus smaller than k')
    query_ids=[r['query_id'] for r in queries]
    if any(not isinstance(x,str) or not x for x in query_ids) or len(set(query_ids))!=len(query_ids):raise ValueError('unique nonempty query IDs required')
    end=0
    for path,start,count in chunks:
        if type(start) is not int or type(count) is not int or count<1 or start!=end or Path(path).stat().st_size!=count*dimension*4:raise ValueError('dense exact-sized source chunks required')
        end+=count
    if end!=expected_rows or end>2**64:raise ValueError('declared corpus coverage differs')
    backend=backend or NumpyBatchedIntervals();to_numpy=to_numpy or np.asarray
    q=np.asarray([r['vector'] for r in queries],dtype=np.float32);tiny=np.finfo(np.float32).tiny
    if q.shape!=(len(queries),dimension) or not np.isfinite(q).all() or np.any((np.abs(q)>0)&(np.abs(q)<tiny)):raise ValueError('finite normal-or-zero queries required')
    q64=q.astype(np.float64);normq=np.linalg.norm(q64,axis=1)
    filters=[BatchedRunningFilter(k,min(query_block,len(q)-first),backend) for first in range(0,len(q),query_block)]
    best=[(np.empty(0,dtype=np.uint64),np.empty(0,dtype=np.float64)) for _ in queries]
    retained=np.zeros(len(q),dtype=np.int64);checks=0;maximum_error=0.;started=time.monotonic()
    timing={name:0. for name in ['source_copy_and_bounds_wall_s','upload_source_wall_s','score_wall_s','full_validation_wall_s','filter_and_transfer_wall_s','CPU_rerank_wall_s']};batches=0;maximum_coordinates=0
    for path,start,count in chunks:
        source=np.memmap(path,dtype='<f4',mode='r',shape=(count,dimension))
        try:
            for offset in range(0,count,corpus_block):
                began=time.monotonic();x=np.array(source[offset:offset+corpus_block],dtype=np.float32)
                if not np.isfinite(x).all() or np.any((np.abs(x)>0)&(np.abs(x)<tiny)):raise ValueError('invalid source arithmetic range')
                x64=x.astype(np.float64);max_norm=float(np.linalg.norm(x64,axis=1).max())
                # Cauchy-Schwarz is symmetric: one certified vectorized call
                # supplies every query bound and rejects overflow before scoring.
                errors=fp32_ip_distance_error(max_norm,normq,dimension)
                timing['source_copy_and_bounds_wall_s']+=time.monotonic()-began
                began=time.monotonic();scorer.begin_block(x);timing['upload_source_wall_s']+=time.monotonic()-began
                for batch,qs in enumerate(range(0,len(q),query_block)):
                    size=min(query_block,len(q)-qs);began=time.monotonic();approximate=scorer.score(q[qs:qs+size]);timing['score_wall_s']+=time.monotonic()-began
                    if tuple(approximate.shape)!=(size,len(x)):raise ValueError('score shape mismatch')
                    # Designated complete checks bypass filtering, including all
                    # rejected rows. Their CPU/D2H work has a separate boundary.
                    check_count=min(size,max(0,min(full_score_queries,len(q))-qs))
                    if check_count:
                        began=time.monotonic();all_scores=np.asarray(to_numpy(approximate[:check_count]))
                        if all_scores.dtype!=np.float32 or all_scores.shape!=(check_count,len(x)) or not np.isfinite(all_scores).all():raise ValueError('invalid full-check scores')
                        for local in range(check_count):
                            exact=1-np.einsum('ij,j->i',x64,q64[qs+local],optimize=False);difference=np.abs(exact-all_scores[local])
                            if np.any(difference>errors[qs+local]):raise ValueError('full-score envelope violated')
                            checks+=len(x);maximum_error=max(maximum_error,float(difference.max()))
                        timing['full_validation_wall_s']+=time.monotonic()-began
                    began=time.monotonic();coordinates=np.asarray(to_numpy(filters[batch].select(approximate,errors[qs:qs+size])))
                    if coordinates.dtype.kind not in ('i','u') or coordinates.ndim!=2 or coordinates.shape[1]!=2 or np.any(coordinates<0) or np.any(coordinates[:,0]>=size) or np.any(coordinates[:,1]>=len(x)):raise ValueError('invalid selected coordinates')
                    # Verify strict row-major unique coordinates without sorting
                    # or allocating another whole coordinate matrix.
                    flat=coordinates[:,0].astype(np.int64)*len(x)+coordinates[:,1].astype(np.int64)
                    if np.any(flat[1:]<=flat[:-1]):raise ValueError('unordered or duplicate selected coordinates')
                    row_starts=np.searchsorted(coordinates[:,0],np.arange(size+1))
                    timing['filter_and_transfer_wall_s']+=time.monotonic()-began;batches+=1;maximum_coordinates=max(maximum_coordinates,len(coordinates))
                    began=time.monotonic()
                    for local in range(size):
                        index=qs+local;selected=coordinates[row_starts[local]:row_starts[local+1],1];retained[index]+=len(selected)
                        ids=np.asarray(start+offset+selected,dtype=np.uint64);exact=1-np.einsum('ij,j->i',x64[selected],q64[index],optimize=False)
                        old_ids,old_d=best[index];ids=np.concatenate((old_ids,ids));distances=np.concatenate((old_d,exact));order=np.lexsort((ids,distances))[:k];best[index]=(ids[order],distances[order])
                    timing['CPU_rerank_wall_s']+=time.monotonic()-began
        finally:source._mmap.close()
    rows=[dict(query_id=r['query_id'],ids=ids.tolist(),distances=d.tolist()) for r,(ids,d) in zip(queries,best)]
    return rows,dict(corpus_rows=end,full_score_checks=checks,max_error=maximum_error,retained_candidates_per_query=retained.tolist(),wall_s=time.monotonic()-started,phase_wall_s=timing,coordinate_transfers=batches,maximum_uncapped_coordinates=maximum_coordinates,query_block=query_block,corpus_block=corpus_block,timing_scope='Host wall boundaries; filter includes device completion and coordinate D2H when to_numpy synchronizes. Scorer must disclose synchronization. No kernel-only attribution.')


class CudaBatchedScorer:
    """Strict reviewed scorer runtime with the new batched interval probes."""
    def __init__(self):
        from .reference_gpu import CudaScorer
        from .reference_batched_filter import TorchBatchedIntervals,check_batched_conformance
        self.runtime=CudaScorer();self.torch=self.runtime.torch;self.backend=TorchBatchedIntervals()
        check_batched_conformance(self.backend,lambda x:self.torch.tensor(x,device='cuda',dtype=self.torch.float32),lambda x:x.detach().cpu().numpy())
    def begin_block(self,x):self.runtime.begin_block(x)
    def score(self,q):
        from .reference_device_engine import CudaDeviceScorer
        return CudaDeviceScorer.score(self,q)
    def to_numpy(self,value):
        from .reference_device_engine import CudaDeviceScorer
        return CudaDeviceScorer.to_numpy(self,value)
    def metadata(self):return self.runtime.metadata()
