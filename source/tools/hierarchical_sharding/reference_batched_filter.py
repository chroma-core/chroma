"""Optional query-batched conservative intervals; existing engines stay unchanged.

Each query keeps its own kth upper endpoint over every processed corpus row.
All current upper endpoints update that threshold before inclusive filtering.
Only selected (query,row) coordinates leave the device; retention is uncapped.
"""
import numpy as np


class NumpyBatchedIntervals:
    def empty(self, queries):return np.empty((queries,0),dtype=np.float64)
    def endpoints(self,scores,errors):
        if scores.dtype!=np.float32 or scores.ndim!=2 or not np.isfinite(scores).all():raise ValueError('finite matrix of float32 scores required')
        value=scores.astype(np.float64)
        return np.nextafter(value-errors[:,None],-np.inf),np.nextafter(value+errors[:,None],np.inf)
    def update(self,previous,upper,k):return np.sort(np.concatenate((previous,upper),axis=1),axis=1)[:,:k]
    def select(self,lower,upper,complete):
        threshold=upper[:,-1] if complete else np.full(len(lower),np.inf,dtype=np.float64)
        return np.argwhere(lower<=threshold[:,None])


class TorchBatchedIntervals:
    """Float64 row-wise endpoints, exact top-k and inclusive device comparisons."""
    def __init__(self,device='cuda'):
        import torch
        self.torch=torch;self.device=device
    def empty(self,queries):return self.torch.empty((queries,0),dtype=self.torch.float64,device=self.device)
    def endpoints(self,scores,errors):
        t=self.torch
        if scores.dtype!=t.float32 or scores.ndim!=2 or not bool(t.isfinite(scores).all()):raise ValueError('finite matrix of float32 scores required')
        # Upload one certified f64 bound per query; no narrowing or scalar sync.
        error=t.as_tensor(errors,dtype=t.float64,device=scores.device)[:,None]
        value=scores.to(t.float64)
        return t.nextafter(value-error,t.full_like(value,-float('inf'))),t.nextafter(value+error,t.full_like(value,float('inf')))
    def update(self,previous,upper,k):
        values=self.torch.cat((previous,upper),dim=1)
        return self.torch.topk(values,min(k,values.shape[1]),dim=1,largest=False,sorted=True).values
    def select(self,lower,upper,complete):
        threshold=upper[:,-1:] if complete else self.torch.full((lower.shape[0],1),float('inf'),dtype=self.torch.float64,device=lower.device)
        return self.torch.nonzero(lower<=threshold,as_tuple=False)


class BatchedRunningFilter:
    def __init__(self,k,queries,backend=None):
        if type(k) is not int or k<1 or type(queries) is not int or not 1<=queries<=32:raise ValueError('positive k and query batch at most32 required')
        self.k=k;self.queries=queries;self.backend=backend or NumpyBatchedIntervals();self.upper=self.backend.empty(queries);self.rows=0
    def select(self,scores,errors):
        errors=np.asarray(errors)
        if errors.dtype!=np.float64 or errors.shape!=(self.queries,) or not np.isfinite(errors).all() or np.any(errors<0):raise ValueError('one finite nonnegative certified f64 error per query required')
        if tuple(scores.shape[:1])!=(self.queries,) or len(scores.shape)!=2 or scores.shape[1]<1:raise ValueError('score batch shape differs')
        lower,upper=self.backend.endpoints(scores,errors)
        self.upper=self.backend.update(self.upper,upper,self.k);self.rows+=scores.shape[1]
        return self.backend.select(lower,self.upper,self.rows>=self.k)


def check_batched_conformance(backend,make_scores,to_numpy):
    """Compare complete histories, endpoint bits and uncapped coordinates."""
    from .reference_device_filter import RunningFilter,NumpyIntervals
    tiny=np.nextafter(np.float32(0),np.float32(1))
    values=np.array([-1.,-tiny,-0.,0.,tiny,1.,np.nextafter(np.float32(1),np.float32(2))],dtype=np.float32)
    for k in (1,5,20):
        for queries in (1,3,32):
            cpu=BatchedRunningFilter(k,queries);device=BatchedRunningFilter(k,queries,backend)
            scalar=[RunningFilter(k,NumpyIntervals()) for _ in range(queries)]
            for n,error in [(2,0.),(7,float(tiny)),(3,2**-53),(11,1.),(5,np.nextafter(1.,np.inf))]:
                scores=np.stack([np.resize(np.roll(values,i),n) for i in range(queries)])
                errors=np.full(queries,error,dtype=np.float64)
                a=cpu.select(scores,errors);b=np.asarray(to_numpy(device.select(make_scores(scores),errors)))
                if not np.array_equal(a,b):raise ValueError('batched device retention differs')
                if not np.array_equal(cpu.upper.view(np.uint64),np.asarray(to_numpy(device.upper),dtype=np.float64).view(np.uint64)):raise ValueError('batched device running upper values differ')
                for i in range(queries):
                    chosen=scalar[i].select(scores[i],float(errors[i]))
                    if not np.array_equal(a[a[:,0]==i,1],chosen):raise ValueError('batched/scalar retention differs')
                x=cpu.backend.endpoints(scores,errors);y=backend.endpoints(make_scores(scores),errors)
                if any(not np.array_equal(v.view(np.uint64),np.asarray(to_numpy(w),dtype=np.float64).view(np.uint64)) for v,w in zip(x,y)):raise ValueError('batched endpoint bits differ')
    return True


def batched_memory_projection(dimension,corpus_block,query_block,queries,k):
    if any(type(v) is not int or v<1 for v in (dimension,corpus_block,query_block,queries,k)) or query_block>32:raise ValueError('positive dimensions and query batch at most32 required')
    # Covers simultaneously live scores, conversion/endpoint/topk/concat/mask/
    # nonzero outputs even when every tie survives. Vendor workspace is unknown.
    terms=dict(source_f32=4*corpus_block*dimension,query_f32=4*query_block*dimension,
               matrix_temporaries_and_uncapped_coordinates=128*query_block*corpus_block,
               persistent_upper_and_replacement=16*queries*k,allocator_allowance=4096*queries)
    return dict(terms=terms,projected_peak_bytes=2*sum(terms.values())+2*1024**3,query_block=query_block,corpus_block=corpus_block,
                source_uploaded_once_per_corpus_block=True,scope='Planning allowance, not a vendor workspace bound; actual peak and fail-closed OOM report required.')
