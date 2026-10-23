"""Optional conservative device filter; independent of the tested reference engine.

The caller validates arithmetic, input ranges and CPU-certified error bounds
before scoring. Full-score check queries must bypass this component entirely.
"""
import numpy as np


class NumpyIntervals:
    """Exact endpoint/selection contract used by CPU conformance tests."""
    def endpoints(self, scores, error):
        scores=np.asarray(scores)
        if scores.dtype!=np.float32 or scores.ndim!=1 or not np.isfinite(scores).all():
            raise ValueError('finite one-dimensional float32 scores required')
        value=scores.astype(np.float64)
        return np.nextafter(value-error,-np.inf),np.nextafter(value+error,np.inf)

    def smallest(self, values, k):
        return np.sort(values)[:k]

    def select(self, lower, threshold):
        return np.flatnonzero(lower<=threshold)

    def combine(self, previous, current):
        return np.concatenate((previous,current))

    def empty(self):return np.empty(0,dtype=np.float64)
    def count(self, values):return len(values)
    def threshold(self, values):return float(values[-1])


class TorchIntervals:
    """CUDA float64 endpoints and exact selection; imports torch only on use."""
    def __init__(self, device='cuda'):
        import torch
        self.torch=torch;self.device=device

    def endpoints(self,scores,error):
        t=self.torch
        if scores.dtype!=t.float32 or scores.ndim!=1 or not bool(t.isfinite(scores).all()):
            raise ValueError('finite one-dimensional float32 scores required')
        value=scores.to(t.float64)
        return (t.nextafter(value-error,t.full_like(value,-float('inf'))),
                t.nextafter(value+error,t.full_like(value,float('inf'))))

    def smallest(self,values,k):
        return self.torch.topk(values,min(k,len(values)),largest=False,sorted=True).values

    def select(self,lower,threshold):return self.torch.nonzero(lower<=threshold).flatten()
    def combine(self,previous,current):return self.torch.cat((previous,current))
    def empty(self):return self.torch.empty(0,dtype=self.torch.float64,device=self.device)
    def count(self,values):return len(values)
    def threshold(self,values):return values[-1]  # Keep the threshold on device in f64.


class RunningFilter:
    """One query's global kth-upper threshold, with uncapped inclusive retention."""
    def __init__(self,k,backend=None):
        if type(k) is not int or k<1:raise ValueError('positive integer k required')
        self.k=k;self.backend=backend or NumpyIntervals();self.upper=self.backend.empty();self.rows=0

    def select(self,scores,error):
        # E is a CPU-certified scalar shared by this block; narrowing is forbidden.
        if not isinstance(error,(float,np.float64)) or not np.isfinite(error) or error<0:
            raise ValueError('finite nonnegative float64 scalar error required')
        lower,upper=self.backend.endpoints(scores,float(error))
        # ALL current upper endpoints participate before any row is excluded.
        self.upper=self.backend.smallest(self.backend.combine(self.upper,upper),self.k)
        self.rows+=self.backend.count(upper)
        threshold=float('inf') if self.rows<self.k else self.backend.threshold(self.upper)
        return self.backend.select(lower,threshold)


def check_endpoint_conformance(backend,make_scores,to_numpy):
    """Fail closed on device endpoint or selection differences, including rejects."""
    tiny=np.nextafter(np.float32(0),np.float32(1))
    scores=np.array([-1.,-tiny,-0.,0.,tiny,np.nextafter(np.float32(1),np.float32(2)),1.],dtype=np.float32)
    cpu=NumpyIntervals()
    for error in (0.,float(tiny),2**-53,1.,np.nextafter(1.,np.inf)):
        expected=cpu.endpoints(scores,error);actual=backend.endpoints(make_scores(scores),error)
        for a,b in zip(expected,actual):
            if not np.array_equal(a.view(np.uint64),np.asarray(to_numpy(b),dtype=np.float64).view(np.uint64)):
                raise ValueError('device outward endpoints differ from CPU contract')
        for threshold in np.concatenate(expected):
            selected=backend.select(actual[0],float(threshold))
            if not np.array_equal(cpu.select(expected[0],threshold),to_numpy(selected)):
                raise ValueError('device inclusive selection differs from CPU contract')
    histories=[[(scores[:2],0.),(scores[2:5],float(tiny)),(scores[5:],1.)],
               [(np.ones(2,dtype=np.float32),0.),(np.ones(9,dtype=np.float32),0.)],
               [(scores[::-1].copy(),2**-53),(scores.copy(),np.nextafter(1.,np.inf))]]
    for k in (1,5,20):
        for blocks in histories:
            expected=RunningFilter(k,cpu);actual=RunningFilter(k,backend)
            for values,error in blocks:
                a=expected.select(values,error);b=to_numpy(actual.select(make_scores(values),error))
                if not np.array_equal(a,b):raise ValueError('device running threshold differs from CPU contract')
                observed=np.asarray(to_numpy(actual.upper),dtype=np.float64)
                if not np.array_equal(expected.upper.view(np.uint64),observed.view(np.uint64)):
                    raise ValueError('device kth-upper values differ from CPU contract')
    return True
