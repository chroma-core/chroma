"""Conservative candidate intervals for a future full-corpus FP32 reference scan.

This helper does not implement a GPU engine. Its FP32 bound assumes IEEE binary32
products and accumulation, round-to-nearest, no underflow/overflow, and no TF32,
FP16, or quantized arithmetic. Runtime conformance must establish that contract.
"""
import numpy as np


def fp32_ip_distance_error(query_norm, vector_norms, dimension):
    """Bound score error using gamma(2*d), Cauchy-Schwarz, and 1-dot rounding."""
    q=float(query_norm);x=np.asarray(vector_norms,dtype=np.float64)
    if dimension<1 or 2*dimension*2**-24>=1 or not np.isfinite(q) or q<0 or not np.isfinite(x).all() or np.any(x<0):
        raise ValueError('finite nonnegative norms and supported dimension required')
    unit=2**-24;gamma=2*dimension*unit/(1-2*dimension*unit)
    # Norms computed in f64 are rounded upward with additional slack. This also
    # covers the much smaller f64 rerank error at the interval boundary.
    product=q*x*(1+1e-12)
    return gamma*product+2*unit*(1+product)+1e-12


def interval_candidates(distances, errors, k):
    """Retain every ID that can enter top-k under the supplied error bounds."""
    distances=np.asarray(distances,dtype=np.float64);errors=np.broadcast_to(np.asarray(errors,dtype=np.float64),distances.shape)
    if distances.ndim!=1 or not 1<=k<=len(distances) or not np.isfinite(distances).all() or not np.isfinite(errors).all() or np.any(errors<0):
        raise ValueError('finite distance/error vectors and valid k required')
    lower=np.nextafter(distances-errors,-np.inf);upper=np.nextafter(distances+errors,np.inf)
    threshold=float(np.partition(upper,k-1)[k-1])
    return np.flatnonzero(lower<=threshold),threshold


def validate_and_rerank(vectors, query, approximate_distances, global_ids, k):
    """CPU conformance helper; a large engine must retain the same intervals."""
    x=np.asarray(vectors,dtype=np.float32);q=np.asarray(query,dtype=np.float32);ids=np.asarray(global_ids,dtype=np.uint64)
    if x.ndim!=2 or q.shape!=(x.shape[1],) or ids.shape!=(len(x),) or len(np.unique(ids))!=len(ids) or not np.isfinite(x).all() or not np.isfinite(q).all():
        raise ValueError('finite source values and unique global IDs required')
    bounds=fp32_ip_distance_error(np.linalg.norm(q.astype(np.float64)),np.linalg.norm(x.astype(np.float64),axis=1),x.shape[1])
    selected,threshold=interval_candidates(approximate_distances,bounds,k)
    exact=1-x[selected].astype(np.float64)@q.astype(np.float64)
    order=np.lexsort((ids[selected],exact))[:k]
    return dict(ids=ids[selected][order].tolist(),distances=exact[order].tolist(),retained_candidates=len(selected),threshold=threshold)
