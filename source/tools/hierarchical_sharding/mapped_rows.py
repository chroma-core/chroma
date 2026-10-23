"""Copy bounded rows without retaining a file mapping in the caller."""
from contextlib import contextmanager
import numpy as np


@contextmanager
def mapped_rows(path,shape):
    """The caller must copy slices before leaving this scope."""
    value=np.memmap(path,dtype='<f4',mode='r',shape=shape)
    try:yield value
    finally:value._mmap.close()


def copy_rows(path,shape,indices):
    with mapped_rows(path,shape) as source:
        return np.array(source[indices],dtype='<f4',copy=True)


def relative_rows(global_ids,start,count):
    """Subtract a source start without NumPy signed/unsigned float promotion."""
    ids=np.asarray(global_ids)
    if ids.dtype.kind not in ('i','u') or np.any(ids<0) or not isinstance(start,(int,np.integer)) or not isinstance(count,(int,np.integer)):
        raise ValueError('integer IDs and source bounds required')
    start=int(start);count=int(count)
    if start<0 or count<1 or start+count>2**64 or count>np.iinfo(np.intp).max:raise ValueError('unsupported source bounds')
    unsigned=ids.astype(np.uint64)
    if np.any(unsigned<np.uint64(start)) or np.any(unsigned-np.uint64(start)>=count):raise ValueError('global ID outside source chunk')
    return (unsigned-np.uint64(start)).astype(np.intp)
