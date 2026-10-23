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
