import importlib
import shutil
import warnings
from typing import List

import fsspec
import fsspec.asyn
from fsspec.implementations.local import LocalFileSystem

from . import compression


COMPRESSION_FILESYSTEMS: List[compression.BaseCompressedFileFileSystem] = [
    compression.Bz2FileSystem,
    compression.GzipFileSystem,
    compression.Lz4FileSystem,
    compression.XzFileSystem,
    compression.ZstdFileSystem,
]

# Register custom filesystems
for fs_class in COMPRESSION_FILESYSTEMS:
    if fs_class.protocol in fsspec.registry and fsspec.registry[fs_class.protocol] is not fs_class:
        warnings.warn(f"A filesystem protocol was already set for {fs_class.protocol} and will be overwritten.")
    fsspec.register_implementation(fs_class.protocol, fs_class, clobber=True)


def is_remote_filesystem(fs: fsspec.AbstractFileSystem) -> bool:
    """
    Checks if `fs` is a remote filesystem.

    Args:
        fs (`fsspec.spec.AbstractFileSystem`):
            An abstract super-class for pythonic file-systems, e.g. `fsspec.filesystem(\'file\')` or `s3fs.S3FileSystem`.
    """
    return not isinstance(fs, LocalFileSystem)


def rename(fs: fsspec.AbstractFileSystem, src: str, dst: str):
    """
    Renames the file `src` in `fs` to `dst`.
    """
    if not is_remote_filesystem(fs):
        # LocalFileSystem.mv does copy + rm, it is more efficient to simply move a local directory
        shutil.move(fs._strip_protocol(src), fs._strip_protocol(dst))
    else:
        fs.mv(src, dst, recursive=True)
