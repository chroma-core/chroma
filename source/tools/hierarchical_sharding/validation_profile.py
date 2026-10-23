"""Measure existing Python validation reads; this grants no admission authority."""
import os
from pathlib import Path
from unittest.mock import patch

class ValidationIO:
    """Count bytes returned by Path.open streams and fsync calls in one pass.

    This is a single-threaded diagnostic. Rust/provider reads, mmap, allocator
    memory and operating-system cache traffic are outside its counters.
    """
    def __init__(self):
        self.paths={};self.fds={};self._patches=[]
    def __enter__(self):
        original=Path.open;fsync=os.fsync;profile=self
        class Stream:
            def __init__(self,stream,path):self.stream=stream;self.path=path
            def __getattr__(self,key):return getattr(self.stream,key)
            def __enter__(self):self.stream.__enter__();return self
            def __exit__(self,*args):
                fd=self.stream.fileno()
                try:return self.stream.__exit__(*args)
                finally:profile.fds.pop(fd,None)
            def close(self):
                if self.stream.closed:return
                fd=self.stream.fileno()
                try:self.stream.close()
                finally:profile.fds.pop(fd,None)
            def read(self,*args):
                value=self.stream.read(*args);profile.paths[self.path]['read_bytes']+=len(value.encode(self.stream.encoding or 'utf-8')) if isinstance(value,str) else len(value);return value
            def readline(self,*args):
                value=self.stream.readline(*args);profile.paths[self.path]['read_bytes']+=len(value.encode(self.stream.encoding or 'utf-8')) if isinstance(value,str) else len(value);return value
            def readinto(self,buffer):
                size=self.stream.readinto(buffer);profile.paths[self.path]['read_bytes']+=size or 0;return size
            def __iter__(self):return self
            def __next__(self):
                value=self.readline()
                if not value:raise StopIteration
                return value
        def opened(path,*args,**kwargs):
            stream=original(path,*args,**kwargs);name=str(path.absolute());row=profile.paths.setdefault(name,dict(opens=0,read_bytes=0,fsyncs=0));row['opens']+=1;profile.fds[stream.fileno()]=name;return Stream(stream,name)
        def synced(fd):
            name=profile.fds.get(fd,'<directory-or-other-descriptor>');profile.paths.setdefault(name,dict(opens=0,read_bytes=0,fsyncs=0))['fsyncs']+=1;return fsync(fd)
        for target,replacement in ((patch.object(Path,'open',opened),None),(patch.object(os,'fsync',synced),None)):
            target.start();self._patches.append(target)
        return self
    def __exit__(self,*args):
        for target in reversed(self._patches):target.stop()
    def report(self):
        return dict(scope='Python Path.open returned bytes and fsync calls only; excludes Rust/provider/mmap/cache/RSS',paths=self.paths,total_read_bytes=sum(r['read_bytes'] for r in self.paths.values()),total_opens=sum(r['opens'] for r in self.paths.values()),total_fsyncs=sum(r['fsyncs'] for r in self.paths.values()))


def profile_full_record(record):
    """Run the unchanged full oracle and its existing shared-object durability."""
    from .dynamic_deployment import validate_record
    from .dynamic_runtime import durable_file
    from .tree_manifest import checked_artifact
    from . import ownership_trie
    with ValidationIO() as audit:result=validate_record(record)
    with ValidationIO() as durability:
        for desc in (record['tree']['model'],record['canonical_vectors']):durable_file(checked_artifact(desc,'.'))
        for owner in record.get('ownership_pages',{}).get('owners',[]):
            for page in owner['pages']:durable_file(Path(page['path']))
        if 'ownership_trie' in record:ownership_trie.durable(record['ownership_trie'])
    return dict(full_validation=result,validation_io=audit.report(),shared_object_durability_io=durability.report(),admission='diagnostic-only; no reused receipt or bypass')
