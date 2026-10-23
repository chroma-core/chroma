"""Restore pinned sources and one verified owner into a fresh task directory."""
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import time

ROOT=Path('/mnt/spann/preparation-attribution-v1')
def sha(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
    return h.hexdigest()
def check_guard():
    assert time.time()+60 < json.loads((ROOT/'deadline.json').read_text())['deadline_unix']
    subprocess.run(['systemctl','is-active','--quiet','spann-preparation-v1-stop.timer'],check=True)
def write(name,value): (ROOT/name).write_text(json.dumps(value,indent=2)+'\n')

check_guard()
if sys.argv[1:] == ['source']:
    m=json.loads((ROOT/'source-manifest.json').read_text())
    assert sha(ROOT/'source.tar.gz')==m['archive_sha256']
    assert shutil.disk_usage(ROOT).free>12*1024**3
    dest=ROOT/'source'
    assert not dest.exists()
    # Workspace metadata references unused crates too; retain their existing files
    # in a private copy and overwrite only pinned paths before compilation.
    shutil.copytree('/mnt/spann/component-replay-v1/source',dest,ignore=shutil.ignore_patterns('.git','target','__pycache__'))
    with tarfile.open(ROOT/'source.tar.gz') as t:
        seen=set()
        for member in t:
            name=PurePosixPath(member.name)
            assert not name.is_absolute() and '..' not in name.parts and member.isfile()
            assert member.name in m['files'] and member.name not in seen
            seen.add(member.name)
            path=dest/member.name
            assert not path.is_symlink() and path.resolve().is_relative_to(dest.resolve())
            path.parent.mkdir(parents=True,exist_ok=True)
            with t.extractfile(member) as src,path.open('wb') as out:shutil.copyfileobj(src,out)
            assert sha(path)==m['files'][member.name]
        assert seen==set(m['files'])
    write('source-verified.json',dict(files=len(seen),source_archive_sha256=m['archive_sha256'],base='Private copy of retained driver source; exact 917 pinned paths verified; build dependency closure still subject to review.'))
else:raise ValueError('source required')
