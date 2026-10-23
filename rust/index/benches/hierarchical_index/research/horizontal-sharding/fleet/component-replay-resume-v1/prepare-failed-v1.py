"""Restore pinned sources and one verified owner into a fresh task directory."""
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import time

ROOT=Path('/mnt/spann/component-replay-v1')
def sha(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
    return h.hexdigest()
def check_guard():
    assert time.time()+60 < json.loads((ROOT/'deadline.json').read_text())['deadline_unix']
    subprocess.run(['systemctl','is-active','--quiet','spann-component-v1-stop.timer'],check=True)
def write(name,value): (ROOT/name).write_text(json.dumps(value,indent=2)+'\n')

check_guard()
if sys.argv[1:] == ['source']:
    m=json.loads((ROOT/'source-manifest.json').read_text())
    assert sha(ROOT/'source.tar.gz')==m['archive_sha256']
    dest=ROOT/'source'
    assert not dest.exists()
    # Workspace metadata references unused crates too; retain their existing files
    # in a private copy and overwrite only pinned paths before compilation.
    shutil.copytree('/mnt/spann/source',dest,ignore=shutil.ignore_patterns('.git','target','__pycache__'))
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
    write('source-verified.json',dict(files=len(seen),source_archive_sha256=m['archive_sha256'],base='Private copy of retained driver source; exact 892 pinned paths verified; build dependency closure still subject to review.'))
elif sys.argv[1:] == ['owner']:
    proof=json.loads((ROOT/'archive-proof.json').read_text())['artifact']
    members=json.loads((ROOT/'owner-members.json').read_text())
    archive=ROOT/'worker1-checkpoints.tar'
    assert not archive.exists() and not (ROOT/'restored').exists()
    assert shutil.disk_usage(ROOT).free > proof['bytes']+12*1024**3
    subprocess.run(['aws','--region','us-east-1','s3api','get-object','--bucket',proof['bucket'],
       '--expected-bucket-owner',proof['expected_bucket_owner'],'--key',proof['key'],
       '--version-id',proof['version_id'],str(archive)],check=True,timeout=900,
       stdout=(ROOT/'download-response.json').open('w'))
    check_guard()
    assert archive.stat().st_size==proof['bytes'] and sha(archive)==proof['sha256']
    dest=ROOT/'restored';dest.mkdir();seen=set()
    with tarfile.open(archive,'r:') as t:
        for member in t:
            name=PurePosixPath(member.name)
            assert not name.is_absolute() and '..' not in name.parts
            if member.isdir():continue
            assert member.isfile(), 'Archive links and special files are forbidden'
            if member.name not in members:continue
            assert member.name not in seen
            seen.add(member.name);expected=members[member.name]
            assert member.size==expected['bytes']
            path=dest/member.name;path.parent.mkdir(parents=True,exist_ok=True)
            with t.extractfile(member) as src,path.open('xb') as out:shutil.copyfileobj(src,out)
            assert sha(path)==expected['sha256']
    assert seen==set(members)
    assert sha(dest/'27/manifest-1.json')=='0d62eb96b71f873872c0685d0472c611e22e1f9a5ac9b6758f7bf8244aa589e7'
    write('owner-verified.json',dict(files=len(seen),logical_bytes=sum(v['bytes'] for v in members.values()),archive_bytes=archive.stat().st_size,archive_sha256=proof['sha256'],version_id=proof['version_id']))
else:raise ValueError('source or owner required')
