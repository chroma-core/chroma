"""Bounded compact result preservation; never includes corpus or model bytes."""
import hashlib,json,tarfile,time
from pathlib import Path
from .tree_manifest import artifact,canonical_bytes,_atomic_write


def inventory(root):
    rows={}
    for p in sorted(Path(root).rglob('*')):
        if p.is_symlink():raise ValueError('result links prohibited')
        if p.is_file():rows[str(p.relative_to(root))]=artifact(p)
    if not rows or len(rows)>512 or sum(d['bytes'] for d in rows.values())>256*1024**2:raise ValueError('compact result archive cap exceeded')
    return rows


def preserve(root,output,store):
    root=Path(root);output=Path(output)
    if root==output or root in output.parents or output in root.parents:raise ValueError('archive/source scopes overlap')
    output.mkdir(exist_ok=False);report=dict(status='running');_atomic_write(output/'report.json',canonical_bytes(report))
    try:
        members=inventory(root);archive=output/'results.tar.gz'
        with tarfile.open(archive,'w:gz',format=tarfile.USTAR_FORMAT) as t:
            for name,d in members.items():
                info=tarfile.TarInfo(name);info.size=d['bytes'];info.mode=0o600;info.mtime=0
                with (root/name).open('rb') as f:t.addfile(info,f)
        if inventory(root)!=members:raise ValueError('results changed during export')
        descriptor=store.upload(archive);restored=store.restore(descriptor,output/'restored.tar.gz');seen=set()
        with tarfile.open(restored,'r:gz') as t:
            for m in t:
                if m.type!=tarfile.REGTYPE or m.name not in members or m.name in seen or m.size!=members[m.name]['bytes']:raise ValueError('unexpected restored result member')
                h=hashlib.sha256();count=0
                with t.extractfile(m) as f:
                    for block in iter(lambda:f.read(65536),b''):h.update(block);count+=len(block)
                if count!=members[m.name]['bytes'] or h.hexdigest()!=members[m.name]['sha256']:raise ValueError('restored result member differs')
                seen.add(m.name)
        if seen!=set(members):raise ValueError('incomplete restored results')
        report.update(status='passed',artifact=descriptor,members=members,scope='All compact numeric outputs/source/runtime/failure evidence, full private version restore and every-member SHA; no corpus bytes.')
    except BaseException as error:report.update(status='failed',error=repr(error));raise
    finally:_atomic_write(output/'report.json',canonical_bytes(report))
    descriptor=store.upload(output/'report.json');store.restore(descriptor,output/'restored-report.json');return dict(status='passed',proof=descriptor,archive=report['artifact'])


def private_store():
    import boto3
    from botocore.config import Config
    from .artifact_store import ArtifactStore
    cfg=Config(connect_timeout=5,read_timeout=20,retries=dict(max_attempts=2,mode='standard'))
    return ArtifactStore('spann-artifacts-079819354890-20261014-ecfcca4d','079819354890',boto3.client('s3',region_name='us-east-1',config=cfg),boto3.client('sts',region_name='us-east-1',config=cfg),'spann-artifact-ecfcca4d')
