"""Publish a preparation handoff only after archive/restore and process reconciliation."""
import json
from pathlib import Path
from .tree_manifest import artifact,canonical_bytes,_atomic_write

def handoff(root,store):
    root=Path(root);pipeline=json.loads((root/'pipeline.json').read_text());guard=json.loads((root/'control/guard-output/report.json').read_text())
    if pipeline.get('status')!='passed' or pipeline.get('archive',{}).get('status')!='passed':raise ValueError('full preparation/archive/restore did not pass')
    if guard.get('status')!='passed' or len(guard.get('outcomes',[]))!=1 or any(x.get('status')!='ok' or x.get('returncode')!=0 or x.get('unreconciled_children') for x in guard['outcomes']):raise ValueError('actual preparation process groups are not reconciled')
    files={name:store.upload(root/name) for name in ('job.json','pipeline.json','control/guard-output/report.json','archive/catalog-object.json')}
    result=dict(status='passed',scope='Complete100M preparation/references, every archived unique object fully restored, and owned process groups reconciled.',proof_objects=files)
    path=root/'handoff.json'
    if path.exists():raise FileExistsError('handoff is immutable')
    _atomic_write(path,canonical_bytes(result));descriptor=store.upload(path);_atomic_write(root/'handoff-object.json',canonical_bytes(descriptor));return result

def main():
    import argparse,boto3
    from .artifact_store import ArtifactStore
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--job',required=True);a=p.parse_args();job=json.loads(Path(a.job).read_text());s=job['artifact_store']
    store=ArtifactStore(s['bucket'],s['account'],boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),s['role']);print(json.dumps(handoff(job['owned_root'],store)))
if __name__=='__main__':main()
