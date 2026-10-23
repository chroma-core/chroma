"""Preserve only completed archival controls and diagnostic metadata on root EBS and S3."""
import argparse,pathlib,json,hashlib,tarfile,time,sys
import boto3
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.artifact_store import ArtifactStore

def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8*1024**2),b''):h.update(b)
 return h.hexdigest()
def run(n):
 root=pathlib.Path('/mnt/spann');out=pathlib.Path('/var/lib/spann-results/archive-control-evidence-v1');out.mkdir(parents=True,exist_ok=False);files=set();states={}
 names=['checkpoint-archive-v1','checkpoint-archive-v2','checkpoint-archive-gen2-v1','checkpoint-archive-gen2-worker0-v1','failure-log-archive-v1']
 for name in names:
  base=root/name
  if not base.exists():continue
  for p in base.glob('*'):
   if p.is_file() and p.suffix in ('.json','.log','.py'):files.add(p)
  files.update(p for p in (base/'producer').glob('*.py') if p.is_file())
  for control in [base/('worker-'+str(n)+'-control'),base/'control']:
   if not control.exists():continue
   r=json.loads((control/'report.json').read_text())
   if any(x.get('unreconciled_children') for x in r.get('outcomes',[])):raise ValueError('unreconciled source controller')
   states[str(control)]=r['status'];files.update(p for p in control.iterdir() if p.is_file() and p.suffix in ('.json','.jsonl','.stdout','.stderr'))
  for data in [base/('worker-'+str(n)+'-data'),base/'data']:
   if not data.exists():continue
   files.update(p for p in data.glob('*.json') if p.is_file())
   for child in data.iterdir():
    if child.is_dir():files.update(p for p in child.glob('*.json') if p.is_file())
 for name in ['fineweb10m-v5-h1-storage-v1','fineweb10m-v5-h2-storage-v1','fineweb10m-v5-storage-v1']:
  files.update(p for p in (root/'results'/name).glob('*.json') if p.is_file())
 if not files or any(p.is_symlink() or p.stat().st_size>64*1024**2 for p in files):raise ValueError('unsafe or oversized metadata source')
 if sum(p.stat().st_size for p in files)>256*1024**2:raise ValueError('metadata cap exceeded')
 members={str(p).lstrip('/'):{'bytes':p.stat().st_size,'sha256':sha(p)} for p in sorted(files)};archive=out/'controls.tar.gz'
 with tarfile.open(archive,'w:gz') as t:
  for p in sorted(files):t.add(p,arcname=str(p).lstrip('/'),recursive=False)
 if any(p.stat().st_size!=members[str(p).lstrip('/')]['bytes'] or sha(p)!=members[str(p).lstrip('/')]['sha256'] for p in files):raise ValueError('completed metadata source changed')
 (out/'members.json').write_text(json.dumps(members,sort_keys=True));store=ArtifactStore('spann-artifacts-079819354890-20261014-ecfcca4d','079819354890',boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d');artifact=store.upload(archive);restored=out/'restored.tar.gz';store.restore(artifact,restored)
 if sha(archive)!=sha(restored):raise ValueError('restored metadata hash mismatch')
 proof={'status':'passed','worker':n,'source_files':len(files),'source_bytes':sum(x['bytes'] for x in members.values()),'artifact':artifact,'members_artifact':store.upload(out/'members.json'),'source_terminal_states':states,'full_restore_verified':True,'scope':'Completed archival controls, reviewed preservation producers, pinned probe/member reports and read-only storage diagnostics only. No checkpoint blocks or raw corpus; failed attempts retained; no deletion.'};(out/'proof.json').write_text(json.dumps(proof,indent=2));print(json.dumps(proof))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--worker',type=int,choices=[0,1],required=True);run(p.parse_args().worker)
