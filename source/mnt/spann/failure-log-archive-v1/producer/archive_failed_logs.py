"""Preserve immutable failed-run logs with a complete member inventory and full restore."""
import argparse,hashlib,json,pathlib,signal,sys,tarfile,time
import boto3
import urllib.request
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.artifact_store import ArtifactStore

def digest(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for block in iter(lambda:f.read(8*1024**2),b''):h.update(block)
 return h.hexdigest()

def inventory(root):
 files={}
 for p in sorted(root.rglob('*')):
  if p.is_symlink():raise ValueError('symlink in immutable log source')
  if p.is_file():files[str(p.relative_to(root))]={'bytes':p.stat().st_size,'sha256':digest(p)}
 return files

def run(config):
 if not __debug__:raise ValueError('optimized Python is unsupported')
 source=pathlib.Path(config['source']);out=pathlib.Path(config['output'])
 for p in (source,out):
  if not p.is_absolute() or '..' in p.parts or p.resolve()!=p or not str(p).startswith('/mnt/spann/'):raise ValueError('invalid owned path')
 if source==out or source in out.parents or out in source.parents:raise ValueError('overlapping paths')
 out.mkdir(exist_ok=False);phase='identity';status='running';error=None;started=time.time()
 def record():
  (out/'report.json').write_text(json.dumps(dict(status=status,phase=phase,error=error,started_unix=started,updated_unix=time.time()),indent=2))
 def terminate(signum,frame):raise SystemExit('terminated by signal '+str(signum))
 previous_term=signal.getsignal(signal.SIGTERM);signal.signal(signal.SIGTERM,terminate);record()
 try:
  token=urllib.request;opener=token.build_opener(token.ProxyHandler({}));req=token.Request('http://169.254.169.254/latest/api/token',method='PUT',headers={'X-aws-ec2-metadata-token-ttl-seconds':'60'});t=opener.open(req,timeout=3).read().decode();req=token.Request('http://169.254.169.254/latest/dynamic/instance-identity/document',headers={'X-aws-ec2-metadata-token':t});identity=json.load(opener.open(req,timeout=3))
  if identity['accountId']!='079819354890' or identity['instanceId']!=config['expected_instance_id']:raise ValueError('wrong instance/account')
  (out/'identity.json').write_text(json.dumps(identity,indent=2));phase='inventory';record();members=inventory(source)
  if not members or sum(x['bytes'] for x in members.values())>config['source_cap_bytes']:raise ValueError('empty or oversized logs')
  phase='export';record();archive=out/'failed-logs.tar.gz'
  with tarfile.open(archive,'w:gz') as tar:
   for rel in members:tar.add(source/rel,arcname=rel,recursive=False)
  if inventory(source)!=members:raise ValueError('log source changed during export')
  (out/'members.json').write_text(json.dumps(members,sort_keys=True));phase='upload';record();store=ArtifactStore(config['bucket'],'079819354890',boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d');artifact=store.upload(archive);(out/'artifact.json').write_text(json.dumps(artifact,indent=2));phase='full-restore';record();restored=out/'restored.tar.gz';store.restore(artifact,restored)
  if digest(archive)!=digest(restored):raise ValueError('restored archive hash mismatch')
  phase='verify-every-member';record();seen=set()
  with tarfile.open(restored) as tar:
   for m in tar:
    if not m.isfile() or m.name not in members or m.name in seen:raise ValueError('unexpected archive member')
    h=hashlib.sha256();n=0
    with tar.extractfile(m) as f:
     for block in iter(lambda:f.read(8*1024**2),b''):h.update(block);n+=len(block)
    if {'bytes':n,'sha256':h.hexdigest()}!=members[m.name]:raise ValueError('restored member mismatch')
    seen.add(m.name)
  if seen!=set(members):raise ValueError('missing archived member')
  proof={'status':'passed','instance_id':identity['instanceId'],'source':str(source),'files':len(members),'source_bytes':sum(x['bytes'] for x in members.values()),'artifact':artifact,'members_artifact':store.upload(out/'members.json'),'elapsed_s':time.time()-started,'scope':'Full raw failed-run control/log tree; byte preservation only; no index data and no deletion.'};(out/'proof.json').write_text(json.dumps(proof,indent=2));store.upload(out/'proof.json');status='passed';phase='complete'
 except BaseException as failure:error=repr(failure);status='failed';raise
 finally:
  try:record()
  finally:signal.signal(signal.SIGTERM,previous_term)
 return proof
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--config',required=True);a=p.parse_args();print(json.dumps(run(json.loads(pathlib.Path(a.config).read_text()))))
