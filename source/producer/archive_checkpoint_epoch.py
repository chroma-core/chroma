"""Archive completed owned stores, restore every byte, and reopen/query every owner."""
from pathlib import Path
import argparse,hashlib,json,os,re,signal,subprocess,sys,tarfile,time,urllib.request
import boto3
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.artifact_store import ArtifactStore

def sha(path):
 with Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()

def identity():
 req=urllib.request.Request('http://169.254.169.254/latest/api/token',method='PUT',headers={'X-aws-ec2-metadata-token-ttl-seconds':'60'})
 token=urllib.request.urlopen(req,timeout=3).read().decode();req=urllib.request.Request('http://169.254.169.254/latest/dynamic/instance-identity/document',headers={'X-aws-ec2-metadata-token':token})
 return json.load(urllib.request.urlopen(req,timeout=3))

def pinned_probe(binary,directory,slot,query):
 manifest=directory/('manifest-'+str(slot['generation'])+'.json');assert sha(manifest)==slot['snapshot_sha256']
 m=json.loads(manifest.read_text());mapping=Path(m['id_mapping']['path']);mapping=mapping if mapping.is_absolute() else directory/mapping
 # Absolute map paths cannot produce a portable restore and fail before archiving.
 assert not Path(m['id_mapping']['path']).is_absolute()
 assert sha(mapping)==slot['map_sha256'] and mapping.stat().st_size==slot['vector_count']*8
 a=m['integrity_audit'];assert a['generation']==slot['generation'] and a['block_ids']==m['block_ids'] and a['id_map_sha256']==slot['map_sha256']
 assert a['coverage']['expected_rows']==a['coverage']['unique_live_rows']==slot['vector_count']
 requests=[dict(op='describe_root',generation=slot['generation']),dict(op='query',generation=slot['generation'],query=query,k=1,rerank_factor=2)]
 r=subprocess.run([binary,'read','--checkpoint-dir',str(directory),'--generation',str(slot['generation'])],input=''.join(json.dumps(x)+'\n' for x in requests),capture_output=True,text=True,timeout=60,check=True,env=dict(os.environ,TOKIO_WORKER_THREADS='2'))
 rows=[json.loads(l) for l in r.stdout.splitlines() if l.strip()];assert len(rows)==2 and all(not x.get('error') for x in rows)
 d,q=rows;assert d['generation']==slot['generation'] and d['shard_id']==slot['subtree_id'] and d['vector_count']==slot['vector_count']
 assert d['root']['node_id']==m['root_ref']['id'] and d['root']['is_leaf']==(m['root_ref']['kind']=='leaf')
 assert len(q['ids'])==len(q['distances'])==1
 return dict(generation=d['generation'],shard_id=d['shard_id'],vector_count=d['vector_count'],root=d['root'],ids=q['ids'],distances=q['distances'])

def extract_verified(archive,destination,members):
 destination.mkdir(exist_ok=False);seen=set()
 with tarfile.open(archive) as t:
  for m in t:
   rel=Path(m.name)
   if not m.isfile() or rel.is_absolute() or '..' in rel.parts or m.name not in members or m.name in seen:raise ValueError('unsafe or unexpected archived member')
   desc=members[m.name];assert m.size==desc['bytes'];p=destination/rel;p.parent.mkdir(parents=True,exist_ok=True);h=hashlib.sha256();n=0
   with t.extractfile(m) as source,p.open('xb') as out:
    while True:
     block=source.read(8*1024**2)
     if not block:break
     out.write(block);h.update(block);n+=len(block)
   assert n==desc['bytes'] and h.hexdigest()==desc['sha256'];seen.add(m.name)
 assert seen==set(members)

def inventory(source):
 out={}
 for p in sorted(source.rglob('*')):
  if p.is_symlink():raise ValueError('source symlink')
  if p.is_file():out[str(p.relative_to(source))]=dict(bytes=p.stat().st_size,sha256=sha(p))
 return out

def validate(config):
 if not __debug__:raise ValueError('optimized Python disables required checks')
 root=Path(config['output']);sources=[];names=set()
 if not root.is_absolute() or '..' in root.parts or root.resolve()!=root or not str(root).startswith('/mnt/spann/'):raise ValueError('invalid owned output')
 for case in config['stores']:
  name=case['name'];source=Path(case['source'])
  if not re.fullmatch('[a-zA-Z0-9-]+',name) or name in names:raise ValueError('invalid/duplicate store name')
  names.add(name)
  if not source.is_absolute() or '..' in source.parts or source.resolve()!=source or not str(source).startswith('/mnt/spann/'):raise ValueError('invalid source')
  if source==root or source in root.parents or root in source.parents:raise ValueError('source/output overlap')
  if source in sources:raise ValueError('duplicate source')
  sources.append(source);slots=case['slots']
  if not slots or len({x['subtree_id'] for x in slots})!=len(slots):raise ValueError('missing/duplicate slot')
  for slot in slots:
   if not re.fullmatch('[a-zA-Z0-9-]+',slot['subtree_id']) or type(slot['generation']) is not int or slot['generation']<1 or type(slot['vector_count']) is not int or slot['vector_count']<1:raise ValueError('invalid slot')
   for key in ('snapshot_sha256','map_sha256'):
    if not re.fullmatch('[0-9a-f]{64}',slot[key]):raise ValueError('invalid slot hash')
  if not re.fullmatch('[0-9a-f]{64}',case['tree_sha256']):raise ValueError('invalid tree hash')

def archive_one(case,config,root,store):
 started=time.monotonic();source=Path(case['source']);out=root/case['name'];out.mkdir();slots=case['slots'];phase='source-probe'
 def record(status,**fields):(out/'progress.json').write_text(json.dumps(dict(status=status,phase=phase,updated_unix=time.time(),**fields),indent=2))
 try:
  record('running');before={x['subtree_id']:pinned_probe(config['binary'],source/x['subtree_id'],x,config['query']) for x in slots};members={};archive=out/'checkpoints.tar';phase='export';record('running')
  with tarfile.open(archive,'w',dereference=True) as t:
   for p in sorted(source.rglob('*')):
    if p.is_symlink():raise ValueError('source symlink')
    if not p.is_file():continue
    rel=str(p.relative_to(source));st=p.stat();members[rel]=dict(bytes=st.st_size,sha256=sha(p));t.add(p,arcname=rel,recursive=False);after=p.stat();assert (st.st_ino,st.st_size,st.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns)
  phase='verify-source-after-export';record('running');assert inventory(source)==members
  assert {x['subtree_id']:pinned_probe(config['binary'],source/x['subtree_id'],x,config['query']) for x in slots}==before
  (out/'members.json').write_text(json.dumps(members,sort_keys=True));phase='upload';record('running');descriptor=store.upload(archive);(out/'artifact.json').write_text(json.dumps(descriptor,indent=2));phase='full-restore';record('running');store.restore(descriptor,out/'restored.tar');phase='extract-every-member';record('running');extract_verified(out/'restored.tar',out/'restored',members);phase='restored-probes';record('running');after={x['subtree_id']:pinned_probe(config['binary'],out/'restored'/x['subtree_id'],x,config['query']) for x in slots};assert before==after
  proof=dict(status='passed',case=case['name'],tree_sha256=case['tree_sha256'],owners=len(slots),rows=sum(x['vector_count'] for x in slots),files=len(members),logical_bytes=sum(x['bytes'] for x in members.values()),artifact=descriptor,source_and_restored_probes=after,elapsed_s=time.monotonic()-started,members_artifact=store.upload(out/'members.json'))
  (out/'report.json').write_text(json.dumps(proof,indent=2));proof['report_artifact']=store.upload(out/'report.json');phase='complete';record('passed');return proof
 except BaseException as error:
  record('failed',error=repr(error));raise

def run(config):
 validate(config);actual=identity();assert actual['accountId']=='079819354890' and actual['instanceId']==config['expected_instance_id'];assert sha(config['binary'])==config['binary_sha256'];root=Path(config['output']);root.mkdir(exist_ok=False);(root/'identity.json').write_text(json.dumps(actual));reports=[];status='running';error=None;previous_term=signal.getsignal(signal.SIGTERM)
 def terminate(signum,frame):raise SystemExit('archive terminated by signal '+str(signum))
 signal.signal(signal.SIGTERM,terminate)
 (root/'report.json').write_text(json.dumps(dict(status='running',identity=actual,stores=[],started_unix=time.time())))
 try:
  store=ArtifactStore(config['bucket'],'079819354890',boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d')
  for case in config['stores']:
   proof=archive_one(case,config,root,store);reports.append(proof);print(json.dumps({k:proof[k] for k in ('status','case','owners','rows','files','logical_bytes','elapsed_s')}),flush=True)
  status='passed'
 except BaseException as failure:
  status='failed';error=repr(failure);raise
 finally:
  result=dict(status=status,error=error,identity=actual,stores=reports,finished_unix=time.time(),scope='Every owned file archived and fully restored; every member SHA checked; every pinned lower snapshot audit/map reopened and deterministic k1 query matches before/after. This is preservation parity, not an independent exact-neighbor semantic test. No files deleted.');(root/'report.json').write_text(json.dumps(result,indent=2));signal.signal(signal.SIGTERM,previous_term)
 return result
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--config',required=True);a=p.parse_args();run(json.loads(Path(a.config).read_text()))
