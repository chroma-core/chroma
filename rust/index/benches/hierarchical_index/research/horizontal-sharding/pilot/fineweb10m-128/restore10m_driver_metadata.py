import pathlib,json,sys,boto3,tarfile,hashlib,time
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.artifact_store import ArtifactStore
base=pathlib.Path('/mnt/spann');out=base/'results/fineweb10m-staging-v1';out.mkdir(exist_ok=True);sha='88d08ace27c102a769b21874b1c83cdc6198662535d9b4de5011278a14f4d088';d=dict(bucket='spann-artifacts-079819354890-20261014-ecfcca4d',expected_bucket_owner='079819354890',key='sha256/88/'+sha,sha256=sha,bytes=25917011,version_id='4agPVcriR.xa3CSEJJW0MBGCkIa4zmm1');started=time.time();store=ArtifactStore(d['bucket'],d['expected_bucket_owner'],boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d');archive=out/'driver-metadata.tar.gz';proof=store.restore(d,archive);total=0;files=[]
with tarfile.open(archive,'r:*') as tar:
 for m in tar.getmembers():
  rel=pathlib.PurePosixPath(m.name);assert not rel.is_absolute() and '..' not in rel.parts
  if m.isdir():continue
  assert m.isfile() and not m.issym() and not m.islnk();assert m.size<128*1024**2;total+=m.size;assert total<256*1024**2
  target=base/pathlib.Path(m.name);assert target.resolve()==target;data=tar.extractfile(m).read();digest=hashlib.sha256(data).hexdigest()
  if target.exists():assert target.is_file() and hashlib.sha256(target.read_bytes()).hexdigest()==digest
  else:target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
  files.append(dict(path=m.name,sha256=digest,bytes=m.size))
input=base/'fineweb10m-128-v1/input.json';assert hashlib.sha256(input.read_bytes()).hexdigest()=='739c0b520b204c9ade837aa92a501c6ccd8c87d735248a5f395b0df39202cbcb';report=dict(status='passed',restore=proof,artifact=d,files=files,raw_bytes=total,elapsed_s=time.time()-started);(out/'driver-metadata-restore.json').write_text(json.dumps(report,indent=2));p=json.load(open(input));print(json.dumps(dict(status='passed',rows=p['global_row_count'],slots=len(p['subtrees']),raw_bytes=total,elapsed_s=report['elapsed_s'])))
