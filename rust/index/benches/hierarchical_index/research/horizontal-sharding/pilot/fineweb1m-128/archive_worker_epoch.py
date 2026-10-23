import pathlib,tarfile,json,time,sys,boto3,hashlib
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.artifact_store import ArtifactStore
host=sys.argv[1];base=pathlib.Path('/mnt/spann/results/fineweb1m-durable-v1');base.mkdir(exist_ok=True);case=pathlib.Path('/mnt/spann/fineweb1m128-h2-a16-c16');archive=base/('snapshots-'+host+'.tar');started=time.time();files=[]
with tarfile.open(archive,'w') as t:
 for p in sorted((case/'indexes').rglob('*')):
  if p.is_file():
   relative=str(p.relative_to(case));t.add(p,arcname=relative,recursive=False);files.append(dict(path=relative,bytes=p.stat().st_size))
store=ArtifactStore('spann-artifacts-079819354890-20261014-ecfcca4d','079819354890',boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d');descriptor=store.upload(archive);result=dict(status='archived',host=host,started_unix=started,finished_unix=time.time(),files=files,artifact=descriptor);(base/('snapshots-'+host+'.json')).write_text(json.dumps(result,indent=2));print(json.dumps(dict(status='archived',host=host,bytes=descriptor['bytes'],sha256=descriptor['sha256'],elapsed_s=time.time()-started)),flush=True)
