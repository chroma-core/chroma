import pathlib,json,sys,boto3,time,concurrent.futures,shutil
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.artifact_store import ArtifactStore
base=pathlib.Path('/mnt/spann');root=base/'fineweb10m-owner-inputs-v1';host=sys.argv[1];cfg=json.load(open(base/'10m-build-config.json'));catalog=json.load(open(base/'10m-owner-inputs-catalog.json'));assert catalog['status']=='passed' and catalog['input_sha256']=='739c0b520b204c9ade837aa92a501c6ccd8c87d735248a5f395b0df39202cbcb';owners={sid for sid,e in cfg['entries'].items() if e['host']==host};assert owners;files=[]
for row in catalog['files']:
 p=pathlib.PurePosixPath(row['path']);assert p.parts[0]=='fineweb10m-128-v1' and not p.is_absolute() and '..' not in p.parts
 rel=pathlib.Path(*p.parts[1:])
 if len(p.parts)==2 or (len(p.parts)==4 and p.parts[1]=='owners' and p.parts[2] in owners):files.append((row,rel))
assert len(files)==2*len(owners)+3;expected=sum(x['artifact']['bytes'] for x,_ in files);assert expected<64*1024**3;assert shutil.disk_usage(root).free>640*1024**3
store=ArtifactStore('spann-artifacts-079819354890-20261014-ecfcca4d','079819354890',boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d');out=base/'results'/('10m-input-restore-'+host.split('@')[-1]+'-v1');out.mkdir(exist_ok=False);started=time.time();results=[]
def restore(item):
 row,rel=item;assert shutil.disk_usage(root).free>640*1024**3;target=root/rel;assert target.resolve()==target;proof=store.restore(row['artifact'],target);return dict(path=str(rel),artifact=row['artifact'],restore=proof)
try:
 with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
  for proof in pool.map(restore,files):
   results.append(proof)
   with (out/'completed.jsonl').open('a') as f:f.write(json.dumps(proof)+'\n')
 report=dict(status='passed',host=host,owners=len(owners),files=len(results),bytes=expected,elapsed_s=time.time()-started,source='Pinned public FineWeb content-addressed artifact objects; owner inputs only, no source corpus.',started_unix=started,finished_unix=time.time());(out/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)
except BaseException as e:
 (out/'failure.json').write_text(json.dumps(dict(status='failed',error=str(e),completed_files=len(results),elapsed_s=time.time()-started)));raise
