"""Preserve GPU results and failures, excluding reconstructible corpus/runtime caches."""
import sys,json,time,tarfile,os,stat,shutil
from pathlib import Path
sys.path.insert(0,'/mnt/spann/source')
import boto3
from tools.hierarchical_sharding.artifact_store import ArtifactStore,file_checksums
from tools.hierarchical_sharding.tree_manifest import artifact,canonical_bytes,_atomic_write
R=Path('/mnt/spann');E=Path('/var/lib/spann-publication-evidence');O=R/'publication-archive-v1';O.mkdir(exist_ok=False);started=time.monotonic();report=dict(status='running')
def inventory(root):
 logical=allocated=links=count=0;seen=set()
 for directory,dirs,files in os.walk(root,followlinks=False):
  for name in dirs+files:
   p=Path(directory)/name;s=p.lstat();count+=1
   if stat.S_ISLNK(s.st_mode):links+=s.st_size
   elif stat.S_ISREG(s.st_mode):
    logical+=s.st_size;key=(s.st_dev,s.st_ino)
    if key not in seen:seen.add(key);allocated+=s.st_blocks*512
 return dict(regular_logical_path_bytes=logical,unique_regular_allocated_bytes=allocated,symlink_own_bytes=links,entries=count,free_bytes=shutil.disk_usage(root).free,symlink_policy='lstat link bytes only, never follow target')
try:
 inv={name:inventory(path) for name,path in [('all_owned_nvme',R),('installed_numeric_runtime',R/'publication-venv'),('installed_artifact_runtime',R/'artifact-venv'),('package_cache',R/'package-cache'),('model_cache',R/'publication-results/model-cache'),('root_ebs_proofs',E)]}
 if inv['root_ebs_proofs']['regular_logical_path_bytes']>4*1024**3 or inv['root_ebs_proofs']['free_bytes']<16*1024**3:raise ValueError('root proof size/free-space gate exceeded')
 _atomic_write(E/'final-separated-inventory.json',canonical_bytes(inv))
 files=[]
 for scope in [R/'source',R/'publication-bootstrap',R/'publication-results',R/'calibration-fallback-v1',E]:
  for p in scope.rglob('*'):
   if not p.is_file() or p.is_symlink():continue
   if scope==R/'publication-results' and 'model-cache' in p.relative_to(scope).parts:continue
   if scope==E and any(part.startswith('archive-') for part in p.relative_to(scope).parts):continue
   if '__pycache__' in p.parts:continue
   files.append(p)
 for p in (R/'publication-inputs').iterdir():
  if p.is_file():files.append(p)
 for name in ['deadline.json','stage-report.json','publication-job.json','guard-config.json','guard-config-v2.json','guard-config-v3.json','run-pipeline.sh','run-pipeline-v2.sh','run-publication.sh','stage-inputs.py','stage-inputs-v2.py','run-calibration-fallback.py']:
  if (R/name).exists():files.append(R/name)
 entries=[]
 with tarfile.open(O/'results.tar.gz','w:gz',compresslevel=1,dereference=True) as tar:
  for p in sorted(set(files)):
   name=('root-ebs/'+str(p.relative_to(E))) if p.is_relative_to(E) else str(p.relative_to(R))
   checks=file_checksums(p);tar.add(p,arcname=name,recursive=False);entries.append(dict(path=name,**checks))
 _atomic_write(O/'files.json',canonical_bytes(entries))
 store=ArtifactStore('spann-artifacts-079819354890-20261014-ecfcca4d','079819354890',boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d')
 descriptor=store.upload(O/'results.tar.gz');restored=store.restore(descriptor,O/'restored.tar.gz')
 # Independently check every regular archive member against the pre-tar bytes.
 import hashlib
 actual={row['path']:row for row in entries}
 with tarfile.open(O/'restored.tar.gz','r:gz') as tar:
  members=tar.getmembers()
  if len(members)!=len(entries):raise ValueError('archive member coverage differs')
  for member in members:
   row=actual[member.name]
   if not member.isfile() or member.size!=row['bytes']:raise ValueError('archive member kind/size differs')
   h=hashlib.sha256()
   with tar.extractfile(member) as body:
    for block in iter(lambda:body.read(8*1024*1024),b''):h.update(block)
   if h.hexdigest()!=row['sha256']:raise ValueError('restored member SHA differs')
 report['inventory_boundary']='after compute, before archive output copies'
 report.update(status='passed',wall_s=time.monotonic()-started,artifact=descriptor,restore=restored,file_count=len(entries),input_logical_bytes=sum(row['bytes'] for row in entries),inventory=inv)
except BaseException as error:report.update(status='failed',error=repr(error),wall_s=time.monotonic()-started);raise
finally:_atomic_write(O/'report.json',canonical_bytes(report))
print(json.dumps({k:v for k,v in report.items() if k!='inventory'}))
