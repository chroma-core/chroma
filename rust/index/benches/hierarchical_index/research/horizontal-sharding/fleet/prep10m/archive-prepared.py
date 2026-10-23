"""Archive completed preparation through the restricted instance role only."""
import concurrent.futures,json,os,sys,time,hashlib,shutil,threading
from pathlib import Path
sys.path.insert(0,'/mnt/spann/source')
import boto3
from tools.hierarchical_sharding.artifact_store import ArtifactStore,file_checksums
from tools.hierarchical_sharding.tree_manifest import canonical_bytes,_atomic_write,checked_artifact
ROOT=Path('/mnt/spann');EBS=Path('/var/lib/spann-prep-evidence');OUT=EBS/'archive-v1';OUT.mkdir(exist_ok=False)
started=time.monotonic();deadline=started+1800;report=dict(status='running',files=[],started_epoch=time.time(),caps=dict(wall_s=1800,concurrency=4,local_restore_files_at_once=4))
store=ArtifactStore('spann-artifacts-079819354890-20261014-ecfcca4d','079819354890',boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d')
def save():_atomic_write(OUT/'manifest.json',canonical_bytes(report))
latest=json.loads((ROOT/'fineweb10m-reports-v1/latest.json').read_text());run=json.loads(checked_artifact(latest['report'],ROOT/'fineweb10m-reports-v1').read_text())
if run['status']!='passed':raise ValueError('full preparation/reference report must pass before archiving as complete')
prepared=json.loads(checked_artifact(run['input'],ROOT).read_text());prepared_base=ROOT/'fineweb10m-128-v1'
checked_artifact(prepared['model'],prepared_base)
for owner in prepared['subtrees']:
 checked_artifact(owner['vectors'],prepared_base);checked_artifact(owner['local_to_global'],prepared_base)
for chunk in prepared['source']['chunks']:
 checked_artifact(chunk['vectors'],prepared_base);checked_artifact(chunk['uuids'],prepared_base)
checked_artifact(prepared['uuid_index'],prepared_base)
# Failed route copies are aliases only after actual bytes match authoritative files.
expected_files={}
for owner in prepared['subtrees']:
 for key in ('vectors','local_to_global'):
  desc=owner[key];expected_files[str(Path(desc['path']))]=desc
aliases=[]
for old in prepared_base.glob('owners-incomplete-*'):
 for path in old.rglob('*'):
  if not path.is_file():continue
  current=Path('owners')/path.relative_to(old);expected=expected_files[str(current)];checks=file_checksums(path)
  if checks['bytes']!=expected['bytes'] or checks['sha256']!=expected['sha256']:raise ValueError('failed route differs from authoritative output; preserve separately before stopping')
  aliases.append(dict(retained_path=str(path),authoritative_path=str(current),bytes=checks['bytes'],sha256=checks['sha256']))
_atomic_write(EBS/'provenance/failed-route-aliases.json',canonical_bytes(dict(status='passed',scope='failed route copies retained locally, byte-identical to authoritative files; excluded from authoritative archive/staging',aliases=aliases)))
paths=[]
for scope in ['fineweb-columnar10m-v2','fineweb10m-128-v1','fineweb10m-reports-v1','fineweb-pilot-queries','preflight']:
 paths.extend(p for p in (ROOT/scope).rglob('*') if p.is_file() and not any(part.startswith('owners-incomplete-') for part in p.relative_to(ROOT).parts))
if (EBS/'source-sample-check.json').exists():paths.append(EBS/'source-sample-check.json')
paths.extend([ROOT/'fineweb-10m-source-plan-v1.json',ROOT/'fineweb-source-manifest-v2.json',ROOT/'ten-million-job-v2-fresh.json',ROOT/'guard-config-v1.json',ROOT/'guard-config-v2.json'])
for scope in ['provenance','guard-v1','guard-v2','source-check-guard-v1','failed-attempt0']:
 paths.extend(p for p in (EBS/scope).rglob('*') if p.is_file())
report['caps']['maximum_concurrent_restore_bytes']=4*max(p.stat().st_size for p in paths)
restore=ROOT/'archive-owned-restores-v1'/'attempt-0000';restore.mkdir(parents=True,exist_ok=False)
unique={};unique_lock=threading.Lock()
def one(item):
 number,path=item
 if time.monotonic()>deadline or shutil.disk_usage(ROOT).free<512*1024**3:raise ValueError('archive time/free-space gate exceeded')
 if path.is_symlink():raise ValueError('archive does not follow symlinks')
 checks=file_checksums(path);identity=(checks['bytes'],checks['sha256'])
 with unique_lock:
  owner=identity not in unique
  if owner:unique[identity]=concurrent.futures.Future()
  future=unique[identity]
 if owner:
  try:
   descriptor=store.upload(path);destination=restore/str(number);checked=store.restore(descriptor,destination)
   destination.unlink()  # Only the explicitly owned, fully checked disposable restore.
   future.set_result((descriptor,checked))
  except BaseException as error:future.set_exception(error);raise
 else:
  descriptor,original_check=future.result();checked=dict(reuses_verified_object=True,sha256=checks['sha256'],version_id=descriptor['version_id'])
 try:relative=str(path.relative_to(ROOT))
 except ValueError:relative='root-ebs/'+str(path.relative_to(EBS))
 return dict(path=relative,artifact=descriptor,restore=checked)
save()
try:
 with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
  futures=[pool.submit(one,pair) for pair in enumerate(sorted(paths))]
  for future in concurrent.futures.as_completed(futures):
   report['files'].append(future.result());save()
 report['files'].sort(key=lambda row:row['path']);report.update(status='passed',wall_s=time.monotonic()-started,archive_input_bytes=sum(f['artifact']['bytes'] for f in report['files']),file_count=len(report['files']),unique_object_count=len(unique),unique_object_bytes=sum(size for size,sha in unique));save()
 # Store a version-pinned copy of the complete descriptor after all objects pass.
 descriptor=store.upload(OUT/'manifest.json');_atomic_write(OUT/'manifest-object.json',canonical_bytes(descriptor))
 restored=restore/'manifest';store.restore(descriptor,restored);assert restored.read_bytes()==(OUT/'manifest.json').read_bytes();restored.unlink()
 print(json.dumps(dict(status='passed',manifest=descriptor,file_count=len(report['files']))),flush=True)
except Exception as error:
 report.update(status='failed',wall_s=time.monotonic()-started,error_type=type(error).__name__,error=str(error));save();raise
