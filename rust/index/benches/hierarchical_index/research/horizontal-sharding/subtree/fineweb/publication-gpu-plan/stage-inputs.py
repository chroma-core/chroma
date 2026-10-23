"""Restore only canonical vectors and small pinned receipts on the new GPU node."""
import json,sys,time,tarfile,hashlib
from pathlib import Path
sys.path.insert(0,'/mnt/spann/source')
import boto3
from botocore.config import Config
from tools.hierarchical_sharding.artifact_store import ArtifactStore
from tools.hierarchical_sharding.tree_manifest import artifact,canonical_bytes,_atomic_write
ROOT=Path('/mnt/spann');BASE=ROOT/'publication-inputs';started=time.monotonic();report=dict(status='running',objects=[])
try:
 cfg=json.loads((BASE/'stage.json').read_text());deadline=json.loads((ROOT/'deadline.json').read_text())
 if deadline['stop_epoch']-deadline['started_epoch']>3600 or deadline['stop_epoch']-time.time()<1800:raise ValueError('stage would leave insufficient fixed pilot budget')
 client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=10,read_timeout=30,retries=dict(max_attempts=3)))
 store=ArtifactStore('spann-artifacts-079819354890-20261014-ecfcca4d','079819354890',client,boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d')
 total=sum(o['artifact']['bytes'] for o in cfg['objects'])+cfg['driver_bundle']['bytes']
 if total>cfg['max_source_bytes']:raise ValueError('staging source cap exceeded')
 for row in cfg['objects']:
  if time.monotonic()-started>cfg['max_wall_s']:raise TimeoutError('source staging time cap exceeded')
  dest=BASE/row['path']
  if Path(row['path']).is_absolute() or '..' in Path(row['path']).parts:raise ValueError('unsafe restore path')
  check=store.restore(row['artifact'],dest);report['objects'].append(dict(path=row['path'],restore=check))
 bundle=BASE/'driver-metadata.tar.gz';store.restore(cfg['driver_bundle'],bundle)
 names=['fineweb10m-reports-v1/attempt-0001/calibration.exact.jsonl','fineweb10m-reports-v1/attempt-0001/test.exact.jsonl','fineweb-pilot-queries/calibration.jsonl','fineweb-pilot-queries/test.jsonl']
 selected={}
 with tarfile.open(bundle,'r:gz') as tar:
  for name in names:
   member=tar.getmember(name)
   if not member.isfile() or member.size>16*1024**2:raise ValueError('unexpected bundle member')
   selected[name]=tar.extractfile(member).read()
 (BASE/'expected10m.jsonl').write_bytes(b''.join(selected[n] for n in names[:2]))
 (BASE/'queries200.jsonl').write_bytes(b''.join(selected[n] for n in names[2:]))
 from tools.hierarchical_sharding.publication_launch import PRODUCERS,KEYS
 paths=dict(prepared1m='input1m.json',prepared10m='input10m.json',queries200='queries200.jsonl',expected1m='expected1m.jsonl',expected10m='expected10m.jsonl',texts11000='texts11000.jsonl',old_texts200='old-texts200.jsonl',model_metadata='model-metadata.json',code_metadata='code-metadata.json',runtime_lock='publication-requirements.lock')
 job=dict(**deadline,inputs={k:artifact(BASE/v,ROOT) for k,v in paths.items()},producer_sources=[artifact(ROOT/'source/tools/hierarchical_sharding'/name,ROOT) for name in sorted(PRODUCERS)],source_chunks=[artifact(BASE/o['path'],ROOT) for o in cfg['objects']])
 # Source order is canonical row order, independent of archive path sorting.
 tree=json.loads((BASE/'input10m.json').read_text());job['source_chunks']=[artifact(BASE/c['vectors']['path'],ROOT) for c in tree['source']['chunks']]
 _atomic_write(ROOT/'publication-job.json',canonical_bytes(job));report.update(status='passed',wall_s=time.monotonic()-started,source_bytes=total,job=artifact(ROOT/'publication-job.json',ROOT))
except BaseException as error:report.update(status='failed',error=repr(error),wall_s=time.monotonic()-started);raise
finally:_atomic_write(ROOT/'stage-report.json',canonical_bytes(report))
