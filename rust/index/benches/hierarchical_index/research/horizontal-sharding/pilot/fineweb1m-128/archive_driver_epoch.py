import sys,pathlib,json,tarfile,time,boto3,hashlib
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.artifact_store import ArtifactStore,file_checksums
base=pathlib.Path('/mnt/spann/results/fineweb1m-durable-v1');base.mkdir(exist_ok=True);inputs=[pathlib.Path('/mnt/spann')/name for name in ['hierarchical_spann_shard-audited-v3','hierarchical_spann_shard-memory-v4','subtree-audit-source.tar.gz','reader-memory-v4-source.tar.gz','read-adapter-next-source.tar.gz','SOURCE-EPOCHS.json','restore_epoch_source.py','fineweb1m-pilot-data','fineweb-stream1m-128-v1/input.json','fineweb-stream1m-128-v1/model.json','results/fineweb1m-publication-metadata.tar.gz','results/fineweb1m-128-allocation','results/artifact-restore-v1/report.json','results/artifact-restore-v1/artifacts.json']];files={};started=time.time();archive=base/'driver-provenance.tar'
with tarfile.open(archive,'w') as t:
 for entry in inputs:
  for p in sorted(entry.rglob('*')) if entry.is_dir() else [entry]:
   if p.is_file():
    rel=str(p.relative_to('/mnt/spann'));files[rel]=file_checksums(p);t.add(p,arcname=rel,recursive=False)
store=ArtifactStore('spann-artifacts-079819354890-20261014-ecfcca4d','079819354890',boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d');desc=store.upload(archive);report=dict(status='archived',started_unix=started,finished_unix=time.time(),artifact=desc,files=files);(base/'driver-provenance.json').write_text(json.dumps(report,indent=2));print(json.dumps(dict(status='archived',bytes=desc['bytes'],elapsed_s=time.time()-started,sha256=desc['sha256'])),flush=True)
