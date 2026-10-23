"""Archive only manifest-listed public FineWeb benchmark numerical artifacts."""
import sys,pathlib,json,tarfile,time,boto3,hashlib
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.artifact_store import ArtifactStore,file_checksums
root=pathlib.Path('/mnt/spann/fineweb-stream1m-128-v1');out=pathlib.Path('/mnt/spann/results/fineweb1m-durable-v1');prepared=json.loads((root/'input.json').read_text());assert prepared['construction_epoch']=='fineweb1m-128slots-v1' and prepared['global_row_count']==1000000 and prepared['metric']=='ip'
descriptors=[prepared['model']]+[c[key] for c in prepared['source']['chunks'] for key in ('vectors','uuids')]+[s[key] for s in prepared['subtrees'] for key in ('vectors','local_to_global')];files={'input.json':file_checksums(root/'input.json')}
for desc in descriptors:
 rel=pathlib.Path(desc['path']);assert not rel.is_absolute() and '..' not in rel.parts
 p=root/rel;assert not p.is_symlink() and p.is_file();actual=file_checksums(p);assert actual['sha256']==desc['sha256'] and actual['bytes']==desc['bytes'];files[str(rel)]=actual
archive=out/'public-prepared-inputs.tar';started=time.time()
with tarfile.open(archive,'w') as t:
 for rel in sorted(files):t.add(root/rel,arcname=rel,recursive=False)
s=ArtifactStore('spann-artifacts-079819354890-20261014-ecfcca4d','079819354890',boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d');d=s.upload(archive);report=dict(status='archived',payload='Public pinned Qdrant/FineWeb1M benchmark vectors and UUIDs plus deterministic numerical subtree row maps/model; no internal records.',started_unix=started,finished_unix=time.time(),artifact=d,files=files);(out/'prepared-inputs.json').write_text(json.dumps(report,indent=2));print(json.dumps(dict(status='archived',bytes=d['bytes'],elapsed_s=time.time()-started,sha256=d['sha256'])),flush=True)
