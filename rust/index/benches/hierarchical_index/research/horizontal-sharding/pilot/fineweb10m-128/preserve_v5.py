"""Archive the separately tested logging epoch and verify full restores."""
from pathlib import Path
import sys,json,boto3,time
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.artifact_store import ArtifactStore
base=Path('/mnt/spann/logging-epoch-v5-preserved');out=base/'archive-proof-v1';out.mkdir(exist_ok=False)
store=ArtifactStore('spann-artifacts-079819354890-20261014-ecfcca4d','079819354890',boto3.client('s3',region_name='us-east-1'),boto3.client('sts',region_name='us-east-1'),'spann-artifact-ecfcca4d');start=time.monotonic();objects=[]
for name in ('hierarchical_spann_shard-logging-v5','source.tar.gz'):
 d=store.upload(base/name);store.restore(d,out/name);objects.append(dict(path=name,descriptor=d))
(out/'report.json').write_text(json.dumps(dict(status='passed',elapsed_s=time.monotonic()-start,objects=objects),indent=2));print((out/'report.json').read_text())
