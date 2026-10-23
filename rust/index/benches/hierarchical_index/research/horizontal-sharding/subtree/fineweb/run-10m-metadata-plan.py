"""Read bounded source metadata before the required real 10M preparation gate."""
import json,sys
from pathlib import Path
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.fineweb_source_plan import plan_prefix
from tools.hierarchical_sharding.tree_manifest import canonical_bytes,_atomic_write
root=Path('/mnt/spann');output=root/'fineweb-10m-source-plan-v1.json'
if output.exists():raise FileExistsError(output)
manifest=json.loads((root/'fineweb-source-manifest-v2.json').read_text())
result=plan_prefix(manifest,10000000,max_source_bytes=16*1024**2,max_wall_s=120,max_files=32,max_column_read_bytes=256*1024**2)
result['caps']=dict(metadata_source_bytes=16*1024**2,wall_s=120,files=32,column_read_bytes=256*1024**2)
result['proposed_ingest']=dict(concurrency=2,seed_previous=None,chunk_rows=100000,decoder_epoch='arrow_primitive_f16_v2',count=10000000)
result['engineering_gates']=['Check complete-file seed receipt and actual hashes before reuse.','Derive shared download cap from projected column bytes plus metadata and bounded retry allowance.','Check measured current free storage against output, retry, owner routing and checkpoint headroom.','Freeze a new 10M whole-prefix routing model; do not refit the 1M append epoch.','Recompute exact references over all 10M rows; keep the 200-query integration pool separate from the text-only 11k pool.']
_atomic_write(output,canonical_bytes(result));print(json.dumps(result),flush=True)
