"""Serve one immutable10M tree under identical one/two-host allocations."""
import pathlib,json,hashlib,time,sys
source='/mnt/spann/owner-reduction-source-v2';sys.path.insert(0,source)
from tools.hierarchical_sharding.subtree_orchestrate import serve_case
case=pathlib.Path('/mnt/spann/results/fineweb10m128-h2-a16-c16-v2');binary='/mnt/spann/hierarchical_spann_shard-memory-v4';expected='7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158';assert hashlib.file_digest(open(binary,'rb'),'sha256').hexdigest()==expected
files=sorted((pathlib.Path(source)/'tools/hierarchical_sharding').glob('*.py'));epoch=dict(name='fineweb10m-owner-reduction-v2',binary=binary,binary_sha256=expected,construction_binary_epoch='audited-v3',source_root=source,source_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in files},adapters_per_host=4,logical_cpus_per_host=16,physical_cores_per_host=8,runtime_threads_per_reader=2,profile_reader=True,profile_memory=True,owner_candidate_reduction=True,query_timeout_s=30,request_limit_bytes=4*1024**2,response_limit_bytes=64*1024**2,cache='Fresh application readers at startup; OS/page cache remains uncontrolled and warm after construction/copy. No cold-storage claim.')
(case/'read-epoch.json').write_text(json.dumps(epoch,indent=2));started=time.monotonic()
for cohost,port in ((True,9600),(False,9700)):
 path=serve_case(case,port,cohost=cohost,adapters_per_host=4,read_binary=binary,profile_reader=True,profile_memory=True,source_root=source,owner_candidate_reduction=True)
 print(json.dumps(dict(status='ready',placement=str(path),elapsed_s=time.monotonic()-started)),flush=True)
