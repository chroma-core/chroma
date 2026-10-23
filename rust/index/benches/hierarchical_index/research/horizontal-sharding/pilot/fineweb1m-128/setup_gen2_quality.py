"""Serve one immutable10M tree under identical one/two-host allocations."""
import pathlib,json,hashlib,time,sys
source='/mnt/spann/owner-reduction-source-v2';sys.path.insert(0,source)
from tools.hierarchical_sharding import subtree_orchestrate as orchestration
original_ssh=orchestration.ssh
def isolated_ssh(host,code):
 return original_ssh(host,code.replace('/mnt/spann/source',source))
orchestration.ssh=isolated_ssh
serve_case=orchestration.serve_case
original=pathlib.Path('/mnt/spann/results/fineweb-compaction-p3-h2-v1');case=pathlib.Path('/mnt/spann/results/fineweb1m1-gen2-quality-v3');case.mkdir(exist_ok=False);metadata=json.loads((original/'case.json').read_text());metadata['host_order']=sorted({x['host'] for x in metadata['entries'].values()});assert metadata['tree_sha256']=='04d57d52834fdd41b2065202775dcdd6769ed85e7721d8d8907174771951ea27';(case/'case.json').write_text(json.dumps(metadata));binary='/mnt/spann/hierarchical_spann_shard-memory-v4';expected='7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158';assert hashlib.file_digest(open(binary,'rb'),'sha256').hexdigest()==expected
files=sorted((pathlib.Path(source)/'tools/hierarchical_sharding').glob('*.py'));epoch=dict(name='fineweb1m1-selected-gen2-quality-v1',binary=binary,binary_sha256=expected,construction_binary_epoch='audited-v3',source_root=source,source_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in files},adapters_per_host=4,logical_cpus_per_host=16,physical_cores_per_host=8,runtime_threads_per_reader=2,profile_reader=True,profile_memory=True,owner_candidate_reduction=True,query_timeout_s=30,request_limit_bytes=4*1024**2,response_limit_bytes=64*1024**2,cache='Fresh application readers at startup; OS/page cache remains uncontrolled and warm after construction/copy. No cold-storage claim.')
(case/'read-epoch.json').write_text(json.dumps(epoch,indent=2));started=time.monotonic()
for cohost,port in ((True,9800),(False,9900)):
 path=serve_case(case,port,cohost=cohost,adapters_per_host=4,read_binary=binary,profile_reader=True,profile_memory=True,source_root=source,owner_candidate_reduction=True)
 print(json.dumps(dict(status='ready',placement=str(path),elapsed_s=time.monotonic()-started)),flush=True)
