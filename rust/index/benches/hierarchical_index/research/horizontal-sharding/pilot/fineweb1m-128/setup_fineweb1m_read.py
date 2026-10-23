import pathlib,json,hashlib,time
from tools.hierarchical_sharding.subtree_orchestrate import serve_case
case=pathlib.Path('/mnt/spann/results/fineweb1m128-h2-a16-c16');binary='/mnt/spann/hierarchical_spann_shard-memory-v4';expected='7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158'
assert hashlib.file_digest(open(binary,'rb'),'sha256').hexdigest()==expected
base=pathlib.Path('/mnt/spann/source/tools/hierarchical_sharding');epoch=dict(name='fineweb1m-memory-v4-four-adapter-v1',binary=binary,binary_sha256=expected,construction_binary_epoch='audited-v3',adapters_per_host=4,logical_cpus_per_host=16,physical_cores_per_host=8,adapter_cpu_masks=['0,1,32,33','2,3,34,35','4,5,36,37','6,7,38,39'],runtime_threads_per_reader=2,profile_reader=True,profile_memory=True,per_query_timeout_s=30,request_limit_bytes=4*1024*1024,owner_response_limit_bytes=64*1024*1024,source_sha256={name:hashlib.file_digest(open(base/name,'rb'),'sha256').hexdigest() for name in ['server.py','subtree_server.py','subtree_transport.py','subtree_orchestrate.py','subtree_benchmark.py']},cache='Fresh application readers before held-out placement comparison; OS page cache remains warm and uncontrolled. No cold-storage claim.')
(case/'read-epoch.json').write_text(json.dumps(epoch,indent=2));started=time.time()
for cohost,port in [(True,9200),(False,9300)]:
 path=serve_case(case,port,cohost=cohost,adapters_per_host=4,read_binary=binary,profile_reader=True,profile_memory=True);print(json.dumps(dict(stage='placement_ready',placement=str(path),elapsed_s=time.time()-started)),flush=True)
