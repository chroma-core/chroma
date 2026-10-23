"""Bind one repaired candidate3M arm against the validated epoch21 reference."""
import hashlib,importlib.util,json,re
from pathlib import Path
P=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('job',P/'remote-job-epoch15.py');remote=importlib.util.module_from_spec(s);s.loader.exec_module(remote)
WORKER='i-011ff180f03584f40';PEER='i-00268d1e1dfea0357';BASE='/home/ubuntu/spann-two-machine-v1/epoch28b'
INPUT='/home/ubuntu/spann-two-machine-v1/epoch2/input/fineweb3m.f32'
DESCRIPTOR='/home/ubuntu/spann-two-machine-v1/epoch2/input/3m.json'
INPUT_SHA='18fb3c71c45bb70d28730f8edfdbd2ae90a3361b150a4871f09d870cb6684aba'
def generate(a,now):
 assert a.arm=='3m-distributed' and a.mode in ('run','serve')
 assert re.fullmatch(r'epoch28b-[a-z0-9-]+',a.window_id) and a.base==BASE and now<a.job_deadline<=now+590
 template=P/'window-epoch27/3m-distributed/run.json';v=json.loads(template.read_text());assert 'profile_gate' not in v
 owner=1 if a.mode=='run' else 2;name=a.arm+f'-owner{owner}'
 v.update(job_id=name,window_id=a.window_id,deadline_unix=a.job_deadline,window_deadline_unix=a.window_deadline,mode=a.mode,instance_id=WORKER if owner==1 else PEER,minimum_free_bytes=4*1024**3,minimum_mem_available_bytes=32*1024**3)
 hashfile=P/'runtime-binaries-epoch28b.json';hashes=json.loads(hashfile.read_text());assert set(hashes)=={'two_machine_spann','two_machine_reader'} and all(re.fullmatch('[0-9a-f]{64}',x) for x in hashes.values())
 v.update(binary=BASE+'/build/two_machine_spann',binary_sha256=hashes['two_machine_spann'],reader=BASE+'/build/two_machine_reader',reader_sha256=hashes['two_machine_reader'])
 data=v['args'];host,other=(a.worker_ip,a.peer_ip) if owner==1 else (a.peer_ip,a.worker_ip)
 data.update({'--mode':a.mode,'--owner':owner,'--rows':3000000,'--workers':32,'--output':BASE+'/'+name,'--input-descriptor':DESCRIPTOR,'--owner-budget':2160000,'--first-owner-budget':0,'--shutdown-at-unix':a.job_deadline,'--run':int.from_bytes(hashlib.sha256((a.window_id+':'+a.arm).encode()).digest()[:16],'big'),'--data-bind':host+':9100','--control-bind':host+':9101','--peer-data':other+':9100','--peer-control':other+':9101','--reader-binary':v['reader']})
 v['provenance']={'artifact_hash_file_sha256':hashlib.sha256(hashfile.read_bytes()).hexdigest(),'inherited_template_sha256':hashlib.sha256(template.read_bytes()).hexdigest(),'baseline_binary_registry_sha256':hashlib.sha256((P/'runtime-binaries-epoch27.json').read_bytes()).hexdigest(),'arm':a.arm,'rows':3000000,'input_bytes':9216000000,'input_sha256':INPUT_SHA,'external_profile':False,'replicates':1,'initial_batch':100000,'transition':1000000,'later_batch':1000000,'owner_capacity':2160000,'question':'One-source one-peer batched transfers; same accepted3M workload','reference':'Validated epoch21 current instrumented3M reference retained; no rerun'}
 remote.validate(v,now);return v
