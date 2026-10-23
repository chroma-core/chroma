"""Prepare only one candidate500K run against the retained epoch17 candidate and epoch14 reference."""
import hashlib,importlib.util,json,re
from pathlib import Path
P=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('job',P/'remote-job-epoch15.py');remote=importlib.util.module_from_spec(s);s.loader.exec_module(remote)
WORKER='i-011ff180f03584f40';PEER='i-00268d1e1dfea0357';BASE='/home/ubuntu/spann-two-machine-v1/epoch18'
PREFIX='165f6baa847dacb65c1e9c718c1f6edd976f52ec3907136c447de9ebf5972f48'
def generate(a,now):
 assert a.arm=='500k-distributed' and a.mode in ('serve','run')
 assert re.fullmatch(r'epoch18-[a-z0-9-]+',a.window_id) and a.base==BASE and now<a.job_deadline<=now+590
 v=json.loads((P/'window-epoch17/500k-distributed/run.json').read_text());assert 'profile_gate' not in v
 owner=1 if a.mode=='run' else 2;name=a.arm+f'-owner{owner}'
 v.update(job_id=name,window_id=a.window_id,deadline_unix=a.job_deadline,window_deadline_unix=a.window_deadline,mode=a.mode,instance_id=WORKER if owner==1 else PEER)
 hashfile=P/'runtime-binaries-epoch18.json';hashes=json.loads(hashfile.read_text())
 v.update(binary=BASE+'/build/two_machine_spann',binary_sha256=hashes['two_machine_spann'],reader=BASE+'/build/two_machine_reader',reader_sha256=hashes['two_machine_reader'])
 data=v['args'];host,other=(a.worker_ip,a.peer_ip) if owner==1 else (a.peer_ip,a.worker_ip)
 data.update({'--mode':a.mode,'--owner':owner,'--rows':500000,'--workers':32,'--output':BASE+'/'+name,'--input-descriptor':'/home/ubuntu/spann-two-machine-v1/epoch14/input/500k.json','--owner-budget':500000,'--first-owner-budget':0,'--shutdown-at-unix':a.job_deadline,'--run':int.from_bytes(hashlib.sha256((a.window_id+':'+a.arm).encode()).digest()[:16],'big'),'--data-bind':host+':9100','--control-bind':host+':9101','--peer-data':other+':9100','--peer-control':other+':9101'})
 data['--reader-binary']=v['reader']
 for name in ('worker','peer'):
  assert json.loads((P/'window-epoch14'/('prefix-'+name+'.json')).read_text())['prefix_sha256']==PREFIX
 v['provenance']={'artifact_hash_file_sha256':hashlib.sha256(hashfile.read_bytes()).hexdigest(),'arm':a.arm,'rows':500000,'input_bytes':1536000000,'input_sha256':PREFIX,'input_origin':'retained verified epoch14 prefix on both hosts','reference':'retained epoch14/500k-reference; no rerun','external_profile':False,'replicates':1,'initial_batch':100000,'transition':1000000,'later_batch':1000000}
 remote.validate(v,now);return v
