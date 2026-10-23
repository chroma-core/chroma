"""Prepare only the user-authorized retained-binary500K comparison, without a profiler gate."""
import copy,hashlib,importlib.util,json,re
from pathlib import Path
P=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('job',P/'remote-job-epoch13.py');remote=importlib.util.module_from_spec(s);s.loader.exec_module(remote)
WORKER='i-011ff180f03584f40';PEER='i-00268d1e1dfea0357';BASE='/home/ubuntu/spann-two-machine-v1/epoch14'
def generate(a,now):
 assert a.arm in ('500k-reference','500k-distributed') and a.mode in ('serve','run')
 assert a.arm!='500k-reference' or a.mode=='run'
 assert re.fullmatch(r'epoch14-[a-z0-9-]+',a.window_id) and a.base==BASE and now<a.job_deadline<=now+590
 old=P/'window-epoch13/100k-distributed/run.json'
 if a.arm=='500k-reference':old=P/'window-epoch12/100k-reference-ackfix/run.json'
 if not old.exists() and a.arm=='500k-reference':old=P/'window-epoch12/100k-reference/run.json'
 v=json.loads(old.read_text());v.pop('profile_gate',None)
 owner=1 if a.mode=='run' else 2;name=a.arm+f'-owner{owner}'
 v.update(job_id=name,window_id=a.window_id,deadline_unix=a.job_deadline,window_deadline_unix=a.window_deadline,mode=a.mode,instance_id=WORKER if owner==1 else PEER,minimum_free_bytes=4*1024**3,minimum_mem_available_bytes=32*1024**3)
 hashfile=P/'runtime-binaries-epoch13.json'; hashes=json.loads(hashfile.read_text()); v.update(binary=('/home/ubuntu/spann-two-machine-v1/epoch12b/build/reference.bin' if a.arm=='500k-reference' else '/home/ubuntu/spann-two-machine-v1/epoch13/build/two_machine_spann'),binary_sha256=hashes['reference' if a.arm=='500k-reference' else 'two_machine_spann'],reader='/home/ubuntu/spann-two-machine-v1/epoch13/build/two_machine_reader',reader_sha256=hashes['two_machine_reader'])
 data=v['args'];data['--rows']=500000
 if '--reader-binary' in data:data['--reader-binary']=v['reader']
 prefix=json.loads((P/'window-epoch14/prefix-worker.json').read_text())['prefix_sha256'];assert re.fullmatch('[0-9a-f]{64}',prefix)
 if a.arm=='500k-reference':
  data.update({'--input':BASE+'/input/fineweb500k.f32','--input-sha256':prefix,'--output-dir':BASE+'/'+name})
 else:
  host,other=(a.worker_ip,a.peer_ip) if owner==1 else (a.peer_ip,a.worker_ip)
  data.update({'--mode':a.mode,'--owner':owner,'--output':BASE+'/'+name,'--input-descriptor':BASE+'/input/500k.json','--owner-budget':500000,'--first-owner-budget':0,'--shutdown-at-unix':a.job_deadline,'--run':int.from_bytes(hashlib.sha256((a.window_id+':'+a.arm).encode()).digest()[:16],'big'),'--data-bind':host+':9100','--control-bind':host+':9101','--peer-data':other+':9100','--peer-control':other+':9101'})
 v['provenance']={'artifact_hash_file_sha256':hashlib.sha256(hashfile.read_bytes()).hexdigest(),'arm':a.arm,'rows':500000,'input_bytes':1536000000,'input_sha256':prefix,'input_origin':'verified retained3M first500K rows','owner_local_construction':'capacity500K gives80%trigger400K; no moves at<=400K, final500Ktarget350K','external_profile':False,'replicates':1,'initial_batch':100000,'transition':1000000,'later_batch':1000000}
 remote.validate(v,now);return v
