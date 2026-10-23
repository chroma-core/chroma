"""Bind one retained-binary reference or candidate3M arm; importing runs nothing."""
import hashlib,importlib.util,json,re
from pathlib import Path
P=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('job',P/'remote-job-epoch15.py');remote=importlib.util.module_from_spec(s);s.loader.exec_module(remote)
WORKER='i-011ff180f03584f40';PEER='i-00268d1e1dfea0357';BASE='/home/ubuntu/spann-two-machine-v1/epoch21'
INPUT='/home/ubuntu/spann-two-machine-v1/epoch2/input/fineweb3m.f32'
DESCRIPTOR='/home/ubuntu/spann-two-machine-v1/epoch2/input/3m.json'
INPUT_SHA='18fb3c71c45bb70d28730f8edfdbd2ae90a3361b150a4871f09d870cb6684aba'
EXPECTED={'reference':'8566a052e2fe0908188862c5ede2b98215664236198f6ab40a43193248016fb6','two_machine_spann':'5efa72832ae7647de5361dd45ed4a4a95f8905a8e47ee77ade79a8da5b539167','two_machine_reader':'5d98397f6a4b159cc6e818d667ddfb1888c4c75a6e78e4cb47d33e7b3c3917f4'}
def generate(a,now):
 assert a.arm in ('3m-reference','3m-distributed') and a.mode in ('run','serve')
 assert a.arm!='3m-reference' or a.mode=='run'
 assert re.fullmatch(r'epoch21-[a-z0-9-]+',a.window_id) and a.base==BASE and now<a.job_deadline<=now+590
 template=P/('window-epoch14/500k-reference/run.json' if a.arm=='3m-reference' else 'window-epoch20/500k-distributed/run.json')
 v=json.loads(template.read_text());assert 'profile_gate' not in v
 owner=1 if a.mode=='run' else 2;name=a.arm+f'-owner{owner}'
 v.update(job_id=name,window_id=a.window_id,deadline_unix=a.job_deadline,window_deadline_unix=a.window_deadline,mode=a.mode,instance_id=WORKER if owner==1 else PEER,minimum_free_bytes=(12 if a.arm=='3m-reference' else 4)*1024**3,minimum_mem_available_bytes=32*1024**3)
 hashfile=P/'runtime-binaries-epoch21.json';hashes=json.loads(hashfile.read_text());assert hashes==EXPECTED
 v.update(binary=('/home/ubuntu/spann-two-machine-v1/epoch12b/build/reference.bin' if a.arm=='3m-reference' else '/home/ubuntu/spann-two-machine-v1/epoch20/build/two_machine_spann'),binary_sha256=hashes['reference' if a.arm=='3m-reference' else 'two_machine_spann'],reader='/home/ubuntu/spann-two-machine-v1/epoch20/build/two_machine_reader',reader_sha256=hashes['two_machine_reader'])
 data=v['args'];data['--rows']=3000000;data['--workers']=32
 if a.arm=='3m-reference':data.update({'--input':INPUT,'--input-sha256':INPUT_SHA,'--output-dir':BASE+'/'+name})
 else:
  host,other=(a.worker_ip,a.peer_ip) if owner==1 else (a.peer_ip,a.worker_ip)
  data.update({'--mode':a.mode,'--owner':owner,'--output':BASE+'/'+name,'--input-descriptor':DESCRIPTOR,'--owner-budget':2160000,'--first-owner-budget':0,'--shutdown-at-unix':a.job_deadline,'--run':int.from_bytes(hashlib.sha256((a.window_id+':'+a.arm).encode()).digest()[:16],'big'),'--data-bind':host+':9100','--control-bind':host+':9101','--peer-data':other+':9100','--peer-control':other+':9101','--reader-binary':v['reader']})
 v['provenance']={'artifact_hash_file_sha256':hashlib.sha256(hashfile.read_bytes()).hexdigest(),'inherited_template_sha256':hashlib.sha256(template.read_bytes()).hexdigest(),'arm':a.arm,'rows':3000000,'input_bytes':9216000000,'input_sha256':INPUT_SHA,'external_profile':False,'replicates':1,'initial_batch':100000,'transition':1000000,'later_batch':1000000,'owner_capacity':2160000,'question':'Final1M construction work on both owners after2M redistribution','historical_reference':'6ff2767b... is not the matched instrumented reference'}
 remote.validate(v,now);return v
