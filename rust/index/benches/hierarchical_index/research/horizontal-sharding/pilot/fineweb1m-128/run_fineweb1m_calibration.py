import sys,pathlib,json,os,time,subprocess,hashlib
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.subtree_benchmark import curve
from tools.hierarchical_sharding.subtree_orchestrate import ssh
case=pathlib.Path('/mnt/spann/results/fineweb1m128-h2-a16-c16');out=pathlib.Path('/mnt/spann/results/fineweb1m-read-calibration-v1');sample=pathlib.Path('/mnt/spann/results/fineweb1m-read-samples-v1');sample.mkdir(exist_ok=False);servers={};samplers=[]
for label in ('cohost','two-hosts'):
 for row in json.loads((case/('servers-'+label+'.json')).read_text()):servers.setdefault(row['host'],{}).setdefault(label,[]).append(row['pid'])
for host,groups in servers.items():
 directory=str(sample);groups_path=directory+'/groups.json';output=directory+'/processes.jsonl';code='import pathlib,subprocess\np=pathlib.Path('+repr(directory)+');p.mkdir(parents=True,exist_ok=True)\npathlib.Path('+repr(groups_path)+').write_text('+repr(json.dumps(groups))+')\np=subprocess.Popen(["python3","/mnt/spann/sample_process_groups.py","--groups",'+repr(groups_path)+',"--output",'+repr(output)+',"--duration","7200"],stdout=subprocess.DEVNULL,stderr=open('+repr(directory+'/sampler.stderr')+',"w"),start_new_session=True)\nprint(p.pid)\n';samplers.append(dict(host=host,pid=int(ssh(host,code))))
driver=subprocess.Popen(['/mnt/spann/pilot-venv/bin/python','-m','tools.hierarchical_sharding.host_metrics','--pid',str(os.getpid()),'--interval','.5','--duration','7200','--output',str(sample/'driver.jsonl')],cwd='/mnt/spann/source');(sample/'samplers.json').write_text(json.dumps(dict(remote=samplers,driver=driver.pid),indent=2))
epoch=json.loads((case/'read-epoch.json').read_text());base=pathlib.Path('/mnt/spann/source/tools/hierarchical_sharding');epoch['source_sha256']={name:hashlib.file_digest(open(base/name,'rb'),'sha256').hexdigest() for name in epoch['source_sha256']};(case/'read-epoch.json').write_text(json.dumps(epoch,indent=2))
try:
 result=curve(case,'/mnt/spann/fineweb1m-pilot-data',out,[(b,m) for b in (64,128,256,512,1024) for m in (1000,4000)],.95,calibration_only=True);print(json.dumps(dict(stage='calibration_finished',**result)),flush=True)
finally:
 driver.terminate();driver.wait(timeout=10)
 for item in samplers:
  try:ssh(item['host'],'import os,signal\nos.kill('+str(item['pid'])+',signal.SIGTERM)\n')
  except Exception as error:print(json.dumps(dict(stage='sampler_stop_error',error=str(error))),flush=True)
