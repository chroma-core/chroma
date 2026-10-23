"""Serve an unchanged committed tree with a separately pinned adapter protocol."""
import argparse,json,pathlib,sys,subprocess,time,hashlib
p=argparse.ArgumentParser();p.add_argument('--baseline',required=True);p.add_argument('--output',required=True);p.add_argument('--source',required=True);a=p.parse_args()
sys.path.insert(0,a.source)
from tools.hierarchical_sharding.subtree_transport import TreeCoordinator
from tools.hierarchical_sharding.subtree_orchestrate import ssh
old=pathlib.Path(a.baseline);out=pathlib.Path(a.output);out.mkdir(exist_ok=False);case=json.loads((old/'case.json').read_text());(out/'case.json').write_text(json.dumps(case,indent=2));servers=[]
for label in ('cohost','two-hosts'):
 placement=json.loads((old/('placement-'+label+'.json')).read_text())
 for owner in placement['owners'].values():
  endpoint,port=owner['endpoint'].rsplit(':',1);owner['endpoint']=endpoint+':'+str(int(port)+200)
 (out/('placement-'+label+'.json')).write_text(json.dumps(placement,indent=2))
 for row in json.loads((old/('servers-'+label+'.json')).read_text()):
  source_config=json.loads(ssh(row['host'],'from pathlib import Path\nprint(Path('+repr(row['config'])+').read_text())\n'))
  source_config['owner_candidate_reduction']=True
  port=int(pathlib.Path(row['config']).stem.split('-')[-1])+200
  remote=pathlib.Path('/mnt/spann/results')/out.name;config=str(remote/('host-'+str(port)+'.json'));log=str(remote/('host-'+str(port)+'.log'))
  command=['taskset','-c',row['cpu_affinity'],'python3','-m','tools.hierarchical_sharding.subtree_server','--config',config,'--host','0.0.0.0','--port',str(port)]
  code='import pathlib,subprocess\np=pathlib.Path('+repr(str(remote))+');p.mkdir(parents=True,exist_ok=True)\npathlib.Path('+repr(config)+').write_text('+repr(json.dumps(source_config))+')\nf=open('+repr(log)+',"w")\np=subprocess.Popen('+repr(command)+',cwd='+repr(a.source)+',stdout=f,stderr=f,start_new_session=True)\nprint(p.pid)\n'
  record=dict(row,pid=int(ssh(row['host'],code)),config=config,log=log,source_root=a.source,config_sha256=hashlib.sha256(json.dumps(source_config).encode()).hexdigest());servers.append(record)
 (out/('servers-'+label+'.json')).write_text(json.dumps(servers[-len(json.loads((old/('servers-'+label+'.json')).read_text())):],indent=2))
 coordinator=TreeCoordinator.from_files(case['tree'],out/('placement-'+label+'.json'));started=time.monotonic();deadline=started+120
 try:
  while True:
   try:r=coordinator.verify_roots(timeout=min(30,max(.001,deadline-time.monotonic())));break
   except Exception:
    if time.monotonic()>=deadline:raise
    time.sleep(.2)
 finally:coordinator.close()
 (out/('ready-'+label+'.json')).write_text(json.dumps(dict(status='passed',startup_s=time.monotonic()-started,verification=r),indent=2));print(json.dumps(dict(placement=label,status='ready',startup_s=time.monotonic()-started)),flush=True)
