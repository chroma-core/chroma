import pathlib,json,time,sys
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.subtree_orchestrate import ssh
from tools.hierarchical_sharding.subtree_transport import TreeCoordinator,TreeError
case=pathlib.Path('/mnt/spann/results/fineweb1m128-h2-a16-c16');out=pathlib.Path('/mnt/spann/results/load32-endpoint-startup-v1');out.mkdir(exist_ok=False)
for label in ('cohost','two-hosts'):
 path=case/('servers-'+label+'.json');old=json.load(open(path));(out/(label+'-prior-servers.json')).write_text(json.dumps(old));new=[]
 for s in old:
  command=['taskset','-c',s['cpu_affinity'],'python3','-m','tools.hierarchical_sharding.subtree_server','--config',s['config'],'--host','0.0.0.0','--port',pathlib.Path(s['config']).stem.split('-')[-1]]
  code='import pathlib,os,json,subprocess\np=pathlib.Path("/proc")/'+repr(str(s['pid']))+'\nassert not p.exists(),"Old adapter PID still exists; inspect identity before replacement"\nf=open('+repr(s['log'])+',"a")\np=subprocess.Popen('+repr(command)+',cwd="/mnt/spann/source",stdin=subprocess.DEVNULL,stdout=f,stderr=f,start_new_session=True)\nprint(p.pid)\n'
  entry=dict(s);entry['pid']=int(ssh(s['host'],code));new.append(entry)
 path.write_text(json.dumps(new,indent=2));coordinator=TreeCoordinator.from_files(json.load(open(case/'case.json'))['tree'],case/('placement-'+label+'.json'));start=time.monotonic();deadline=start+180
 try:
  while True:
   try:r=coordinator.verify_roots(timeout=min(30,max(.001,deadline-time.monotonic())));break
   except TreeError:
    if time.monotonic()>=deadline:raise
    time.sleep(.2)
 finally:coordinator.close()
 (out/(label+'-ready.json')).write_text(json.dumps(dict(ready=True,startup_s=time.monotonic()-start,verification=r)));print(json.dumps(dict(placement=label,ready=True)),flush=True)
