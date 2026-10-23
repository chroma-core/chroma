"""Restart only recorded readers so each held-out placement starts with fresh application caches."""
import pathlib,json,sys,time
source='/mnt/spann/owner-reduction-source-v2';sys.path.insert(0,source)
from tools.hierarchical_sharding.subtree_orchestrate import ssh
from tools.hierarchical_sharding.subtree_transport import TreeCoordinator
case=pathlib.Path('/mnt/spann/results/fineweb10m128-h2-a16-c16-v2');restart_epoch=sys.argv[1] if len(sys.argv)>1 else 'heldout-restart-v1';assert restart_epoch in ('heldout-restart-v1','heldout-restart-v2');out=case/restart_epoch;out.mkdir(exist_ok=False);metadata=json.loads((case/'case.json').read_text());groups={}
for label in ('cohost','two-hosts'):
 rows=json.loads((case/('servers-'+label+'.json')).read_text());groups[label]=rows;(out/('servers-'+label+'-before.json')).write_text(json.dumps(rows,indent=2))
 for row in rows:
  code="import pathlib,os,signal\npid="+repr(row['pid'])+";p=pathlib.Path('/proc')/str(pid)\na=(p/'cmdline').read_bytes().split(bytes([0]));assert "+repr(row['config'].encode())+" in a and b'tools.hierarchical_sharding.subtree_server' in a\ns=(p/'stat').read_text().rsplit(')',1)[1].split();assert int(s[2])==pid\nos.killpg(pid,signal.SIGTERM)\n"
  ssh(row['host'],code)
for label,rows in groups.items():
 fresh=[]
 for row in rows:
  port=pathlib.Path(row['config']).stem.split('-')[-1];log=str(pathlib.Path(row['log']).with_name(restart_epoch+'-host-'+port+'.log'));command=['taskset','-c',row['cpu_affinity'],'python3','-m','tools.hierarchical_sharding.subtree_server','--config',row['config'],'--host','0.0.0.0','--port',port]
  code='import subprocess\nf=open('+repr(log)+',"w")\np=subprocess.Popen('+repr(command)+',cwd='+repr(source)+',stdout=f,stderr=f,start_new_session=True)\nprint(p.pid)\n'
  fresh.append(dict(row,pid=int(ssh(row['host'],code)),log=log,source_root=source))
 (case/('servers-'+label+'.json')).write_text(json.dumps(fresh,indent=2));coordinator=TreeCoordinator.from_files(metadata['tree'],case/('placement-'+label+'.json'));started=time.monotonic();deadline=started+120
 try:
  while True:
   try:r=coordinator.verify_roots(timeout=min(30,max(.001,deadline-time.monotonic())));break
   except Exception:
    if time.monotonic()>=deadline:raise
    time.sleep(.2)
 finally:coordinator.close()
 (out/('ready-'+label+'.json')).write_text(json.dumps(dict(status='passed',startup_s=time.monotonic()-started,verification=r,cache='Fresh application readers; OS/page cache remains uncontrolled and warm.'),indent=2));print(json.dumps(dict(placement=label,status='restarted',startup_s=time.monotonic()-started)),flush=True)
