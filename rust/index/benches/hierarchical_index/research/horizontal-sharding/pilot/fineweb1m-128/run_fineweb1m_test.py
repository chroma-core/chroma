import sys,pathlib,json,time,hashlib,subprocess,os
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.subtree_benchmark import measure,rows
from tools.hierarchical_sharding.subtree_orchestrate import ssh
from tools.hierarchical_sharding.subtree_transport import TreeCoordinator,TreeError
case=pathlib.Path('/mnt/spann/results/fineweb1m128-h2-a16-c16');selection=pathlib.Path('/mnt/spann/results/fineweb1m-read-calibration-v1/selection.json');chosen=json.loads(selection.read_text())['selected'];assert chosen is not None;out=pathlib.Path('/mnt/spann/results/fineweb1m-read-test-v1');out.mkdir(exist_ok=False);samplers=[]
# A reset removes retained application postings; it does not evict the OS cache.
for label in ('cohost','two-hosts'):
 path=case/('servers-'+label+'.json');old=json.loads(path.read_text());(out/('servers-'+label+'-before.json')).write_text(json.dumps(old,indent=2));new=[]
 for server in old:
  code='import os,pathlib,signal,time,subprocess\npid='+str(server['pid'])+'\np=pathlib.Path("/proc")/str(pid)\ncmd=(p/"cmdline").read_bytes()\nassert '+repr(server['config'].encode())+' in cmd and b"tools.hierarchical_sharding.subtree_server" in cmd\nassert os.getpgid(pid)==pid\nos.killpg(pid,signal.SIGTERM)\ntime.sleep(.1)\ncommand='+repr(['taskset','-c',server['cpu_affinity'],'python3','-m','tools.hierarchical_sharding.subtree_server','--config',server['config'],'--host','0.0.0.0','--port',pathlib.Path(server['config']).stem.split('-')[-1]])+'\nf=open('+repr(server['log'])+',"a")\np=subprocess.Popen(command,cwd="/mnt/spann/source",stdin=subprocess.DEVNULL,stdout=f,stderr=f,start_new_session=True)\nprint(p.pid)\n'
  entry=dict(server);entry['pid']=int(ssh(server['host'],code));new.append(entry)
 path.write_text(json.dumps(new,indent=2));coordinator=TreeCoordinator.from_files(json.loads((case/'case.json').read_text())['tree'],case/('placement-'+label+'.json'));deadline=time.monotonic()+120
 try:
  while True:
   try:coordinator.verify_roots(timeout=min(30,max(.001,deadline-time.monotonic())));break
   except TreeError:
    if time.monotonic()>deadline:raise
    time.sleep(.1)
 finally:coordinator.close()
 print(json.dumps(dict(stage='fresh_readers_ready',placement=label)),flush=True)
sample=out/'resources';sample.mkdir();groups={}
for label in ('cohost','two-hosts'):
 for server in json.loads((case/('servers-'+label+'.json')).read_text()):groups.setdefault(server['host'],{}).setdefault(label,[]).append(server['pid'])
for host,group in groups.items():
 directory=str(sample);code='import pathlib,json,subprocess\np=pathlib.Path('+repr(directory)+');p.mkdir(parents=True,exist_ok=True)\n(p/"groups.json").write_text('+repr(json.dumps(group))+')\np=subprocess.Popen(["python3","/mnt/spann/sample_process_groups.py","--groups",str(p/"groups.json"),"--output",str(p/"processes.jsonl"),"--duration","1800"],stdout=subprocess.DEVNULL,stderr=open(p/"stderr","w"),start_new_session=True)\nprint(p.pid)\n';samplers.append(dict(host=host,pid=int(ssh(host,code))))
driver=subprocess.Popen(['/mnt/spann/pilot-venv/bin/python','-m','tools.hierarchical_sharding.host_metrics','--pid',str(os.getpid()),'--interval','.5','--duration','1800','--output',str(sample/'driver.jsonl')],cwd='/mnt/spann/source');(sample/'samplers.json').write_text(json.dumps(dict(remote=samplers,driver=driver.pid),indent=2));data=pathlib.Path('/mnt/spann/fineweb1m-pilot-data');queries=rows(data/'test.jsonl');exact=rows(data/'test.exact.jsonl');tree=json.loads((case/'case.json').read_text())['tree'];results=[]
try:
 for label in ('cohost','two-hosts'):
  result=measure(tree,case/('placement-'+label+'.json'),queries,exact,chosen['beam_width'],chosen['candidate_budget'],out/('test-'+label+'.jsonl'));results.append(dict(placement=label,**result));print(json.dumps(dict(phase='heldout',placement=label,**result)),flush=True)
 if any(row['status']!='passed' for row in results):raise RuntimeError('held-out setting fails; journals retained')
 left=rows(out/'test-cohost.jsonl');right=rows(out/'test-two-hosts.jsonl');fields=('ids','distances','levels','reached_leaves','selected_candidates','global_candidate_count','candidate_contributions');failures=[]
 for a,b in zip(left,right):
  differences=[field for field in fields if a['result'][field]!=b['result'][field]]
  if a['query_id']!=b['query_id'] or differences:failures.append(dict(query_id=a['query_id'],fields=differences))
 assert len(left)==len(right)==100
 parity=dict(status='failed' if failures else 'passed',queries=100,failures=failures,fields=list(fields));(out/'parity.json').write_text(json.dumps(parity,indent=2));summary=dict(selection_sha256=hashlib.sha256(selection.read_bytes()).hexdigest(),selected=chosen,test=results,parity=parity,cache='Fresh application readers with one warmup query per placement; OS page cache remains warm and uncontrolled.');(out/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(dict(stage='heldout_complete',**summary)),flush=True)
 if failures:raise RuntimeError('same-tree placement parity fails')
finally:
 driver.terminate();driver.wait(timeout=10)
 for item in samplers:
  try:ssh(item['host'],'import os,signal\nos.kill('+str(item['pid'])+',signal.SIGTERM)\n')
  except Exception as error:print(json.dumps(dict(stage='sampler_stop_error',error=str(error))),flush=True)
