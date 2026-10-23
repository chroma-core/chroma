import sys,pathlib,json,os,subprocess,hashlib,time
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.subtree_benchmark import load
from tools.hierarchical_sharding.subtree_orchestrate import ssh

def main():
 case=pathlib.Path('/mnt/spann/results/fineweb1m128-h2-a16-c16');selection=pathlib.Path('/mnt/spann/results/fineweb1m-read-calibration-v1/selection.json');sample=pathlib.Path('/mnt/spann/results/fineweb1m-read-load-resources-v1');sample.mkdir(exist_ok=False);samplers=[];groups={};source=pathlib.Path('/mnt/spann/source/tools/hierarchical_sharding/subtree_benchmark.py')
 plan=dict(binary_epoch='memory-v4',binary_sha256='7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158',tree_sha256='9635309389f9b57694f936effc66296d75c1521ae07f2d28d6ddf35601d63fc5',selection_sha256=hashlib.sha256(selection.read_bytes()).hexdigest(),harness_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),logical_clients=[1,2,4,8,16],coordinator_processes='One independently spawned process per logical client; all roots verified before shared start barrier.',offered_window_s=15,driver_logical_cpus=sorted(os.sched_getaffinity(0)),per_host_logical_cpus=16,adapters_per_host=4,scope='Exploratory single windows over the same warm100-query held-out pool. No supported tail estimate. Throughput includes journals and full trace hashing between requests; query latency ends before that work.16clients oversubscribe the8-vCPU driver and must be interpreted with measured CPU/RSS/context switches.',started_unix=time.time());(sample/'plan.json').write_text(json.dumps(plan,indent=2))
 for label in ('cohost','two-hosts'):
  for server in json.loads((case/('servers-'+label+'.json')).read_text()):groups.setdefault(server['host'],{}).setdefault(label,[]).append(server['pid'])
 for host,group in groups.items():
  directory=str(sample);code='import pathlib,subprocess\np=pathlib.Path('+repr(directory)+');p.mkdir(parents=True,exist_ok=True)\n(p/"groups.json").write_text('+repr(json.dumps(group))+')\np=subprocess.Popen(["python3","/mnt/spann/sample_process_groups.py","--groups",str(p/"groups.json"),"--output",str(p/"processes.jsonl"),"--duration","3600"],stdout=subprocess.DEVNULL,stderr=open(p/"stderr","w"),start_new_session=True)\nprint(p.pid)\n';samplers.append(dict(host=host,pid=int(ssh(host,code))))
 (sample/'driver-groups.json').write_text(json.dumps(dict(coordinator=[os.getpid()])));driver=subprocess.Popen(['python3','/mnt/spann/sample_process_groups.py','--groups',str(sample/'driver-groups.json'),'--output',str(sample/'driver.jsonl'),'--duration','3600']);(sample/'samplers.json').write_text(json.dumps(dict(remote=samplers,driver=driver.pid),indent=2))
 try:load(case,'/mnt/spann/fineweb1m-pilot-data',selection,'/mnt/spann/results/fineweb1m-read-load-v1',15,('cohost','two-hosts'),(1,2,4,8,16))
 finally:
  driver.terminate();driver.wait(timeout=10)
  for row in samplers:
   try:ssh(row['host'],'import os,signal\nos.kill('+str(row['pid'])+',signal.SIGTERM)\n')
   except Exception as error:print(json.dumps(dict(stage='sampler_stop_error',error=str(error))),flush=True)
if __name__=='__main__':main()
