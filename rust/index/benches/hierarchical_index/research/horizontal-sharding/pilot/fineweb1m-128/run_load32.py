import hashlib,json,os,pathlib,subprocess,sys,time
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.subtree_benchmark import load

def main():
 root=pathlib.Path('/mnt/spann/results/fineweb1m-load32-v1');root.mkdir(exist_ok=False);source=pathlib.Path('/mnt/spann/source/tools/hierarchical_sharding/subtree_benchmark.py');selection=pathlib.Path('/mnt/spann/selection.json');case=pathlib.Path('/mnt/spann/results/fineweb1m128-h2-a16-c16')
 plan=dict(binary_sha256='7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158',tree_sha256='9635309389f9b57694f936effc66296d75c1521ae07f2d28d6ddf35601d63fc5',harness_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),selection_sha256=hashlib.sha256(selection.read_bytes()).hexdigest(),driver_instance_id='i-05c41c0090a257a06',driver_cpu_affinity=sorted(os.sched_getaffinity(0)),driver_vcpus=32,driver_memory_gib=128,server_allocation_logical_cpus_per_host=16,adapters_per_host=4,reader_epoch='memory-v4',logical_clients=[1,2,4,8,16],offered_window_s=15,scope='Three alternating paired repetitions, same warm held-out100 query pool. One coordinator process per logical client. Application/server caches warm; OS cache uncontrolled. Journaling and trace hashing between requests count in throughput but not query latency.',started_unix=time.time());(root/'plan.json').write_text(json.dumps(plan,indent=2));(root/'groups.json').write_text(json.dumps(dict(coordinator=[os.getpid()])))
 sampler=subprocess.Popen(['python3','/mnt/spann/sample_process_groups.py','--groups',str(root/'groups.json'),'--output',str(root/'driver.jsonl'),'--duration','1800'])
 try:
  for rep,labels in enumerate((('cohost','two-hosts'),('two-hosts','cohost'),('cohost','two-hosts')),1):
   load(case,'/mnt/spann/fineweb1m-pilot-data',selection,root/f'repetition-{rep}',15,labels,(1,2,4,8,16))
 finally:sampler.terminate();sampler.wait(timeout=10)
 (root/'complete.json').write_text(json.dumps(dict(status='passed',finished_unix=time.time())))
if __name__=='__main__':main()
