#!/usr/bin/env python3
"""Run the shard executable with host/PID series and final child rusage."""
import sys,os,time,json,pathlib,subprocess,resource,shutil
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.host_metrics import snapshot
args=sys.argv[1:];index=args[args.index('--checkpoint-dir')+1];tag=pathlib.Path(index).name+'-'+str(time.time_ns());out=pathlib.Path('/mnt/spann/results')/tag;out.mkdir(parents=True)
os.environ.setdefault('TOKIO_WORKER_THREADS','2')
start=time.monotonic();before=snapshot();p=subprocess.Popen(['/mnt/spann/hierarchical_spann_shard']+args)
with (out/'host.jsonl').open('w') as f:
 f.write(json.dumps(before)+'\n')
 while True:
  row=snapshot(p.pid);row['filesystem_bytes_used']=shutil.disk_usage('/mnt/spann').used;f.write(json.dumps(row)+'\n');f.flush()
  status=p.poll()
  if status is not None:break
  time.sleep(.25)
ru=resource.getrusage(resource.RUSAGE_CHILDREN)
(out/'process.json').write_text(json.dumps(dict(runtime_threads=os.environ['TOKIO_WORKER_THREADS'],command=p.args,returncode=p.returncode,elapsed_s=time.monotonic()-start,ru_maxrss_kib=ru.ru_maxrss,user_cpu_s=ru.ru_utime,system_cpu_s=ru.ru_stime,minor_faults=ru.ru_minflt,major_faults=ru.ru_majflt,input_blocks=ru.ru_inblock,output_blocks=ru.ru_oublock),indent=2)+'\n')
sys.exit(p.returncode)
