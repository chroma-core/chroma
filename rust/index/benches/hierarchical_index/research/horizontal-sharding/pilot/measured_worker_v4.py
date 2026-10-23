#!/usr/bin/env python3
"""Measure a worker without rounding completion to the sampling interval."""
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import threading
import time
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.host_metrics import snapshot

args=sys.argv[1:]
index=args[args.index('--checkpoint-dir')+1]
output=Path('/mnt/spann/results')/(Path(index).name+'-'+str(time.time_ns()))
output.mkdir(parents=True)
os.environ.setdefault('TOKIO_WORKER_THREADS','2')
binary=os.environ.get('SPANN_WORKER_BINARY','/mnt/spann/hierarchical_spann_shard-audited-v3')
configuration=dict(command=[binary]+args,checkpoint_dir=index,started_unix=time.time(),runtime_threads=os.environ['TOKIO_WORKER_THREADS'],cpu_affinity=sorted(os.sched_getaffinity(0)),sampling_interval_s=.25,measurement_epoch='process-wait-v4')
(output/'configuration.json').write_text(json.dumps(configuration,indent=2))
stop=threading.Event();started=time.monotonic();process=subprocess.Popen([binary]+args)
def sample():
    with (output/'host.jsonl').open('w') as stream:
        while True:
            stream.write(json.dumps(snapshot(process.pid))+'\n');stream.flush()
            if stop.wait(.25):break
sampler=threading.Thread(target=sample);sampler.start()
status=process.wait();finished=time.monotonic();stop.set();sampler.join()
rusage=resource.getrusage(resource.RUSAGE_CHILDREN)
result=dict(configuration,returncode=status,finished_unix=time.time(),elapsed_s=finished-started,ru_maxrss_kib=rusage.ru_maxrss,user_cpu_s=rusage.ru_utime,system_cpu_s=rusage.ru_stime,minor_faults=rusage.ru_minflt,major_faults=rusage.ru_majflt,input_blocks=rusage.ru_inblock,output_blocks=rusage.ru_oublock)
(output/'process.json').write_text(json.dumps(result,indent=2)+'\n')
sys.exit(status)
