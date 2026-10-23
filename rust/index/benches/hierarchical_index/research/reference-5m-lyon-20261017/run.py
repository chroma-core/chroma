import os,json,time,subprocess,hashlib,shlex,fcntl
from pathlib import Path
r=Path('/mnt/data/reference-5m-lyon-20261017')
lock=open('/mnt/data/chroma-benchmark.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
assert not (r/'started.json').exists()
h=r/'home';h.mkdir(exist_ok=True)
b=Path('/home/ubuntu/benchmark-data/wikipedia-40m-new-baseline/binary')
assert hashlib.sha256(b.read_bytes()).hexdigest()=='a3bd63919af743118373aa6b17c7f80d27d522dcff0a4f9ad56a12b1cc35af8c'
a=[str(b),'--dataset','wikipedia-en','--metric','l2','--checkpoint','5','--checkpoint-size','1000000','--branching-factor','100','--split-threshold','2048','--merge-threshold','512','--threads','32','--balance-threads','32','--write-navigation','fp','--fp-npa','--write-beam-tau','0.5','--write-beam-min','10','--write-beam-max','16','--write-level-taus','_,_,0.30','--max-replicas','1','--write-rng-epsilon','0','--write-rng-factor','0','--read-beam-min','10','--read-beam-max','256','--read-level-taus','_,_,_','--read-level-min-pcts','50,5,0','--recall-rerank-vectors','1,8','--recall-tau-values','0.7,1,2','--num-queries','100','--max-cache-bytes','1073741824','--compute-gt-clusters','false','--brute-force-gt','true','--save','--save-dir',str(r/'index')]
(r/'command.txt').write_text(shlex.join(a)+'\n')
e=dict(os.environ,HOME=str(h),HF_HOME='/mnt/data/huggingface',HF_HUB_OFFLINE='1',RAYON_NUM_THREADS='32',TOKIO_WORKER_THREADS='32',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
t=time.time();(r/'started.json').write_text(json.dumps({'started_unix':t,'binary_sha256':hashlib.sha256(b.read_bytes()).hexdigest(),'threads':32,'memory_limit':'none imposed','source_commit':'bec6e7213b37ca97066545535bc82d4252f9c0dc','query_seed':0x7810}))
with (r/'full-cli-output.txt').open('xb') as log:
 p=subprocess.run(['/usr/bin/time','-v','-o',str(r/'process-time.txt')]+a,cwd=r,env=e,stdout=log,stderr=subprocess.STDOUT)
(r/'result.json').write_text(json.dumps({'returncode':p.returncode,'elapsed_seconds':time.time()-t},indent=2))
