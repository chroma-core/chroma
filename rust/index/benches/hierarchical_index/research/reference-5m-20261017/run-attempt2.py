import subprocess,os,time,json,hashlib,shutil
from pathlib import Path
r=Path('/mnt/spann/reference-5m-20261017/attempt2'); r.mkdir(exist_ok=False); out=Path('/mnt/spann-matched-2m-dynamic-v1/reference-5m-20261017-attempt2')
out.mkdir(exist_ok=False)
b=Path('/mnt/spann/reference-5m-20261017/build/native')
assert json.loads((b.parent/'result.json').read_text())['returncode']==0
args=[str(b),'--dataset','wikipedia-en','--metric','l2','--checkpoint','5','--checkpoint-size','1000000','--branching-factor','100','--split-threshold','2048','--merge-threshold','512','--threads','8','--write-navigation','fp','--fp-npa','--write-beam-tau','0.5','--write-beam-min','10','--write-beam-max','16','--write-level-taus','_,_,0.30','--max-replicas','1','--write-rng-epsilon','0','--write-rng-factor','0','--read-beam-min','10','--read-beam-max','256','--read-level-taus','_,_,_','--read-level-min-pcts','50,5,0','--recall-rerank-vectors','1,8','--recall-tau-values','0.7,1,2','--num-queries','100','--max-cache-bytes','1073741824','--compute-gt-clusters','false','--brute-force-gt','true','--save','--save-dir',str(out/'index')]
(r/'command.json').write_text(json.dumps(args,indent=2)+'\n')
import shlex
(r/'command.txt').write_text(shlex.join(args)+'\n')
env=dict(os.environ,RAYON_NUM_THREADS='8',TOKIO_WORKER_THREADS='8',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
start=time.time();reason=None
with (r/'full-cli-output.txt').open('xb') as log, (r/'resource-samples.jsonl').open('x') as samples:
 p=subprocess.Popen(['/usr/bin/time','-v','-o',str(r/'process-time.txt')]+args,cwd=r,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 (r/'started.json').write_text(json.dumps({'pid':p.pid,'started_unix':start,'max_seconds':2700,'binary_sha256':hashlib.sha256(b.read_bytes()).hexdigest()}))
 while p.poll() is None:
  rootfree=shutil.disk_usage('/').free; datafree=shutil.disk_usage(out).free
  samples.write(json.dumps({'unix':time.time(),'root_free':rootfree,'data_free':datafree})+'\n');samples.flush()
  if time.time()-start>2700:reason='2700-second runtime limit'
  if min(rootfree,datafree)<4*1024**3:reason='4GiB free-space floor'
  if reason:
   import signal
   os.killpg(p.pid,signal.SIGTERM)
   try:p.wait(timeout=20)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
   break
  time.sleep(5)
(r/'result.json').write_text(json.dumps({'returncode':p.returncode,'stop_reason':reason,'elapsed_seconds':time.time()-start,'log_bytes':(r/'full-cli-output.txt').stat().st_size},indent=2)+'\n')
