import argparse,os,json,time,subprocess,hashlib,shlex,shutil,threading,fcntl
from pathlib import Path
r=Path('/mnt/msmarco/version-cost-profile-20261020'); a=r/'artifacts'
p=argparse.ArgumentParser();p.add_argument('variant',choices=['baseline','instrumented']);p.add_argument('millions',type=int,choices=[1,3]);p.add_argument('repeat',type=int);p.add_argument('--perf',action='store_true');arg=p.parse_args()
lock=open('/mnt/data/chroma-benchmark.lock','w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
sha=(a/f'{arg.variant}.sha').read_text().strip(); name=f'wikipedia-{arg.variant}-{arg.millions}m-{arg.repeat}';out=r/name;out.mkdir();h=out/'home';(h/'.cache/wikipedia_en').mkdir(parents=True)
shutil.copyfile(a/'wikipedia-gt.parquet',h/'.cache/wikipedia_en/ground_truth.parquet');(h/'.cache/huggingface').symlink_to('/mnt/data/huggingface',target_is_directory=True)
b=a/f'{arg.variant}-binary'; args=[str(b),'--dataset','wikipedia-en','--metric','l2','--checkpoint',str(10 if arg.millions==1 else 3),'--checkpoint-size',str(100000 if arg.millions==1 else 1000000),'--branching-factor','100','--split-threshold','2048','--merge-threshold','512','--threads','32','--balance-threads','32','--write-navigation','fp','--fp-npa','--write-beam-tau','0.5','--write-beam-min','10','--write-beam-max','16','--max-cache-bytes','1073741824','--compute-gt-clusters','false','--clear-reader-block-pins','true','--read-beam-min','10','--read-beam-max','256','--recall-tau-values','1.5,2','--recall-rerank-vectors','1,8','--num-queries','100','--brute-force-gt','false','--save','--save-dir',str(out/'index'),'--write-level-taus','_,_,0.30','--read-level-taus','_,_,_','--read-level-min-pcts','50,5,0']
env=dict(os.environ,HOME=str(h),HF_HOME='/mnt/data/huggingface',HF_HUB_OFFLINE='1',RAYON_NUM_THREADS='32',TOKIO_WORKER_THREADS='32',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',TMPDIR=str(r/'tmp'))
(a/f'{name}.command').write_text(shlex.join(args)+'\n');(a/f'{name}.command.json').write_text(json.dumps(args,indent=2)+'\n')
meta={'variant':arg.variant,'sha':sha,'binary_sha256':hashlib.sha256(b.read_bytes()).hexdigest(),'started_unix':time.time(),'threads':32,'validation_enabled':False,'versioned_baseline':True,'sampling':arg.perf,'sample_interval_seconds':1}
(a/f'{name}.started.json').write_text(json.dumps(meta,indent=2)+'\n')
if arg.variant=='instrumented':env['VF_PROFILE_MARKERS']='1'
start=time.monotonic(); proc=subprocess.Popen(['/usr/bin/time','-v','-o',str(a/f'{name}.time')]+args,cwd=out,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1)
def sample():
 with (a/f'{name}.resources.jsonl').open('x') as f:
  while proc.poll() is None:
   record={'unix':time.time(),'elapsed':time.monotonic()-start}
   try:
    kids=Path(f'/proc/{proc.pid}/task/{proc.pid}/children').read_text().split();pid=int(kids[0]) if kids else proc.pid;base=Path(f'/proc/{pid}')
    record.update(pid=pid,stat=(base/'stat').read_text(),io=(base/'io').read_text(),status=(base/'status').read_text())
    switches=[0,0]
    for task in (base/'task').iterdir():
     try:
      for line in (task/'status').read_text().splitlines():
       if line.startswith('voluntary_ctxt_switches:'):switches[0]+=int(line.split()[1])
       if line.startswith('nonvoluntary_ctxt_switches:'):switches[1]+=int(line.split()[1])
     except OSError:pass
    record['task_context_switches']=switches
   except OSError as error:record["sample_error"]=str(error)
   f.write(json.dumps(record)+'\n');f.flush();time.sleep(1)
thread=threading.Thread(target=sample,daemon=True);thread.start()
profiler=None
if arg.perf:
 for _ in range(1000):
  children=Path(f'/proc/{proc.pid}/task/{proc.pid}/children').read_text().split()
  if children:break
  time.sleep(.001)
 assert children
 perfargs=['sudo','-n','perf','record','--clockid','CLOCK_REALTIME','-F','49','-e','cpu-clock:u','--call-graph','dwarf,16384','-p',children[0],'-o',str(a/f'{name}.perf.data')]
 (a/f'{name}.perf-command.json').write_text(json.dumps(perfargs,indent=2))
 perflog=(a/f'{name}.perf.log').open('x');profiler=subprocess.Popen(perfargs,stdout=perflog,stderr=subprocess.STDOUT)

with (a/f'{name}.log').open('x') as log,(a/f'{name}.lines.jsonl').open('x') as times:
 for line in proc.stdout:
  offset=log.tell();log.write(line);log.flush();times.write(json.dumps({'unix':time.time(),'elapsed':time.monotonic()-start,'offset':offset})+'\n')
rc=proc.wait();
if profiler is not None:
 perf_rc=profiler.wait(timeout=30);perflog.close();meta['perf_returncode']=perf_rc
meta.update(returncode=rc,elapsed_seconds=time.monotonic()-start,finished_unix=time.time());(a/f'{name}.result.json').write_text(json.dumps(meta,indent=2)+'\n')
if rc:raise SystemExit(rc)
ck=json.loads((out/'index/checkpoint.json').read_text());assert ck['total_vectors']==arg.millions*1000000
shutil.copyfile(out/'index/checkpoint.json',a/f'{name}.checkpoint.json')
log=(a/f'{name}.log').read_text();assert 'Computing ground truth' not in log and 'using queries keyed at full corpus' not in log
print(name,'completed',meta['elapsed_seconds'],flush=True)
