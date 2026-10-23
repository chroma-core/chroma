import argparse,os,json,time,subprocess,hashlib,shlex,shutil,threading,fcntl
from pathlib import Path
r=Path('/mnt/msmarco/version-free-followup-20261020'); a=r/'artifacts'
arg=argparse.Namespace(variant='replay',millions=9,repeat=1)
lock=open('/mnt/data/chroma-benchmark.lock','w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
sha=(a/f'{arg.variant}.sha').read_text().strip(); name=f'wikipedia-{arg.variant}-{arg.millions}m-{arg.repeat}';out=r/name;out.mkdir();h=out/'home';(h/'.cache/wikipedia_en').mkdir(parents=True)
shutil.copyfile(a/'wikipedia-gt.parquet',h/'.cache/wikipedia_en/ground_truth.parquet');(h/'.cache/huggingface').symlink_to('/mnt/data/huggingface',target_is_directory=True)
source=r/'wikipedia-diagnostic-9m-1/snapshots/8000000'
shutil.copytree(source/'data',out/'index/data',copy_function=shutil.copy2)
shutil.copyfile(source/'checkpoint.json',out/'index/checkpoint.json')
(a/'replay-source-checkpoint.before.json').write_bytes((source/'checkpoint.json').read_bytes())
b=a/f'{arg.variant}-binary'; args=[str(b),'--dataset','wikipedia-en','--metric','l2','--checkpoint',str(10 if arg.millions==1 else 9),'--checkpoint-size',str(100000 if arg.millions==1 else 1000000),'--branching-factor','100','--split-threshold','2048','--merge-threshold','512','--threads','32','--balance-threads','32','--write-navigation','fp','--fp-npa','--write-beam-tau','0.5','--write-beam-min','10','--write-beam-max','16','--max-cache-bytes','1073741824','--compute-gt-clusters','false','--clear-reader-block-pins','true','--read-beam-min','10','--read-beam-max','256','--recall-tau-values','1.5,2','--recall-rerank-vectors','1,8','--num-queries','0','--brute-force-gt','false','--resume','--save','--save-dir',str(out/'index'),'--write-level-taus','_,_,0.30','--read-level-taus','_,_,_','--read-level-min-pcts','50,5,0']
env=dict(os.environ,HOME=str(h),HF_HOME='/mnt/data/huggingface',HF_HUB_OFFLINE='1',RAYON_NUM_THREADS='32',TOKIO_WORKER_THREADS='32',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',TMPDIR=str(r/'tmp'))
(a/f'{name}.command').write_text(shlex.join(args)+'\n');(a/f'{name}.command.json').write_text(json.dumps(args,indent=2)+'\n')
meta={'variant':arg.variant,'sha':sha,'binary_sha256':hashlib.sha256(b.read_bytes()).hexdigest(),'started_unix':time.time(),'threads':32,'validation_enabled':False,'production_preflight_included':True,'sample_interval_seconds':1,'diagnostic_only':True,'resume_from':8000000}
(a/f'{name}.started.json').write_text(json.dumps(meta,indent=2)+'\n')
start=time.monotonic(); proc=subprocess.Popen(['/usr/bin/time','-v','-o',str(a/f'{name}.time')]+args,cwd=out,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1)
def sample():
 with (a/f'{name}.resources.jsonl').open('x') as f:
  while proc.poll() is None:
   record={'unix':time.time(),'elapsed':time.monotonic()-start}
   try:
    kids=Path(f'/proc/{proc.pid}/task/{proc.pid}/children').read_text().split();pid=int(kids[0]) if kids else proc.pid;base=Path(f'/proc/{pid}')
    record.update(pid=pid,stat=(base/'stat').read_text(),io=(base/'io').read_text(),status=(base/'status').read_text())
    switches=[0,0];record['thread_stats']=[]
    for task in (base/'task').iterdir():
     try:
      record['thread_stats'].append({'tid':task.name,'stat':(task/'stat').read_text(),'wchan':(task/'wchan').read_text()})
      for line in (task/'status').read_text().splitlines():
       if line.startswith('voluntary_ctxt_switches:'):switches[0]+=int(line.split()[1])
       if line.startswith('nonvoluntary_ctxt_switches:'):switches[1]+=int(line.split()[1])
     except OSError:pass
    record['task_context_switches']=switches
   except OSError as error:record["sample_error"]=str(error)
   f.write(json.dumps(record)+'\n');f.flush();time.sleep(1)
thread=threading.Thread(target=sample,daemon=True);thread.start()
with (a/f'{name}.log').open('x') as log,(a/f'{name}.lines.jsonl').open('x') as times:
 for line in proc.stdout:
  offset=log.tell();log.write(line);log.flush();times.write(json.dumps({'unix':time.time(),'elapsed':time.monotonic()-start,'offset':offset})+'\n')
rc=proc.wait();meta.update(returncode=rc,elapsed_seconds=time.monotonic()-start,finished_unix=time.time());(a/f'{name}.result.json').write_text(json.dumps(meta,indent=2)+'\n')
if rc:raise SystemExit(rc)
ck=json.loads((out/'index/checkpoint.json').read_text());assert ck['total_vectors']==arg.millions*1000000
shutil.copyfile(out/'index/checkpoint.json',a/f'{name}.checkpoint.json')
log=(a/f'{name}.log').read_text();assert 'Computing ground truth' not in log and 'using queries keyed at full corpus' not in log
print(name,'completed',meta['elapsed_seconds'],flush=True)

assert (source/'checkpoint.json').read_bytes()==(a/'replay-source-checkpoint.before.json').read_bytes()
