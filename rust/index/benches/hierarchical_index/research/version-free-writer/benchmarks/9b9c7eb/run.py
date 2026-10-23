import os,json,time,subprocess,hashlib,shlex,shutil
from pathlib import Path
r=Path('/mnt/msmarco/version-free-writer-7927-9b9c7eb'); a=r/'artifacts'
assert (a/'gt-verification.json').exists()
assert (a/'builds-complete').exists()
shas={'baseline':'1f433be7969b3d28c78e463fe33a299fa0c5fee9','candidate':'9b9c7ebde93144055b1fc86ad29e7ea3b64c140b'}
def run(dataset,variant,repeat=1):
 name=f'{dataset}-{variant}-{repeat}'; out=r/name; out.mkdir(); threads=32 if dataset=='wikipedia' else 64
 h=out/'home'; (h/'.cache').mkdir(parents=True)
 for ds,cache in [('wikipedia','wikipedia_en'),('msmarco','msmarco_v2')]:
  (h/'.cache'/cache).mkdir(); shutil.copyfile(a/f'{ds}-gt.parquet',h/'.cache'/cache/'ground_truth.parquet')
 (h/'.cache/huggingface').symlink_to('/mnt/data/huggingface',target_is_directory=True)
 b=a/f'{variant}-binary'; args=[str(b),'--dataset','wikipedia-en' if dataset=='wikipedia' else 'ms-marco','--metric','l2','--checkpoint','10','--checkpoint-size','100000','--branching-factor','100','--split-threshold','2048','--merge-threshold','512','--threads',str(threads),'--balance-threads',str(threads),'--write-navigation','fp','--fp-npa','--write-beam-tau','0.5' if dataset=='wikipedia' else '1.5','--write-beam-min','10','--write-beam-max','16','--max-cache-bytes','1073741824','--compute-gt-clusters','false','--clear-reader-block-pins','true','--read-beam-min','10','--read-beam-max','256' if dataset=='wikipedia' else '128','--recall-tau-values','1.5,2','--recall-rerank-vectors','1,8','--num-queries','100','--brute-force-gt','false','--validate-postings','--save','--save-dir',str(out/'index')]
 if dataset=='wikipedia':args+=['--write-level-taus','_,_,0.30','--read-level-taus','_,_,_','--read-level-min-pcts','50,5,0']
 env=dict(os.environ,HOME=str(h),HF_HOME='/mnt/data/huggingface',HF_HUB_OFFLINE='1',RAYON_NUM_THREADS=str(threads),TOKIO_WORKER_THREADS=str(threads),OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',TMPDIR=str(r/'tmp'))
 (a/f'{name}.command').write_text(shlex.join(args)+'\n'); (a/f'{name}.command.json').write_text(json.dumps(args,indent=2)+'\n')
 meta={'dataset':dataset,'variant':variant,'sha':shas[variant],'binary_sha256':hashlib.sha256(b.read_bytes()).hexdigest(),'started_unix':time.time(),'threads':threads,'validation_enabled':True}
 (a/f'{name}.started.json').write_text(json.dumps(meta,indent=2)+'\n');print('Starting',name,flush=True)
 t=time.monotonic()
 with (a/f'{name}.log').open('x') as log: rc=subprocess.run(['/usr/bin/time','-v','-o',str(a/f'{name}.time')]+args,cwd=out,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
 meta.update(returncode=rc,elapsed_seconds=time.monotonic()-t,finished_unix=time.time());(a/f'{name}.result.json').write_text(json.dumps(meta,indent=2)+'\n')
 print('Finished',name,meta['elapsed_seconds'],'rc',rc,flush=True)
 if rc:raise SystemExit(rc)
 ck=json.loads((out/'index/checkpoint.json').read_text());assert ck['total_vectors']==1000000 and ck['checkpoint_idx']==9
 shutil.copyfile(out/'index/checkpoint.json',a/f'{name}.checkpoint.json')
 log=(a/f'{name}.log').read_text();assert log.count('[posting validation] after reopen:')==10;assert 'Computing ground truth' not in log and 'using queries keyed at full corpus' not in log
for dataset in ['wikipedia','msmarco']:
 for variant in (['candidate'] if dataset=='wikipedia' else ['baseline','candidate']):run(dataset,variant)
(a/'first-pairs-complete').write_text(str(time.time()))
# Retain the shared lock until the coordinator decides whether a reverse-order repeat is warranted.
while not (r/'followup-ready').exists():time.sleep(10)
exec((r/'followup.py').read_text())
