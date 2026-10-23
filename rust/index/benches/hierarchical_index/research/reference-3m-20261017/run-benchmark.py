import datetime, hashlib, json, os, pathlib, shutil, subprocess, time
r=pathlib.Path('/home/ubuntu/reference3m');ev=r/'evidence'
assert time.time()+1500 < 1792279186
assert subprocess.check_output(['systemctl','is-active','reference3m-20261017-stop.timer'],text=True).strip()=='active'
bins=[p for p in (r/'source/target/release/deps').glob('hierarchical_spann_profile_quantized-*') if p.is_file() and os.access(p,os.X_OK)]
assert len(bins)==1,bins
binary=bins[0]
gt=r/'ground-truth/wikipedia_1m_3m_ground_truth.parquet'
m=json.loads((r/'ground-truth/wikipedia_1m_3m_ground_truth.json').read_text())
assert m['boundaries']==[1000000,2000000,3000000]
assert hashlib.sha256(gt.read_bytes()).hexdigest()==m['parquet_sha256']
gt_dest=pathlib.Path('/home/ubuntu/.cache/wikipedia_en/ground_truth.parquet')
assert not gt_dest.exists()
gt_dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(gt,gt_dest)
args=['--dataset','wikipedia-en','--metric','l2','--checkpoint','3','--checkpoint-size','1000000','--branching-factor','100','--split-threshold','2048','--merge-threshold','512','--threads','32','--balance-threads','32','--write-navigation','fp','--fp-npa','--write-beam-tau','0.5','--write-beam-min','10','--write-beam-max','16','--write-level-taus','_,_,0.30','--max-replicas','1','--write-rng-epsilon','0','--write-rng-factor','0','--read-beam-min','10','--read-beam-max','256','--read-level-taus','_,_,_','--read-level-min-pcts','50,5,0','--recall-rerank-vectors','1,8','--recall-tau-values','0.7,1,2','--num-queries','100','--max-cache-bytes','1073741824','--compute-gt-clusters','false','--save','--save-dir',str(r/'index')]
assert not (r/'index').exists()
env=dict(os.environ,HF_HOME=str(r/'hf'),HF_HUB_OFFLINE='1')
meta=dict(command=[str(binary)]+args,binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),source_commit='bec6e7213b37ca97066545535bc82d4252f9c0dc',storage='existing root gp3 EBS, unlike historical local NVMe',start_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),gt_sha256=m['parquet_sha256'])
(ev/'benchmark-binding.json').write_text(json.dumps(meta,indent=2)+'\n')
started=time.monotonic()
with (ev/'benchmark.log').open('x') as log:
    p=subprocess.Popen(['/usr/bin/time','-v','-o',str(ev/'benchmark.time'),'timeout','--signal=TERM','--kill-after=15','1200',str(binary)]+args,stdout=log,stderr=subprocess.STDOUT,env=env)
    code=p.wait()
meta.update(exit_code=code,elapsed_seconds=time.monotonic()-started,end_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
(ev/'benchmark-result.json').write_text(json.dumps(meta,indent=2)+'\n')
raise SystemExit(code)
