import pathlib,json,hashlib,subprocess,os,time
r=pathlib.Path('/home/ubuntu/owner3m');assert time.time()+390<1792281037
s=json.loads((r/'cache1024/owner-4-spec.json').read_text());s['rows']=200000;s['balance_every']=200000;s['split_working_bytes']=128*1024**3;s['output']=str(r/'four-reservation-check')
h=hashlib.sha256()
with open(s['vectors']['path'],'rb') as f:
 remain=200000*1024*4
 while remain:
  chunk=f.read(min(remain,1048576));assert chunk;h.update(chunk);remain-=len(chunk)
s['prefix_sha256']=h.hexdigest();p=r/'four-reservation-check-spec.json';p.write_text(json.dumps(s,indent=2)+'\n')
bins=[p for p in pathlib.Path('/home/ubuntu/reference3m/source/target/release/deps').glob('hierarchical_spann_shard-*') if p.is_file() and os.access(p,os.X_OK)];assert len(bins)==1
started=time.monotonic()
with (r/'four-reservation-check.log').open('x') as log:
 rc=subprocess.run(['/usr/bin/time','-v','-o',str(r/'four-reservation-check.time'),'timeout','--kill-after=15','300',str(bins[0]),'reference-owner-diagnostic','--spec',str(p)],stdout=log,stderr=subprocess.STDOUT).returncode
(r/'four-reservation-check-exit.json').write_text(json.dumps(dict(exit_code=rc,elapsed_seconds=time.monotonic()-started,binary_sha256=hashlib.sha256(bins[0].read_bytes()).hexdigest()))+'\n');raise SystemExit(rc)
