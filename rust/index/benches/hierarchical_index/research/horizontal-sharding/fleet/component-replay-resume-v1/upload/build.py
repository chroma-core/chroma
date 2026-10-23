"""Build one exact Linux epoch with a fixed timeout and disk floor."""
import hashlib,json,os,shutil,signal,subprocess,time
from pathlib import Path
ROOT=Path('/mnt/spann/component-replay-v1');SRC=ROOT/'source'
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
 return h.hexdigest()
pins=json.loads((ROOT/'source-manifest.json').read_text())['files']
def verify():
 for name,expected in pins.items():assert sha(SRC/name)==expected,name
verify()
deadline=json.loads((ROOT/'deadline.json').read_text())['deadline_unix']
subprocess.run(['systemctl','is-active','--quiet','spann-component-v1-stop.timer'],check=True)
env=dict(os.environ,PATH='/home/ubuntu/.cargo/bin:'+os.environ['PATH'],CARGO_TARGET_DIR=str(ROOT/'target'),CARGO_PROFILE_RELEASE_DEBUG='0',CARGO_INCREMENTAL='0',RAYON_NUM_THREADS='2',TOKIO_WORKER_THREADS='2')
commands=[['cargo','+1.92.0','build','--release','--locked','-j4','-p','chroma-index','--bench','hierarchical_spann_shard'],['cargo','+1.92.0','test','--release','--locked','-j4','-p','chroma-index','--test','hierarchical_spann_reader_integrity','selection::tests']]
results=[]
for index,cmd in enumerate(commands):
 start=time.time();stop=min(start+1800,deadline-300);assert stop>start+60
 with (ROOT/f'build-{index}.stdout').open('w') as out,(ROOT/f'build-{index}.stderr').open('w') as err:
  proc=subprocess.Popen(cmd,cwd=SRC,env=env,stdout=out,stderr=err,start_new_session=True)
  reason=None
  while proc.poll() is None:
   if time.time()>=stop:reason='fixed build deadline'
   if shutil.disk_usage(ROOT).free<10*1024**3:reason='10GiB free floor'
   if reason:
    os.killpg(proc.pid,signal.SIGTERM)
    try:proc.wait(timeout=10)
    except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
    break
   time.sleep(2)
 results.append(dict(command=cmd,returncode=proc.returncode,seconds=time.time()-start,stop_reason=reason))
 (ROOT/'build-result.json').write_text(json.dumps(results,indent=2)+'\n')
 verify()
 if proc.returncode!=0 or reason:raise SystemExit(1)
bins=[p for p in (ROOT/'target/release/deps').glob('hierarchical_spann_shard-*') if p.is_file() and os.access(p,os.X_OK) and not p.suffix]
assert len(bins)==1,[str(p) for p in bins]
shutil.copyfile(bins[0],ROOT/'native-reader');os.chmod(ROOT/'native-reader',0o755)
(ROOT/'binary.json').write_text(json.dumps(dict(sha256=sha(ROOT/'native-reader'),bytes=(ROOT/'native-reader').stat().st_size,source_pins=len(pins),toolchain=subprocess.check_output(['cargo','+1.92.0','--version'],env=env,text=True).strip(),profile='release, debug0, 4 build jobs',architecture=subprocess.check_output(['uname','-m'],text=True).strip()),indent=2)+'\n')
