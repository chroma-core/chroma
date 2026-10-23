import pathlib,tarfile,json,hashlib,os,subprocess,time,fcntl
p=pathlib.Path('/home/ubuntu/spann-two-machine-v1/development30');lock=open('/home/ubuntu/spann-benchmark.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
tarfile.open(p/'source-delta.tar.gz').extractall(p/'source',filter='data')
m=json.loads((p/'source-manifest.json').read_text())
for name,h in m['source_files'].items():
 assert hashlib.sha256((p/'source'/name).read_bytes()).hexdigest()==h,name
for name in m['source_files']:
 (p/'source'/name).touch()
os.makedirs(p/'epoch31v3',exist_ok=False)
env=dict(os.environ,PATH='/home/ubuntu/.cargo/bin:'+os.environ['PATH'],RUSTUP_TOOLCHAIN='1.92.0',CARGO_TARGET_DIR=m['cargo_target_dir'],RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes -C debuginfo=1')
cmd=['cargo','rustc','-p','chroma-index','--release','--locked','--bench','two_machine_spann','--message-format=json','--','--cfg','distributed_owner']
started=time.time()
with open(p/'epoch31v3/build.stdout','w') as out,open(p/'epoch31v3/build.stderr','w') as err:r=subprocess.run(cmd,cwd=p/'source',env=env,stdout=out,stderr=err,timeout=900)
(p/'epoch31v3/build-status.json').write_text(json.dumps({'exit_code':r.returncode,'seconds':time.time()-started}))
if r.returncode==0:
 import shutil
 for line in (p/'epoch31v3/build.stdout').read_text().splitlines():
  try:x=json.loads(line)
  except:continue
  if x.get('reason')=='compiler-artifact' and x.get('executable') and x['target']['name']=='two_machine_spann':
   shutil.copy2(x['executable'],p/'epoch31v3/two_machine_spann')
   (p/'epoch31v3/binary.json').write_text(json.dumps({'sha256':hashlib.sha256((p/'epoch31v3/two_machine_spann').read_bytes()).hexdigest(),'artifact':x}))

