"""Build one frozen research executable only after the benchmark lock is free."""
from run import P,H,B,ssh,put
import json,sys,shlex,time
kind='epoch33v1';worker='i-011ff180f03584f40';peer='i-00268d1e1dfea0357'
archive=P.parent.parent/'packages/source-epoch33v1.tar.gz'
for local,dest in [(archive,B+'/source-epoch33v1.tar.gz'),(P/'epoch33v1-source-manifest.json',B+'/epoch33v1-source-manifest.json')]:put(worker,local,dest)
script='''import pathlib,tarfile,json,hashlib,os,subprocess,time,fcntl,shutil
p=pathlib.Path('/home/ubuntu/spann-two-machine-v1/development30');lock=open('/home/ubuntu/spann-benchmark.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
source=p/'source33';source.mkdir(exist_ok=False);tarfile.open(p/'source-epoch33v1.tar.gz').extractall(source,filter='data')
m=json.loads((p/'epoch33v1-source-manifest.json').read_text())
for name,h in m['source_files'].items():
 assert hashlib.sha256((source/name).read_bytes()).hexdigest()==h,name
 (source/name).touch()
out=p/'epoch33v1';out.mkdir(exist_ok=False)
env=dict(os.environ,PATH='/home/ubuntu/.cargo/bin:'+os.environ['PATH'],RUSTUP_TOOLCHAIN='1.92.0',CARGO_TARGET_DIR=m['cargo_target_dir'],RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes -C debuginfo=1')
cmd=['cargo','rustc','-p','chroma-index','--release','--locked','--bench','two_machine_spann','--message-format=json','--','--cfg','distributed_owner'];started=time.time()
with (out/'build.stdout').open('w') as stdout,(out/'build.stderr').open('w') as stderr:r=subprocess.run(cmd,cwd=source,env=env,stdout=stdout,stderr=stderr,timeout=900)
(out/'build-status.json').write_text(json.dumps({'exit_code':r.returncode,'seconds':time.time()-started}))
if r.returncode==0:
 for line in (out/'build.stdout').read_text().splitlines():
  try:x=json.loads(line)
  except:continue
  if x.get('reason')=='compiler-artifact' and x.get('executable') and x['target']['name']=='two_machine_spann':
   shutil.copy2(x['executable'],out/'two_machine_spann');(out/'binary.json').write_text(json.dumps({'sha256':hashlib.sha256((out/'two_machine_spann').read_bytes()).hexdigest(),'artifact':x}))
'''
(P/'remote-build.py').write_text(script);put(worker,P/'remote-build.py',B+'/build33.py')
print(ssh(worker,'nohup python3 '+B+'/build33.py > '+B+'/build33-supervisor.log 2>&1 < /dev/null & echo $!'),flush=True)
until=time.time()+960
while time.time()<until:
 code="from pathlib import Path; p=Path('"+B+"/epoch33v1/build-status.json'); print(p.read_text() if p.exists() else '')"
 status=ssh(worker,'python3 -c '+shlex.quote(code)).strip()
 if status:break
 time.sleep(5)
else:raise RuntimeError('build deadline')
assert json.loads(status)['exit_code']==0,status
for name in ['build-status.json','build.stderr','build.stdout','binary.json']:
 (P/('epoch33v1-'+name)).write_text(ssh(worker,shlex.join(['cat',B+'/'+kind+'/'+name])))
receipt=json.loads((P/'epoch33v1-binary.json').read_text());h=receipt['sha256']
for i in H:
 put(i,P/'binary-cache.py',B+'/binary-cache33.py');put(i,P/'remote-run33.py',B+'/remote-run33.py')
for i,mode in [(worker,'upload'),(peer,'download')]:
 result=ssh(i,shlex.join(['python3',B+'/binary-cache33.py',mode,B+'/'+kind+'/two_machine_spann',h,'--deadline',str(int(time.time()+180))]),timeout=190);(P/(i+'-binary-transfer.json')).write_text(result)
print('BUILT_AND_DEPLOYED',h,flush=True)
