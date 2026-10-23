"""Bound one development process to its recorded task instance and immutable binary."""
import sys,json,time,os,signal,subprocess,hashlib,fcntl,importlib.util
from pathlib import Path
base=Path('/home/ubuntu/spann-two-machine-v1/development30')
s=importlib.util.spec_from_file_location('guard','/home/ubuntu/spann-two-machine-v1/epoch29c/remote-job.py');guard=importlib.util.module_from_spec(s);s.loader.exec_module(guard)
spec=json.loads(Path(sys.argv[1]).read_text());guard.imds(spec['instance_id'])
lock=open(guard.LOCK,'a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
assert time.time()<spec['deadline']<=time.time()+1200
binary=Path(spec['argv'][0]);assert binary.is_relative_to(base)
assert hashlib.sha256(binary.read_bytes()).hexdigest()==spec['binary_sha256']
out=Path(spec['evidence']);assert out.is_relative_to(base);out.mkdir(exist_ok=False)
(out/'spec.json').write_text(json.dumps(spec,indent=2))
env=dict(os.environ,SPANN_DETAILED_DIAGNOSTICS=str(spec.get('detailed',0)))
allowed={'SPANN_ACCEPT_MODE','SPANN_INSERTION_NAVIGATION_WINDOW','SPANN_SAVE_WORKERS','SPANN_INSERTION_WORKERS','SPANN_SERIALIZE_BOUNDARY_REFRESH'}
assert set(spec.get('env',{}))<=allowed
env.update(spec.get('env',{}))
with open(out/'stdout','w') as stdout,open(out/'stderr','w') as stderr:
 child=subprocess.Popen(spec['argv'],env=env,stdout=stdout,stderr=stderr,start_new_session=True)
 (out/'pid.json').write_text(json.dumps({'pid':child.pid,'argv':spec['argv']}))
 def stop(*_):
  if child.poll() is None:os.killpg(child.pid,signal.SIGTERM)
 signal.signal(signal.SIGTERM,stop)
 started=time.time();profile=None
 try:
  if spec.get('profile'):
   profile=subprocess.Popen(['sudo','perf','record','-F','99','-g','--call-graph','fp','-p',str(child.pid),'-o',str(out/'perf.data'),'--','sleep',str(max(1,int(spec['deadline']-time.time())))],stdout=open(out/'perf.stdout','w'),stderr=open(out/'perf.stderr','w'),start_new_session=True)
  result=child.wait(timeout=max(1,spec['deadline']-time.time()))
 except subprocess.TimeoutExpired:
  stop()
  try:result=child.wait(timeout=8)
  except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);result=child.wait()
 finally:
  if profile is not None:
   subprocess.run(['sudo','kill','-INT','--','-'+str(profile.pid)],check=False)
   try:profile.wait(timeout=10)
   except subprocess.TimeoutExpired:subprocess.run(['sudo','kill','-KILL','--','-'+str(profile.pid)])
  (out/'exit.json').write_text(json.dumps({'exit_code':child.poll(),'seconds':time.time()-started,'completed_unix':time.time()}))
