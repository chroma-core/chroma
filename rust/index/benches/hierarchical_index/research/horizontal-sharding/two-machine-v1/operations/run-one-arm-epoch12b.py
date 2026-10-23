#!/usr/bin/env python3
"""Run exactly one frozen arm; the root operator invokes each arm explicitly."""
import argparse,hashlib,importlib.util,json,shlex,subprocess,time
from pathlib import Path
P=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('make_spec',P/'make-owner-local-spec-epoch12b.py');g=importlib.util.module_from_spec(s);s.loader.exec_module(g)
BASE='/home/ubuntu/spann-two-machine-v1/epoch12b'
SSH=['-i','/Users/chroma/Documents/lyon.pem','-o','BatchMode=yes','-o','ConnectTimeout=15','-o','StrictHostKeyChecking=yes']
def ssh(ip,cmd,timeout=30):
 r=subprocess.run(['ssh',*SSH,'ubuntu@'+ip,cmd],capture_output=True,text=True,timeout=timeout)
 if r.returncode:raise RuntimeError(r.stderr[-4000:]+r.stdout[-4000:])
 return r.stdout

def execution_bounds(arm):
 # One fixed arm per invocation; durations never extend an existing process.
 return (590, 660)

def privileged_collector_command(remote,digest,evidence,collector,collector_hash):
 # The root interpreter executes only source bytes whose exact digest it checks
 # itself. Isolated mode ignores user Python paths and startup configuration.
 bootstrap=("import hashlib,pathlib,sys; "
  "p=pathlib.Path(sys.argv[1]); expected=sys.argv[2]; "
  "assert str(p)=='/home/ubuntu/spann-two-machine-v1/epoch12b/remote-profile-epoch12b.py'; "
  "source=p.read_bytes(); "
  "assert hashlib.sha256(source).hexdigest()==expected, 'collector source hash mismatch'; "
  "sys.argv=[str(p),*sys.argv[3:]]; "
  "exec(compile(source,str(p),'exec'),{'__name__':'__main__','__file__':str(p)})")
 return ['sudo','-n','/usr/bin/timeout','--signal=TERM','--kill-after=2','238',
  '/usr/bin/python3','-I','-c',bootstrap,BASE+'/remote-profile-epoch12b.py',collector_hash,
  '--spec',remote,'--spec-sha256',digest,'--evidence',evidence,'--output',collector]

def collect_final_profile_files(ip,collector):
 script=("import json,pathlib; root=pathlib.Path("+repr(collector)+"); out={}; "
  "names=['result.json','ready.json','perf-report.txt','perf.stderr','report.stderr']; "
  "[(out.update({str(root/n):(root/n).read_text()})) for n in names if (root/n).is_file() and (root/n).stat().st_size<4000000]; "
  "print(json.dumps(out))")
 files=json.loads(ssh(ip,'python3 -c '+shlex.quote(script)))
 if collector+'/perf-report.txt' not in files:raise RuntimeError('Completed collector has no readable final report')
 return files

def main():
 ap=argparse.ArgumentParser();ap.add_argument('arm',choices=('100k-reference','100k-distributed'));a=ap.parse_args();w=P/'window-epoch12';intent=json.loads((w/'start-intent.json').read_text());hosts=json.loads((w/'running-readback.json').read_text());end=int(intent['deadline_unix']);now=time.time()
 job_duration,required_remaining=execution_bounds(a.arm)
 if not intent['window_id'].startswith('epoch12-') or (w/'closed.json').exists() or now+required_remaining>end:raise RuntimeError('Insufficient time in the fresh fixed open epoch12 window')
 attempt_name=a.arm;local=w/(attempt_name+"-ackfix");local.mkdir(exist_ok=False);peer=None;specs={};collector=None;collector_ready=False;jobend=int(now+job_duration);completed=False;cleanup_errors=[]
 for mode in (['run'] if a.arm=='100k-reference' else ['serve','run']):
  size='100k'
  ns=argparse.Namespace(window_id=intent['window_id'],window_deadline=end,job_deadline=jobend,arm=a.arm,mode=mode,worker_ip=hosts[g.WORKER]['PrivateIpAddress'],peer_ip=hosts[g.PEER]['PrivateIpAddress'],base=BASE,corpus_descriptor='/home/ubuntu/spann-two-machine-v1/epoch2/input/'+size+'.json',input_file='/home/ubuntu/spann-two-machine-v1/epoch2/input/fineweb'+size+'.f32')
  spec=g.generate(ns,now)
  if specs and any(prior[0]['provenance']['artifact_hash_file_sha256']!=spec['provenance']['artifact_hash_file_sha256'] for prior in specs.values()):raise RuntimeError('Artifact hash file changed between owner specifications')
  body=(json.dumps(spec,indent=2,sort_keys=True)+'\n').encode();digest=hashlib.sha256(body).hexdigest();path=local/(mode+'.json');path.write_bytes(body);ip=hosts[spec['instance_id']]['PublicIpAddress'];remote=BASE+'/'+attempt_name+'-'+mode+'.json';evidence=BASE+'/'+spec['job_id']+'-process';specs[mode]=(spec,ip,remote,evidence,digest)
  subprocess.run(['scp',*SSH,str(path),'ubuntu@'+ip+':'+remote],check=True,timeout=30)
 def command(mode):
  _,_,remote,evidence,digest=specs[mode]
  return shlex.join(['python3',BASE+'/remote-job-epoch12b.py','run','--spec',remote,'--spec-sha256',digest,'--evidence',evidence])
 try:
  spec,ip,remote,evidence,digest=specs['run'];print('RUNNING '+a.arm,flush=True)
  collector=BASE+'/'+a.arm+'-collector'
  collector_hash=spec['provenance']['collector_script_sha256']
  profile_command=shlex.join(privileged_collector_command(remote,digest,evidence,collector,collector_hash))
  launch='nohup '+profile_command+' > '+shlex.quote(collector+'.supervisor.log')+' 2>&1 < /dev/null & echo $!'
  (local/'collector-launch.txt').write_text(ssh(ip,launch))
  until=time.time()+15
  while True:
   probe="import pathlib,json; p=pathlib.Path("+repr(collector)+"); print(json.dumps({'ready':json.loads((p/'ready.json').read_text()) if (p/'ready.json').exists() else None,'result':json.loads((p/'result.json').read_text()) if (p/'result.json').exists() else None}))"
   state=json.loads(ssh(ip,'python3 -c '+shlex.quote(probe)))
   if state['result'] is not None:raise RuntimeError('Collector preflight failed: '+str(state['result'].get('error')))
   if state['ready'] is not None:
    if state['ready']['spec_sha256']!=digest:raise RuntimeError('Collector ready receipt belongs to another spec')
    collector_ready=True
    break
   if time.time()>until:raise RuntimeError('Collector preflight did not become ready')
   time.sleep(.1)
  if 'serve' in specs:
   spec,ip,_,evidence,digest=specs['serve'];peer=(ip,evidence,digest)
   launch='nohup '+command('serve')+' > '+shlex.quote(evidence+'.supervisor.log')+' 2>&1 < /dev/null & echo $!'
   (local/'peer-launch.txt').write_text(ssh(ip,launch))
   # Wait for its listener after retained100K input preparation.
   deadline=time.time()+90
   while True:
    status=ssh(ip,"python3 -c "+shlex.quote("import socket; s=socket.socket(); s.settimeout(1); print(s.connect_ex(('"+hosts[g.PEER]['PrivateIpAddress']+"',9100))); s.close()"))
    if status.strip()=='0':break
    if time.time()>deadline:raise RuntimeError('Peer did not bind within90seconds')
    time.sleep(1)
  spec,ip,remote,evidence,digest=specs['run']
  state=json.loads(ssh(ip,'python3 -c '+shlex.quote(probe)))
  if state['result'] is not None:raise RuntimeError('Collector ended before workload admission')
  r=subprocess.run(['ssh',*SSH,'ubuntu@'+ip,command('run')],capture_output=True,text=True,timeout=required_remaining)
  (local/'supervisor.stdout').write_text(r.stdout);(local/'supervisor.stderr').write_text(r.stderr)
  if r.returncode:raise RuntimeError('Workload supervisor failed: '+str(r.returncode))
  completed=True
 finally:
  if not completed and 'run' in specs:
   _,ip,_,evidence,digest=specs['run']
   try:(local/'run-stop.json').write_text(ssh(ip,shlex.join(['python3',BASE+'/remote-job-epoch12b.py','stop','--spec-sha256',digest,'--evidence',evidence]),30))
   except Exception as exc:
    (local/'run-stop-error.txt').write_text(str(exc));cleanup_errors.append('run stop unconfirmed')
  if peer:
   ip,evidence,digest=peer
   try:
    stopped=ssh(ip,shlex.join(['python3',BASE+'/remote-job-epoch12b.py','stop','--spec-sha256',digest,'--evidence',evidence]),30)
    (local/'peer-stop.json').write_text(stopped)
    if json.loads(stopped)['exit_receipt']['cleanup_confirmed'] is not True:raise RuntimeError('peer cleanup not confirmed')
   except Exception as exc:
    (local/'peer-stop-error.txt').write_text(str(exc));cleanup_errors.append('peer stop unconfirmed')
  for mode,(spec,ip,_,evidence,digest) in specs.items():
   output=spec['args'].get('--output',spec['args'].get('--output-dir'))
   # Only small JSON/log receipts; corpus, posting and saved-index bytes stay remote.
   roots=[evidence,output]+([collector] if mode=='run' and collector else [])
   script="import pathlib,json; roots=[pathlib.Path(p) for p in "+repr(roots)+"]; out={};\nfor root in roots:\n if root.exists():\n  for p in root.iterdir():\n   if p.is_file() and (p.suffix in ('.json','.jsonl') or p.name in ('stderr','stdout','perf.stderr','report.stderr','perf-report.txt')) and p.stat().st_size<4000000: out[str(p)]=p.read_text()\nprint(json.dumps(out))"
   try:(local/(mode+'-receipts.json')).write_text(ssh(ip,'python3 -c '+shlex.quote(script),30))
   except Exception as exc:
    (local/(mode+'-collect-error.txt')).write_text(str(exc));cleanup_errors.append(mode+' collection failed')
 if collector and collector_ready:
  _,ip,_,_,_=specs['run'];until=time.time()+245
  while True:
   text=ssh(ip,'python3 -c '+shlex.quote("import pathlib; p=pathlib.Path("+repr(collector+'/result.json')+"); print(p.read_text() if p.exists() else '{}')"))
   result=json.loads(text)
   if result:
    (local/'profile-result.json').write_text(json.dumps(result,indent=2)+'\n')
    if result.get('status')!='completed':cleanup_errors.append('profile failed; preserve receipts and do not retry')
    else:(local/'profile-final-files.json').write_text(json.dumps(collect_final_profile_files(ip,collector),indent=2)+'\n')
    break
   if time.time()>until:cleanup_errors.append('collector outcome unconfirmed; absolute collector and host guards remain required');break
   time.sleep(.1)
 if cleanup_errors:raise RuntimeError('; '.join(cleanup_errors))
 print('FINISHED '+a.arm,flush=True)
if __name__=='__main__':main()
