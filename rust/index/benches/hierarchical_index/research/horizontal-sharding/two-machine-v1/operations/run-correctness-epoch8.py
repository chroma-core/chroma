#!/usr/bin/env python3
"""Run the existing reviewed physical fixture once with repaired Linux artifacts."""
import hashlib,importlib.util,json,shlex,subprocess,time
from pathlib import Path
P=Path(__file__).resolve().parent
ss=importlib.util.spec_from_file_location('job',P/'remote-job-epoch8.py');j=importlib.util.module_from_spec(ss);ss.loader.exec_module(j)
W=P/'window-epoch8';BASE='/home/ubuntu/spann-two-machine-v1/epoch8'
SSH=['-i','/Users/chroma/Documents/lyon.pem','-o','BatchMode=yes','-o','ConnectTimeout=15','-o','StrictHostKeyChecking=yes']
def ssh(ip,cmd,timeout=30):
 r=subprocess.run(['ssh',*SSH,'ubuntu@'+ip,cmd],capture_output=True,text=True,timeout=timeout)
 if r.returncode:raise RuntimeError(r.stderr[-3000:]+r.stdout[-3000:])
 return r.stdout

def main():
 hosts=json.loads((W/'running-readback.json').read_text());intent=json.loads((W/'start-intent.json').read_text());end=int(intent['deadline_unix']);now=time.time();assert intent['window_id'].startswith('epoch8-') and not (W/'closed.json').exists() and now+660<end
 local=W/'physical-correctness';local.mkdir();build=json.loads((P/'runtime-binaries-epoch7.json').read_text());deadline=int(now+590);specs={};completed=False;cleanup_errors=[]
 for owner in [2,1]:
  spec=json.loads((P/f'window-epoch1/job-owner{owner}.json').read_text());spec['window_id']=intent['window_id'];spec['window_deadline_unix']=end;spec['deadline_unix']=deadline
  for k in ['binary','reader']:spec[k]=spec[k].replace('/epoch1/','/epoch7/');spec[k+'_sha256']=build[Path(spec[k]).name]
  for k,v in spec['args'].items():
   if isinstance(v,str):spec['args'][k]=v.replace('/epoch1/','/epoch7/')
  spec['args']['--token-file']='/home/ubuntu/spann-two-machine-v1/epoch1/run-auth.token'
  spec['args']['--input-file']='/home/ubuntu/spann-two-machine-v1/epoch1/fixture.f32'
  spec['args']['--output']=BASE+f'/correctness-owner{owner}';spec['args']['--run']=int.from_bytes(hashlib.sha256((intent['window_id']+':correctness').encode()).digest()[:16],'big');spec['args']['--shutdown-at-unix']=deadline
  other='i-00268d1e1dfea0357' if owner==1 else 'i-011ff180f03584f40'
  for k,port in [('--peer-data',9100),('--peer-control',9101)]:spec['args'][k]=hosts[other]['PrivateIpAddress']+':'+str(port)
  j.validate(spec,now);data=(json.dumps(spec,indent=2)+'\n').encode();sha=hashlib.sha256(data).hexdigest();path=local/f'owner{owner}.json';path.write_bytes(data);ip=hosts[spec['instance_id']]['PublicIpAddress'];remote=BASE+f'/job-owner{owner}.json';evidence=BASE+f'/correctness-owner{owner}-process';specs[owner]=(spec,ip,sha,evidence)
  subprocess.run(['scp',*SSH,str(path),'ubuntu@'+ip+':'+remote],check=True,timeout=30)
 def command(owner):
  spec,ip,sha,evidence=specs[owner];return shlex.join(['python3',BASE+'/remote-job-epoch8.py','run','--spec',BASE+f'/job-owner{owner}.json','--spec-sha256',sha,'--evidence',evidence])
 try:
  spec,ip,sha,ev=specs[2];(local/'peer-launch.txt').write_text(ssh(ip,'nohup '+command(2)+' > '+ev+'.log 2>&1 < /dev/null & echo $!'))
  until=time.time()+30
  while True:
   status=ssh(ip,'python3 -c '+shlex.quote("import socket; s=socket.socket(); s.settimeout(1); print(s.connect_ex(('127.0.0.1',9100))); s.close()"))
   if status.strip()=='0':break
   if time.time()>until:raise RuntimeError('Correctness peer did not listen')
   time.sleep(1)
  (local/'supervisor.stdout').write_text(ssh(specs[1][1],command(1),660));completed=True
 finally:
  for owner in ([2] if completed else [1,2]):
   _,ip,sha,ev=specs[owner]
   try:
    result=ssh(ip,shlex.join(['python3',BASE+'/remote-job-epoch8.py','stop','--spec-sha256',sha,'--evidence',ev]));(local/f'owner{owner}-stop.json').write_text(result)
    assert json.loads(result)['exit_receipt']['cleanup_confirmed']
   except Exception as error:
    (local/f'owner{owner}-stop-error.txt').write_text(str(error));cleanup_errors.append(f'owner{owner} cleanup unconfirmed')
  for owner,(spec,ip,sha,ev) in specs.items():
   script="import pathlib,json; roots=[pathlib.Path("+repr(ev)+"),pathlib.Path("+repr(spec['args']['--output'])+")]; out={};\nfor root in roots:\n if root.exists():\n  for p in root.iterdir():\n   if p.is_file() and (p.suffix in ('.json','.jsonl') or p.name in ('stderr','stdout')) and p.stat().st_size<4000000: out[str(p)]=p.read_text()\nprint(json.dumps(out))"
   try:(local/f'owner{owner}-receipts.json').write_text(ssh(ip,'python3 -c '+shlex.quote(script)))
   except Exception as error:
    (local/f'owner{owner}-collect-error.txt').write_text(str(error));cleanup_errors.append(f'owner{owner} receipt collection failed')
  if cleanup_errors:raise RuntimeError('; '.join(cleanup_errors))
 print('Physical fixture complete; inspect invariants before performance.')
if __name__=='__main__':main()
