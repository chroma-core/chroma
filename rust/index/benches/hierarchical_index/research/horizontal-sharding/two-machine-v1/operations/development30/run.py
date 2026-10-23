"""Restore, run and collect one immutable final-million research arm."""
import json,subprocess,shlex,sys,time,concurrent.futures
from pathlib import Path
P=Path(__file__).resolve().parent
SSH=['-i','/Users/chroma/Documents/lyon.pem','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile=/private/tmp/spann-epoch29c-known-hosts']
H=json.loads((P/'hosts.json').read_text());B='/home/ubuntu/spann-two-machine-v1/development30';worker='i-011ff180f03584f40';peer='i-00268d1e1dfea0357'
def ssh(i,cmd,timeout=60):
 r=subprocess.run(['ssh',*SSH,'ubuntu@'+H[i]['PublicIpAddress'],cmd],capture_output=True,text=True,timeout=timeout)
 if r.returncode:raise RuntimeError(r.stderr[-2000:]+r.stdout[-2000:])
 return r.stdout
def put(i,path,dest):subprocess.run(['scp',*SSH,str(path),'ubuntu@'+H[i]['PublicIpAddress']+':'+dest],check=True,timeout=60)
def main():
 name=sys.argv[1];kind=sys.argv[2] if len(sys.argv)>2 else 'baseline';detailed=int('detailed' in sys.argv);profile='profile' in sys.argv
 assert name.replace('-','').isalnum() and kind in ['baseline','baseline2','baseline3','baseline4','candidate','transport1']
 local=P/name;local.mkdir(exist_ok=False);deadline=int(time.time()+1100)
 guard=json.loads((P.parent/'window-epoch29c/start-intent.json').read_text())
 assert deadline+15<int(guard['deadline_unix']), 'Renew development lease before running this arm'
 sha=json.loads((P/(kind+'-binary.json')).read_text())['sha256'];specs={};supervisors={}
 for iid,owner,mode in [(peer,2,'serve'),(worker,1,'run')]:
  other=worker if iid==peer else peer
  argv=[B+'/'+kind+'/two_machine_spann','--mode',mode,'--owner',str(owner),'--run','303030','--input-descriptor','/home/ubuntu/spann-two-machine-v1/epoch2/input/3m.json','--output',B+'/'+name+'-owner'+str(owner),'--data-bind',H[iid]['PrivateIpAddress']+':9100','--control-bind',H[iid]['PrivateIpAddress']+':9101','--peer-data',H[other]['PrivateIpAddress']+':9100','--peer-control',H[other]['PrivateIpAddress']+':9101','--token-file','/home/ubuntu/spann-two-machine-v1/epoch1/run-auth.token','--shutdown-at-unix',str(deadline),'--workers','32','--rows','3000000','--initial-batch','100000','--transition','1000000','--later-batch','1000000','--owner-budget','2160000']
  argv+=['--placement-start','2000000']
  argv+=['--reader-binary','/home/ubuntu/spann-two-machine-v1/epoch29c/build/two_machine_reader']
  if name.startswith('prepare'):argv+=['--checkpoint-at','2000000','--checkpoint-directory',B+'/checkpoint-2m']
  else:
   argv+=['--restore-checkpoint',B+'/checkpoint-2m','--construction-only']
   if 'replay' in name:argv+=['--replay-queries','256']
  spec={'instance_id':iid,'deadline':deadline,'binary_sha256':sha,'argv':argv,'evidence':B+'/'+name+'-process'+str(owner),'detailed':detailed,'profile':profile}
  specs[iid]=spec;(local/(mode+'.json')).write_text(json.dumps(spec,indent=2));put(iid,local/(mode+'.json'),B+'/'+name+'-'+mode+'.json')
 def launch(iid,mode):
  cmd='nohup python3 '+B+'/remote-run.py '+B+'/'+name+'-'+mode+'.json > '+B+'/'+name+'-'+mode+'.supervisor.log 2>&1 < /dev/null & echo $!'
  supervisors[iid]=int(ssh(iid,cmd).strip());(local/'supervisors.json').write_text(json.dumps(supervisors))
 try:
  launch(peer,'serve');ready=time.time()+150
  while time.time()<ready:
   path=B+'/'+name+'-owner2/events.jsonl'
   code="from pathlib import Path; p=Path("+repr(path)+"); print('server_ready' in p.read_text() if p.exists() else False)"
   if ssh(peer,'python3 -c '+shlex.quote(code)).strip()=='True':break
   time.sleep(2)
  else:raise RuntimeError('peer readiness timeout')
  launch(worker,'run');print('RUNNING',name,flush=True)
  while time.time()<deadline+5:
   code="from pathlib import Path; p=Path("+repr(specs[worker]['evidence']+'/exit.json')+"); print(p.read_text() if p.exists() else '')"
   state=ssh(worker,'python3 -c '+shlex.quote(code)).strip()
   if state:
    (local/'exit.json').write_text(state);print('EXIT',state,flush=True);break
   time.sleep(3)
  else:raise RuntimeError('coordinator deadline elapsed')
 finally:
  for iid,pid in supervisors.items():
   code="import os,signal,pathlib; p=pathlib.Path('/proc/"+str(pid)+"/cmdline'); cmd=p.read_bytes() if p.exists() else b''; expected="+repr((B+'/remote-run.py').encode())+";\nif expected in cmd: os.kill("+str(pid)+",signal.SIGTERM)"
   ssh(iid,'python3 -c '+shlex.quote(code))
  time.sleep(2)
  def collect(pair):
   iid,spec=pair;output=spec['argv'][spec['argv'].index('--output')+1]
   code="import pathlib,json; roots=[pathlib.Path("+repr(output)+"),pathlib.Path("+repr(spec['evidence'])+")]; print(json.dumps({str(p):p.read_text() for root in roots if root.exists() for p in root.iterdir() if p.is_file() and p.name!='perf.data' and p.stat().st_size<25000000}))"
   (local/('owner'+('1' if iid==worker else '2')+'-receipts.json')).write_text(ssh(iid,'python3 -c '+shlex.quote(code)))
  with concurrent.futures.ThreadPoolExecutor(2) as pool:list(pool.map(collect,specs.items()))
 print('COLLECTED',name,flush=True)
if __name__=='__main__':main()
