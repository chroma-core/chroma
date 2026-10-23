"""Restore, run and collect one immutable final-million research arm."""
import json,subprocess,shlex,sys,time,concurrent.futures
from pathlib import Path
P=Path(__file__).resolve().parent
SSH=['-C','-i','/Users/chroma/Documents/lyon.pem','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile=/private/tmp/spann-epoch29c-known-hosts']
H=json.loads((P/'hosts.json').read_text());B='/home/ubuntu/spann-two-machine-v1/development30';worker='i-011ff180f03584f40';peer='i-00268d1e1dfea0357'
def ssh(i,cmd,timeout=180):
 r=subprocess.run(['ssh',*SSH,'ubuntu@'+H[i]['PublicIpAddress'],cmd],capture_output=True,text=True,timeout=timeout)
 if r.returncode:raise RuntimeError(r.stderr[-2000:]+r.stdout[-2000:])
 return r.stdout
def put(i,path,dest):subprocess.run(['scp',*SSH,str(path),'ubuntu@'+H[i]['PublicIpAddress']+':'+dest],check=True,timeout=60)
def main():
 name=sys.argv[1];kind=sys.argv[2] if len(sys.argv)>2 else 'baseline';detailed=int('detailed' in sys.argv);profile='profile' in sys.argv
 assert name.replace('-','').isalnum() and kind in ['baseline','baseline2','baseline3','baseline4','candidate','transport1','epoch31v1','epoch31v2','epoch31v3','epoch31v4','epoch32v1','epoch32v2']
 reader='/home/ubuntu/spann-two-machine-v1/epoch29c/build/two_machine_reader'
 if 'save' in sys.argv and name!='save-serial':
  until=time.time()+600
  while not (P/'reader-selection.json').exists():
   assert time.time()<until, 'Verified reader artifact is not ready'
   time.sleep(2)
  selection=json.loads((P/'reader-selection.json').read_text());reader=selection['path']
  code="import hashlib,pathlib; print(hashlib.sha256(pathlib.Path("+repr(reader)+").read_bytes()).hexdigest())"
  assert ssh(worker,'python3 -c '+shlex.quote(code)).strip()==selection['sha256']
 local=P/name;local.mkdir(exist_ok=False);deadline=int(time.time()+1100)
 guard=json.loads((P.parent/'window-epoch29c/start-intent.json').read_text())
 assert deadline+15<int(guard['deadline_unix']), 'Renew development lease before running this arm'
 sha=json.loads((P/(kind+'-binary.json')).read_text())['sha256'];specs={};supervisors={}
 for iid,owner,mode in [(peer,2,'serve'),(worker,1,'run')]:
  other=worker if iid==peer else peer
  if 'save' in sys.argv:
   code="import json,pathlib; p=pathlib.Path('/home/ubuntu/spann-two-machine-v1/epoch29c/storage-config.json'); c=json.loads(p.read_text()); c['prefix']='sha256/epoch31-"+name+"'; pathlib.Path('"+B+'/'+name+"-storage.json').write_text(json.dumps(c))"
   ssh(iid,'python3 -c '+shlex.quote(code))
  argv=[B+'/'+kind+'/two_machine_spann','--mode',mode,'--owner',str(owner),'--run','303030','--input-descriptor','/home/ubuntu/spann-two-machine-v1/epoch2/input/3m.json','--output',B+'/'+name+'-owner'+str(owner),'--data-bind',H[iid]['PrivateIpAddress']+':9100','--control-bind',H[iid]['PrivateIpAddress']+':9101','--peer-data',H[other]['PrivateIpAddress']+':9100','--peer-control',H[other]['PrivateIpAddress']+':9101','--token-file','/home/ubuntu/spann-two-machine-v1/epoch1/run-auth.token','--shutdown-at-unix',str(deadline),'--workers','32','--rows','3000000','--initial-batch','100000','--transition','1000000','--later-batch','1000000','--owner-budget','2160000']
  balance_workers=int(__import__('os').environ.get('SPANN_BALANCE_WORKERS','32'));assert 1<=balance_workers<=32
  argv[argv.index('--workers')+1]=str(balance_workers)
  argv+=['--placement-start','2000000']
  argv+=['--reader-binary',reader]
  if name.startswith('prepare'):argv+=['--checkpoint-at','2000000','--checkpoint-directory',B+'/checkpoint-2m']
  else:
   argv+=['--restore-checkpoint',B+'/checkpoint-2m']
   if 'save' in sys.argv: argv+=['--storage-config',B+'/'+name+'-storage.json','--save-unchanged-generation']
   else: argv+=['--construction-only']
   if 'replay' in name:argv+=['--replay-queries','256']
   if 'checkpoint-after-add' in sys.argv:argv+=['--checkpoint-after-add-at','3000000','--checkpoint-directory',B+'/checkpoint-3m-prebalance32']
   if 'balance-only' in sys.argv:
    argv[argv.index('--restore-checkpoint')+1]=B+'/checkpoint-3m-prebalance32'
    argv+=['--balance-only']
  experiment_env={k:v for k,v in __import__('os').environ.items() if k in ['SPANN_ACCEPT_MODE','SPANN_INSERTION_NAVIGATION_WINDOW','SPANN_SAVE_WORKERS','SPANN_INSERTION_WORKERS','SPANN_SERIALIZE_BOUNDARY_REFRESH','SPANN_REFRESH_PREPARE_UNDER_TURN']}
  spec={'env':experiment_env,'instance_id':iid,'deadline':deadline,'binary_sha256':sha,'reader_binary':reader,'argv':argv,'evidence':B+'/'+name+'-process'+str(owner),'detailed':detailed,'profile':profile}
  specs[iid]=spec;(local/(mode+'.json')).write_text(json.dumps(spec,indent=2));put(iid,local/(mode+'.json'),B+'/'+name+'-'+mode+'.json')
 def launch(iid,mode):
  supervisor='remote-run32.py' if kind.startswith('epoch32') else 'remote-run31.py'
  cmd='nohup python3 '+B+'/'+supervisor+' '+B+'/'+name+'-'+mode+'.json > '+B+'/'+name+'-'+mode+'.supervisor.log 2>&1 < /dev/null & echo $!'
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
   code="import os,signal,pathlib; p=pathlib.Path('/proc/"+str(pid)+"/cmdline'); cmd=p.read_bytes() if p.exists() else b''; expected="+repr((B+('/remote-run32.py' if kind.startswith('epoch32') else '/remote-run31.py')).encode())+";\nif expected in cmd: os.kill("+str(pid)+",signal.SIGTERM)"
   ssh(iid,'python3 -c '+shlex.quote(code))
  time.sleep(2)
  def collect(pair):
   iid,spec=pair;output=spec['argv'][spec['argv'].index('--output')+1]
   code="import pathlib,json; roots=[pathlib.Path("+repr(output)+"),pathlib.Path("+repr(spec['evidence'])+")]; print(json.dumps({str(p):p.read_text() for root in roots if root.exists() for p in root.iterdir() if p.is_file() and p.name!='perf.data' and p.stat().st_size<200000000}))"
   (local/('owner'+('1' if iid==worker else '2')+'-receipts.json')).write_text(ssh(iid,'python3 -c '+shlex.quote(code),timeout=180))
  with concurrent.futures.ThreadPoolExecutor(2) as pool:list(pool.map(collect,specs.items()))
 print('COLLECTED',name,flush=True)
if __name__=='__main__':main()
