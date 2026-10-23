import json,subprocess,time,shlex
from pathlib import Path
P=Path('horizontal-sharding/two-machine-v1/operations').resolve()
W=P/'window-epoch28-recovery'
intent=json.loads((W/'start-intent.json').read_text());end=int(intent['deadline_unix'])
hosts=json.loads((W/'running-readback.json').read_text())
def run(args,**kw):
 return subprocess.run(args,check=True,capture_output=True,text=True,timeout=45,**kw).stdout
if True:
 for iid,host in hosts.items():
  ip=host['PublicIpAddress'];key=(P/'window-epoch28'/('hostkey-'+iid+'.txt')).read_text().split()
  known=W/('known-host-'+iid);known.write_text(ip+' '+key[1]+' '+key[2]+'\n')
  ssh=['ssh','-i','/Users/chroma/Documents/lyon.pem','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile="'+str(known)+'"','ubuntu@'+ip]
  while time.time()<end-120:
   try:run(ssh+['true']);break
   except subprocess.CalledProcessError:time.sleep(3)
  else:raise RuntimeError('SSH not ready')
  command=f'SPANN_DEADLINE_UNIX={end} SPANN_WINDOW_ID=epoch28-recovery SPANN_INSTANCE_ID={iid} bash -s'
  if not (W/('remote-guard-'+iid+'.txt')).exists(): (W/('remote-guard-'+iid+'.txt')).write_text(run(ssh+[command],input=(P/'remote-guard.sh').read_text()))
  script='''import pathlib,json,hashlib
root=pathlib.Path('/home/ubuntu/spann-two-machine-v1/epoch28')
out={'files':[],'text':{}}; total=0
for p in sorted(root.rglob('*')):
 if not p.is_file() or p.is_symlink() or 'source' in p.relative_to(root).parts: continue
 n=p.stat().st_size
 out['files'].append({'path':str(p),'bytes':n})
 if n<4000000 and total+n<16000000 and (p.suffix in ('.json','.jsonl','.stderr','.stdout','.log','.txt') or p.name in ('stderr','stdout')):
  data=p.read_bytes();out['text'][str(p)]={'sha256':hashlib.sha256(data).hexdigest(),'content':data.decode('utf-8',errors='replace')};total+=n
print(json.dumps(out))'''
  (W/('recovered-'+iid+'.json')).write_text(run(ssh+['python3 -c '+shlex.quote(script)]))
  print('Recovered '+iid,flush=True)
