#!/usr/bin/env python3
"""Recover bounded text evidence from frozen28; always stop both task hosts."""
import hashlib,json,shlex,subprocess,time
from pathlib import Path
import lifecycle as l
P=Path(__file__).resolve().parent
W=P/'window-epoch28-recovery'
A=P/'admission-epoch28-recovery.json'
def run(args,**kw):
 return subprocess.run(args,check=True,capture_output=True,text=True,timeout=45,**kw).stdout
prices=json.loads((P/'r6i16-price-epoch28-recovery.json').read_text())
rates=[]
for raw in prices['PriceList']:
 for term in json.loads(raw)['terms']['OnDemand'].values():
  for d in term['priceDimensions'].values():
   if d['unit']=='Hrs':rates.append(float(d['pricePerUnit']['USD']))
assert rates and set(rates)=={4.032}, rates
old=json.loads((P/'admission-epoch28.json').read_text())
for item in old['local_artifacts']:
 assert l.digest(Path(item['path']))==item['sha256'],item['path']
artifacts=[P/'recover-epoch28.py',P/'lifecycle.py',P/'remote-guard.sh',P/'admission-epoch28.json',P/'r6i16-price-epoch28-recovery.json']
l.write(A,dict(root_authorized_execution=True,combined_review_passed=True,review_basis='Retained frozen28 independent review and unchanged lifecycle/guard; this window only recovers text evidence, with no build or benchmark admission.',window_seconds=600,rates_usd_hour={i:4.032 for i in l.BINDINGS},compute_cap_usd=1.60,scope='Recover small retained epoch28 logs only; no build, workload, corpus download or source mutation.',local_artifacts=[dict(path=str(p),sha256=l.digest(p)) for p in artifacts]))
try:
 print(run(['python3',str(P/'lifecycle.py'),'start','--window',str(W),'--admission',str(A)]),flush=True)
 intent=json.loads((W/'start-intent.json').read_text()); end=int(intent['deadline_unix'])
 while time.time()<end-180:
  hosts=l.instances()
  if all(i['State']['Name']=='running' and i.get('PublicIpAddress') for i in hosts.values()):break
  time.sleep(3)
 else:raise RuntimeError('Hosts not ready in recovery window')
 l.write(W/'running-readback.json',hosts)
 for iid,host in hosts.items():
  ip=host['PublicIpAddress'];key=(P/'window-epoch28'/('hostkey-'+iid+'.txt')).read_text().split()
  known=W/('known-host-'+iid);known.write_text(ip+' '+key[1]+' '+key[2]+'\n')
  ssh=['ssh','-i','/Users/chroma/Documents/lyon.pem','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+str(known),'ubuntu@'+ip]
  while time.time()<end-120:
   try:run(ssh+['true']);break
   except subprocess.CalledProcessError:time.sleep(3)
  else:raise RuntimeError('SSH not ready')
  command=f'SPANN_DEADLINE_UNIX={end} SPANN_WINDOW_ID=epoch28-recovery SPANN_INSTANCE_ID={iid} bash -s'
  (W/('remote-guard-'+iid+'.txt')).write_text(run(ssh+[command],input=(P/'remote-guard.sh').read_text()))
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
finally:
 if (W/'start-intent.json').exists() and not (W/'closed.json').exists():
  l.stop(W,'recovery-stop')
  print('Both task instances API-stopped',flush=True)
