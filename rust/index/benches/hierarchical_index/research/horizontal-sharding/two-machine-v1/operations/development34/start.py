"""Reuse only the two admitted sandbox instances under a new guarded window."""
from pathlib import Path
import sys,json,time,subprocess,concurrent.futures,hashlib
from decimal import Decimal
P=Path(__file__).resolve().parent;O=P.parent;sys.path.insert(0,str(O));import lifecycle as l
W=O/'window-epoch34';A=P/'admission.json'
# Close the prior guarded reservation exactly once using its recorded API stop receipt.
ledger_path=O.parent.parent/'SPEND-LEDGER.json';ledger=json.loads(ledger_path.read_text());prior=O/'window-epoch29c/closed.json';rel=str(prior.relative_to(O.parent.parent));summary=ledger['covered_compute_summary']
if not any(x['path']==rel for x in summary['sources']):
 cost=Decimal(str(json.loads(prior.read_text())['compute_usd_upper_bound']))
 for k in ['compute_upper_bound_usd','first_month_compute_plus_reserved_storage_usd']:summary[k]=str(Decimal(summary[k])+cost)
 summary['approval_minus_covered_first_month_bound_usd']=str(Decimal(str(ledger['approval_usd']))-Decimal(summary['first_month_compute_plus_reserved_storage_usd']))
 summary['sources'].append({'path':rel,'bytes':prior.stat().st_size,'sha256':l.digest(prior)})
ledger['active_compute_reservations']=[x for x in ledger['active_compute_reservations'] if x['window']!='window-epoch29c'];assert Decimal(summary['first_month_compute_plus_reserved_storage_usd'])+Decimal('12.30')<Decimal(str(ledger['approval_usd']));ledger_path.write_text(json.dumps(ledger,indent=2)+'\n')
artifacts=[P/'start.py',P/'run.py',O/'lifecycle.py',O/'remote-guard.sh',O/'development33/accepted-profile.json',O.parent/'local-tests/EPOCH33-COMPLETION.json']
l.write(A,{'root_authorized_execution':True,'combined_review_passed':True,'review_basis':'Existing reviewed instance-bound lifecycle and prior accepted binary; user explicitly authorizes new diagnostic, implementation and benchmarks. New source must pass local validation and review before build.','window_seconds':5400,'rates_usd_hour':{i:4.032 for i in l.BINDINGS},'compute_cap_usd':12.30,'scope':'Reuse retained 2M checkpoint and compiler cache; diagnostic, focused optimization and same-binary comparison; leave machines running under guards. No new instances or volumes.','local_artifacts':[{'path':str(f),'sha256':l.digest(f)} for f in artifacts]})
subprocess.run([sys.executable,str(O/'lifecycle.py'),'start','--window',str(W),'--admission',str(A)],check=True)
intent=json.loads((W/'start-intent.json').read_text());end=int(intent['deadline_unix']);wid='epoch34-'+str(int(intent['observed_start_unix']));intent['window_id']=wid;l.write(W/'start-intent.json',intent)
while time.time()<end-1200:
 hosts=l.instances()
 if all(x['State']['Name']=='running' and x.get('PublicIpAddress') for x in hosts.values()):break
 time.sleep(3)
else:raise RuntimeError('Hosts did not become ready')
l.write(P/'hosts.json',hosts);l.write(W/'running-readback.json',hosts)
known=Path('/private/tmp/spann-epoch34-known-hosts');lines=[]
for i,h in hosts.items():
 key=(O/'window-epoch28'/('hostkey-'+i+'.txt')).read_text().split();lines.append(h['PublicIpAddress']+' '+key[1]+' '+key[2])
known.write_text('\n'.join(lines)+'\n')
SSH=['-i','/Users/chroma/Documents/lyon.pem','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+str(known)]
def prepare(pair):
 i,h=pair;args=['ssh',*SSH,'ubuntu@'+h['PublicIpAddress']]
 for attempt in range(50):
  r=subprocess.run(args+['true'],capture_output=True,text=True)
  if r.returncode==0:break
  time.sleep(3)
 else:raise RuntimeError('SSH unavailable '+i)
 cmd=f'SPANN_DEADLINE_UNIX={end} SPANN_WINDOW_ID={wid} SPANN_INSTANCE_ID={i} bash -s'
 r=subprocess.run(args+[cmd],input=(O/'remote-guard.sh').read_text(),text=True,capture_output=True,timeout=50);(P/(i+'-guard.txt')).write_text(r.stdout+r.stderr);assert r.returncode==0,r.stderr
 r=subprocess.run(args+["df -B1 /home/ubuntu; du -h -d 1 /home/ubuntu/spann-two-machine-v1/development30 | sort -h | tail -n 15"],capture_output=True,text=True,timeout=60);(P/(i+'-disk.txt')).write_text(r.stdout+r.stderr);assert r.returncode==0
with concurrent.futures.ThreadPoolExecutor(2) as pool:list(pool.map(prepare,hosts.items()))
ledger=json.loads(ledger_path.read_text());ledger['active_compute_reservations'].append({'window':'window-epoch34','maximum_compute_usd':12.30,'deadline_unix':end,'hourly_rate_both_machines_usd':8.064,'status':'running with local and both remote guards','not_yet_in_closed_compute_summary':True});ledger_path.write_text(json.dumps(ledger,indent=2)+'\n');print('READY',json.dumps({i:h['PublicIpAddress'] for i,h in hosts.items()}),flush=True)
