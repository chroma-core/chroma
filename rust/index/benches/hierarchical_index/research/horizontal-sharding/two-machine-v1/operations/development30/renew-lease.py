"""Extend the existing development lease without restarting either task machine."""
import sys,os,signal,json,subprocess,time,shlex
from pathlib import Path
P=Path(__file__).resolve().parent;sys.path.insert(0,str(P.parent));import lifecycle as l
sys.path.insert(0,str(P));from run import ssh,H
l.identity();hosts=l.instances();assert set(hosts)==set(H) and all(x['State']['Name']=='running' for x in hosts.values())
W=P.parent/'window-epoch29c';old=json.loads((W/'start-intent.json').read_text());old_watch=json.loads((W/'watch-pid.json').read_text());now=time.time();end=int(now+5350);wid='epoch30-development-'+str(int(now));renew=P/('lease-'+str(int(now)));renew.mkdir()
(renew/'previous-intent.json').write_text(json.dumps(old,indent=2));(renew/'previous-watch.json').write_text(json.dumps(old_watch,indent=2))
ledger_path=P.parent.parent.parent/'SPEND-LEDGER.json';ledger=json.loads(ledger_path.read_text());cap=(end-old['observed_start_unix'])*8.064/3600+0.25
assert float(ledger['covered_compute_summary']['first_month_compute_plus_reserved_storage_usd'])+cap<ledger['approval_usd']
# New remote timers are verified before the old guards are retired.
for iid in H:
 command=f'SPANN_DEADLINE_UNIX={end} SPANN_WINDOW_ID={wid} SPANN_INSTANCE_ID={iid} bash -s'
 args=['ssh','-i','/Users/chroma/Documents/lyon.pem','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile=/private/tmp/spann-epoch29c-known-hosts','ubuntu@'+H[iid]['PublicIpAddress'],command]
 r=subprocess.run(args,input=(P.parent/'remote-guard.sh').read_text(),text=True,capture_output=True,timeout=45);(renew/(iid+'-guard.txt')).write_text(r.stdout+r.stderr);assert r.returncode==0,r.stderr
admission={'authorized_by':'User requests completing checkpoint baseline/optimization/comparison and permits keeping the two development machines running; existing $1000 budget applies','observed_unix':now,'deadline_unix':end,'maximum_compute_usd_since_original_start':cap,'account':'079819354890','instances':list(H),'no_start_or_stop_cycle':True}
(renew/'admission.json').write_text(json.dumps(admission,indent=2))
old.update(deadline_unix=end,window_id=wid,compute_cap_usd=cap,admission_path=str(renew/'admission.json'),admission_sha256=l.digest(renew/'admission.json'))
import datetime
old['deadline_utc']=datetime.datetime.fromtimestamp(end,datetime.timezone.utc).isoformat();l.write(W/'start-intent.json',old)
with (renew/'watch.log').open('a') as log:child=subprocess.Popen([sys.executable,str(P.parent/'lifecycle.py'),'watch','--window',str(W),'--deadline',str(end)],env=l.ENV,stdout=log,stderr=log,start_new_session=True)
for _ in range(50):
 ready=json.loads((W/'watch-ready.json').read_text())
 if ready.get('pid')==child.pid and ready.get('deadline')==end:break
 time.sleep(.1)
else:raise RuntimeError('new watchdog not ready; old guards remain')
# Verify the previous PID still names this exact task watchdog before signalling it.
cmd=subprocess.check_output(['ps','-p',str(old_watch['pid']),'-o','command='],text=True).strip()
assert str(P.parent/'lifecycle.py') in cmd and str(W) in cmd and ' watch ' in cmd,cmd
os.kill(old_watch['pid'],signal.SIGTERM)
prior=json.loads((renew/'previous-intent.json').read_text())
for iid in H:
 unit='spann-two-machine-'+prior['window_id']+'-stop.timer'
 (renew/(iid+'-retire-old.txt')).write_text(ssh(iid,shlex.join(['sudo','systemctl','stop',unit])))
l.write(W/'watch-pid.json',{'pid':child.pid,'deadline':end})
for row in ledger['active_compute_reservations']:
 if row['window']=='window-epoch29c':row.update(maximum_compute_usd=cap,deadline_unix=end,status='running development lease renewed without restart; local and remote budget guards active',elapsed_compute_upper_bound_usd_at_snapshot=(now-old['observed_start_unix'])*8.064/3600,snapshot_observed_unix=now)
ledger_path.write_text(json.dumps(ledger,indent=2)+'\n');print(json.dumps(admission,indent=2))
