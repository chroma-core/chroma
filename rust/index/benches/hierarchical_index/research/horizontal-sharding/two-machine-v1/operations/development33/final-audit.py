"""Verify retained task machines and reconcile the open compute reservation."""
from pathlib import Path
import sys,json,time,concurrent.futures
from run import P,H,ssh
sys.path.insert(0,str(P.parent));import lifecycle as l
who=l.identity();hosts=l.instances();assert who['Account']=='079819354890';assert set(hosts)==set(H)
def inspect(i):
 return i,ssh(i,"ps -eo pid,args | grep -E '[t]wo_machine_spann|[r]emote-run33.py'; systemctl list-timers --all --no-pager | grep 'spann-two-machine'; df -B1 /home/ubuntu | tail -1")
with concurrent.futures.ThreadPoolExecutor(2) as pool:evidence=dict(pool.map(inspect,H))
for i,output in evidence.items():
 assert 'remote-run33.py' not in output and 'two_machine_spann' not in output,output
 assert '-stop.timer' in output,output
now=time.time();window=json.loads((P.parent/'window-epoch29c/start-intent.json').read_text());assert now<window['deadline_unix']
result={'account':who['Account'],'observed_unix':now,'instances':{i:h['State']['Name'] for i,h in hosts.items()},'ssh_evidence':evidence,'guard_deadline_unix':window['deadline_unix'],'machines_left_running_by_user_request':True,'recurring_followup':'PAUSED','no_benchmark_processes':True}
(P/'final-cloud-audit.json').write_text(json.dumps(result,indent=2)+'\n')
ledger_path=P.parent.parent.parent/'SPEND-LEDGER.json';ledger=json.loads(ledger_path.read_text())
for row in ledger['active_compute_reservations']:
 if row['window']=='window-epoch29c':row.update(elapsed_compute_upper_bound_usd_at_snapshot=(now-window['observed_start_unix'])*8.064/3600,snapshot_observed_unix=now,status='epoch33 complete; machines left running by user request with existing local and remote budget guards')
ledger_path.write_text(json.dumps(ledger,indent=2)+'\n');print(json.dumps(result,indent=2))
