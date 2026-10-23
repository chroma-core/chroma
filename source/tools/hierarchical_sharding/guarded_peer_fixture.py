"""Run a tiny intentional failure on two explicitly supplied idle sandbox hosts.

This is a control-plane integration test, not an index benchmark. It creates
fresh owned fixture directories, never deletes them, and launches no indexer.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import socket
import subprocess
import sys
import time
import uuid
from .guarded_host_batch import run_batch
from .host_supervisor import atomic_json
from .resource_guard import group_members,same_process


def stage(root,index,mask,run_id):
    root=Path(root)/str(index);root.mkdir(parents=True,exist_ok=False);scope=root/'owned';scope.mkdir()
    command=[sys.executable,'-c','import time;time.sleep(2);raise SystemExit(3)' if index==0 else 'import time;time.sleep(30)']
    guard=dict(commands=[command],concurrency=1,scopes=[dict(path=str(scope),role='empty-owned-fixture')],abort_plan='verified-process-groups-no-detach',sample_interval_s=.1,termination_grace_s=.1,limits=dict(scope_bytes_cap=16*1024**2,scope_stop_margin_bytes=1024**2,disk_free_floor_bytes=1024**3,disk_stop_margin_bytes=1024**3,rss_cap_bytes=256*1024**2,rss_stop_margin_bytes=16*1024**2))
    config=dict(run_id=run_id,cpu_affinity=mask,lease_path=str(root/'lease.json'),lease_ttl_s=60,absolute_deadline_unix=time.time()+90,guard=guard)
    path=root/'config.json';atomic_json(path,config)
    source=Path(__file__).resolve().parent
    return dict(actual_host=socket.gethostname(),config_path=str(path),config_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),run_id=run_id,output_path=str(root/'supervisor'),source_root=str(source.parents[1]),source_sha256={name:hashlib.sha256((source/name).read_bytes()).hexdigest() for name in ('resource_guard.py','host_supervisor.py','guarded_host_batch.py')})


def verify(entry):
    output=Path(entry['output_path']);terminal=json.loads((output/'terminal.json').read_text());identity=json.loads((output/'identity.json').read_text())
    groups=terminal['known_writer_groups']
    if terminal.get('reconciled') is not True or not groups:raise AssertionError('no actual reconciled writer group evidence')
    deadline=time.monotonic()+5
    while time.monotonic()<deadline and any(same_process(row) or group_members(row) for row in groups):time.sleep(.05)
    if any(same_process(row) or group_members(row) for row in groups):raise AssertionError('recorded writer PID or group survives')
    return dict(actual_host=socket.gethostname(),run_id=entry['run_id'],supervisor_identity=identity,terminal=terminal,recorded_writer_pids_absent=True,recorded_writer_groups_empty=True,abort_marker_present=(output/'abort.json').exists())


def remote(host,function,args):
    if not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.@-]*',host):raise ValueError('trusted explicit sandbox host required')
    code='import sys,json\nsys.path.insert(0,"/mnt/spann/source")\nfrom tools.hierarchical_sharding.guarded_peer_fixture import '+function+'\nprint(json.dumps('+function+'(*'+repr(args)+')))\n'
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5',host,shlex.join(['python3','-c',code])],capture_output=True,text=True,timeout=10,check=True)
    return json.loads(result.stdout)


def run(hosts,output,mask='0'):
    if len(hosts)!=2 or len(set(hosts))!=2:raise ValueError('two distinct sandbox hosts required')
    output=Path(output);output.mkdir(parents=True,exist_ok=False);run_id='peer-fixture-'+uuid.uuid4().hex;root='/mnt/spann/results/'+run_id
    report=dict(status='running',run_id=run_id,remote_root=root)
    try:
        entries=[]
        for index,host in enumerate(hosts):
            entry=remote(host,'stage',[root,index,mask,run_id+'-'+str(index)]);entry['host']=host;entries.append(entry)
        if len({e['actual_host'] for e in entries})!=2:raise ValueError('SSH names resolve to the same physical host')
        atomic_json(output/'staged.json',entries)
        config=dict(hosts=entries,poll_interval_s=.2,heartbeat_interval_s=10,control_timeout_s=5,timeout_s=45)
        result=run_batch(config,output/'driver')
        if result['complete'] or result['missing_terminal_hosts']:raise AssertionError('intentional peer failure was successful or unreconciled')
        checked=[remote(e['host'],'verify',[e]) for e in entries]
        a,b=checked
        if a['terminal']['status']!='aborted-or-failed' or b['terminal']['status']!='aborted-or-failed':raise AssertionError('failure/peer-abort outcomes missing')
        if not b['abort_marker_present'] or b['terminal'].get('reason') not in ('driver requested abort','lease expired or has an invalid clock'):raise AssertionError('second host did not stop through the peer-abort path')
        if any(f['phase'] not in ('terminal',) for f in result['failures']):raise AssertionError('control failure invalidates intended fixture')
        report.update(status='passed',expected_failure='host0 command exits3; host1 writer stops through peer abort',hosts=checked,batch_report=result)
        return report
    except BaseException as error:
        report.update(status='failed',error=str(error));raise
    finally:atomic_json(output/'fixture.json',report)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--hosts',nargs=2,required=True);parser.add_argument('--output',required=True);parser.add_argument('--cpu-affinity',default='0');args=parser.parse_args()
    print(json.dumps(run(args.hosts,args.output,args.cpu_affinity)))

if __name__=='__main__':main()
