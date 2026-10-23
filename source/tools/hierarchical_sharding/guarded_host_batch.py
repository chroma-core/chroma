"""Coordinate independent worker supervisors; uncertain hosts block publication.

Host configuration files and input directories are staged before this command.
Every host runs local writer argv jobs. SSH carries bounded control messages,
not the lifetime of a writer. Failed cases and directories are never retried.
"""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess
import time
from .host_supervisor import atomic_json, renew_lease
from .subtree_compaction import journal


def remote_control(action,entry):
    """Execute on a worker; retain the supervisor when the SSH channel closes."""
    import os
    import sys
    config_path=Path(entry['config_path']);payload=config_path.read_bytes()
    if hashlib.sha256(payload).hexdigest()!=entry['config_sha256']:raise ValueError('host configuration hash differs')
    config=json.loads(payload)
    if config['run_id']!=entry['run_id']:raise ValueError('host configuration run identity differs')
    output=Path(entry['output_path']);lease=Path(config['lease_path'])
    if action=='launch':
        if output.exists():raise ValueError('host output exists; never reuse a partial run')
        renew_lease(lease,entry['run_id'])
        log_path=output.with_name(output.name+'.launch.log')
        with log_path.open('xb') as log:
            process=subprocess.Popen([sys.executable,'-m','tools.hierarchical_sharding.host_supervisor','--config',str(config_path),'--output',str(output)],cwd=entry.get('source_root','/mnt/spann/source'),stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
        from .resource_guard import process_identity
        return dict(status='launched',run_id=entry['run_id'],identity=process_identity(process.pid),commands=len(config['guard']['commands']),lease_ttl_s=config.get('lease_ttl_s',30),absolute_deadline_unix=config['absolute_deadline_unix'])
    if action=='renew':
        renew_lease(lease,entry['run_id']);return dict(status='renewed')
    if action=='abort':
        # Revocation also covers the short interval before supervisor mkdir.
        atomic_json(lease,dict(run_id=entry['run_id'],renewed_unix=0))
        if output.exists():atomic_json(output/'abort.json',dict(run_id=entry['run_id'],requested_unix=time.time()))
        return dict(status='abort-requested')
    if action=='poll':
        terminal=output/'terminal.json'
        if not terminal.exists():
            from .host_supervisor import lease_reason
            reason=('driver requested abort' if (output/'abort.json').exists() else
                    'absolute deadline reached' if time.time()>=config['absolute_deadline_unix'] else
                    lease_reason(lease,entry['run_id'],config.get('lease_ttl_s',30),time.time()))
            return dict(status='aborting' if reason else 'running',reason=reason)
        result=json.loads(terminal.read_text())
        if result.get('run_id')!=entry['run_id']:raise ValueError('terminal belongs to another run')
        return dict(status='terminal',terminal=result)
    raise ValueError('unsupported control operation')


def ssh_control(action,entry,timeout):
    host=entry['host']
    if not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.@-]*',host):raise ValueError('trusted SSH host required')
    source=entry.get('source_root','/mnt/spann/source')
    code='import sys,json\nsys.path.insert(0,'+repr(source)+')\nfrom tools.hierarchical_sharding.guarded_host_batch import remote_control\nprint(json.dumps(remote_control('+repr(action)+','+repr(entry)+')))\n'
    command=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5',host,shlex.join([entry.get('python','python3'),'-c',code])]
    result=subprocess.run(command,capture_output=True,text=True,timeout=timeout,check=True)
    return json.loads(result.stdout)


def accepted_terminal(terminal,commands):
    guard=terminal.get('guard_report') or {}
    outcomes=guard.get('outcomes',[])
    return (terminal.get('status')=='passed' and terminal.get('reconciled') is True
            and guard.get('status')=='passed' and len(outcomes)==commands
            and {r.get('index') for r in outcomes}==set(range(commands))
            and all(r.get('status')=='ok' and r.get('returncode')==0 for r in outcomes))


def run_batch(config,output,control=ssh_control,clock=time.monotonic,sleep=time.sleep,on_failure=None):
    """Return complete only after every host proves all local writer outcomes."""
    entries=config['hosts']
    if not entries or len(entries)>8 or len({e['host'] for e in entries})!=len(entries):raise ValueError('one unique entry per worker required')
    if len({(e['host'],e['output_path']) for e in entries})!=len(entries):raise ValueError('host outputs must be unique')
    interval=config.get('poll_interval_s',2);heartbeat=config.get('heartbeat_interval_s',10);timeout=config.get('control_timeout_s',5)
    if not 0<interval<=10 or not 0<heartbeat<=10 or not 0<timeout<=10:raise ValueError('bounded control intervals required')
    output=Path(output);output.mkdir(parents=True,exist_ok=False);log=output/'journal.jsonl'
    atomic_json(output/'config.json',config)
    launched={};terminals={};failures=[];aborting=False;abort_deadline=None;start=clock();last_renew=start
    def controls(action,subset):
        results={}
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8,len(subset) or 1)) as pool:
            futures={pool.submit(control,action,entry,timeout):entry for entry in subset}
            for future in concurrent.futures.as_completed(futures):
                entry=futures[future];host=entry['host']
                try:results[host]=future.result();journal(log,action,host=host,result=results[host])
                except Exception as error:
                    failures.append(dict(host=host,phase=action,error=str(error)))
                    if on_failure is not None:on_failure('control failed: '+action)
                    journal(log,'control-failed',failure=failures[-1])
        return results
    def abort_all(reason):
        nonlocal aborting,abort_deadline
        if aborting:return
        aborting=True
        if on_failure is not None:on_failure(reason)
        journal(log,'peer-abort',reason=reason)
        controls('abort',entries)
        # Lost SSH control is bounded by each independently running lease.
        ttl=max([r.get('lease_ttl_s',120) for r in launched.values()]+[120])
        abort_deadline=clock()+ttl+20
    try:
        launched=controls('launch',entries)
        for host,result in launched.items():
            if result.get('status')!='launched' or not result.get('identity'):
                failures.append(dict(host=host,phase='launch',error='supervisor identity missing'))
            if result.get('lease_ttl_s',0)<=heartbeat+2*timeout:
                failures.append(dict(host=host,phase='launch',error='lease has insufficient heartbeat/control headroom'))
        if failures:abort_all('one or more host launches failed or are uncertain')
        while True:
            pending=[e for e in entries if e['host'] not in terminals]
            if not pending:break
            results=controls('poll',pending)
            for host,row in results.items():
                if row.get('status') not in ('running','terminal'):
                    failures.append(dict(host=host,phase='poll',error='host is aborting or has invalid health'))
                    if on_failure is not None:on_failure('host no longer healthy')
                if row.get('status')=='terminal':
                    terminal=row['terminal'];terminals[host]=terminal
                    atomic_json(output/('terminal-'+str(entries.index(next(e for e in entries if e['host']==host)))+'.json'),terminal)
                    if not accepted_terminal(terminal,launched.get(host,{}).get('commands',-1)):
                        failures.append(dict(host=host,phase='terminal',error='host failed or writer groups are unreconciled'))
                        if on_failure is not None:on_failure('host terminal failed')
            if failures:abort_all('peer failure or uncertain control state')
            if not aborting and clock()-last_renew>=heartbeat:
                controls('renew',pending);last_renew=clock()
                if failures:abort_all('lease renewal failed')
            if not aborting and clock()-start>=config.get('timeout_s',7200):
                failures.append(dict(phase='deadline',error='driver batch deadline reached'));abort_all('driver deadline')
            if aborting and clock()>=abort_deadline:break
            sleep(interval)
    except BaseException as error:
        failures.append(dict(phase='driver-interrupted',error=str(error)))
        abort_all(str(error));journal(log,'driver-interrupted',error=str(error))
        # Independent leases still bound activity if this controller cannot poll.
        raise
    finally:
        missing=[e['host'] for e in entries if e['host'] not in terminals]
        complete=not failures and not missing and all(accepted_terminal(terminals[e['host']],launched[e['host']]['commands']) for e in entries)
        report=dict(complete=complete,status='passed' if complete else 'failed-or-unreconciled',hosts=terminals,launched=launched,failures=failures,missing_terminal_hosts=missing,elapsed_s=clock()-start,publication_allowed=complete,retry_allowed=False,scope='Host-local process groups; no blind remote kills, deletion, or automatic retry.')
        atomic_json(output/'report.json',report)
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
    result=run_batch(json.loads(Path(args.config).read_text()),args.output);print(json.dumps(result));raise SystemExit(0 if result['complete'] else 1)

if __name__=='__main__':main()
