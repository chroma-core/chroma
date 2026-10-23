"""Supervise one worker's local writer groups with a renewable driver lease.

Launch this process independently of the SSH connection. The driver renews a
run-bound lease on each host and writes abort.json to every host on any failure.
Success requires every host's reconciled terminal report. Lost contact never
counts as completion; the lease and absolute deadline bound remote activity.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import re
import subprocess
import sys
import time
from .resource_guard import process_identity, same_process, stop_groups, group_members
from .subtree_compaction import journal


def atomic_json(path,value):
    path=Path(path);temporary=path.with_name(path.name+'.tmp')
    with temporary.open('x') as stream:
        json.dump(value,stream,allow_nan=False);stream.flush();os.fsync(stream.fileno())
    os.replace(temporary,path)
    descriptor=os.open(path.parent,os.O_RDONLY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)


def lease_reason(path,run_id,ttl,now):
    try:
        value=json.loads(Path(path).read_text())
        if value.get('run_id')!=run_id:return 'lease belongs to another run'
        renewed=value['renewed_unix']
        if type(renewed) not in (int,float) or not (0<=now-renewed<=ttl):return 'lease expired or has an invalid clock'
    except (OSError,ValueError,KeyError):return 'lease missing or malformed'
    return None


def renew_lease(path,run_id):
    """Invoke on the worker so lease timestamps use that worker's clock."""
    path=Path(path)
    if path.exists() and json.loads(path.read_text()).get('run_id')!=run_id:
        raise ValueError('cannot overwrite another run lease')
    atomic_json(path,dict(run_id=run_id,renewed_unix=time.time()))


def writer_identities(path):
    identities={}
    if Path(path).exists():
        for line in Path(path).read_text().splitlines():
            try:row=json.loads(line)
            except ValueError:continue  # A final partial record cannot hide prior starts.
            if row.get('phase')=='writer-start':
                identity=row['identity'];identities[(identity['pid'],identity['start_ticks'])]=identity
    return list(identities.values())


def validate_config(config,now):
    if not isinstance(config.get('run_id'),str) or not config['run_id']:raise ValueError('run_id required')
    ttl=config.get('lease_ttl_s',30)
    if not 2<=ttl<=120:raise ValueError('lease TTL must be between two and 120 seconds')
    deadline=config.get('absolute_deadline_unix')
    if type(deadline) not in (int,float) or not now<deadline<=now+7200:raise ValueError('absolute deadline must be in the next two hours')
    mask=config.get('cpu_affinity')
    if not isinstance(mask,str) or not re.fullmatch(r'\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*',mask):raise ValueError('explicit CPU affinity mask required')
    guard=config['guard']
    if not guard.get('commands'):raise ValueError('local writer commands required')
    for command in guard['commands']:
        if not isinstance(command,list) or not command or any(not isinstance(token,str) for token in command):raise ValueError('commands must be explicit local argv lists')
        if any(Path(token).name in ('ssh','scp','sftp','rsync') for token in command):raise ValueError('host supervisor accepts local commands only')
    if guard.get('abort_plan')!='verified-process-groups-no-detach':raise ValueError('nondetaching process-group plan required')
    if type(guard.get('concurrency')) is not int or guard['concurrency']<1:raise ValueError('explicit positive writer concurrency required')
    return ttl,deadline


def supervise_host(config,output):
    ttl,deadline=validate_config(config,time.time())
    cpus=set()
    for interval in config['cpu_affinity'].split(','):
        parts=list(map(int,interval.split('-')));first=parts[0];last=parts[-1]
        if last<first:raise ValueError('CPU affinity range must ascend')
        cpus.update(range(first,last+1))
    os.sched_setaffinity(0,cpus)
    if set(os.sched_getaffinity(0))!=cpus:raise ValueError('supervisor CPU affinity differs')
    output=Path(output);output.mkdir(parents=True,exist_ok=False);log=output/'journal.jsonl'
    lease=Path(config['lease_path']);abort_path=output/'abort.json'
    reason=lease_reason(lease,config['run_id'],ttl,time.time())
    if reason:
        atomic_json(output/'terminal.json',dict(status='rejected',reason=reason,reconciled=True,run_id=config['run_id']))
        raise ValueError(reason)
    guard=dict(config['guard']);guard['timeout_s']=min(guard.get('timeout_s',7200),max(1,deadline-time.time()))
    atomic_json(output/'guard-config.json',guard)
    atomic_json(output/'identity.json',dict(run_id=config['run_id'],identity=process_identity(os.getpid()),cpu_affinity=sorted(cpus),absolute_deadline_unix=deadline,lease_path=str(lease),lease_ttl_s=ttl,guard_config_sha256=hashlib.sha256((output/'guard-config.json').read_bytes()).hexdigest()))
    process=None;cause=None;reconciled=False;previous={}
    def interrupt(signum,frame):raise KeyboardInterrupt('host supervisor received signal '+str(signum))
    for signum in (signal.SIGTERM,signal.SIGINT):previous[signum]=signal.signal(signum,interrupt)
    try:
        with (output/'guard.stdout').open('wb') as stdout,(output/'guard.stderr').open('wb') as stderr:
            process=subprocess.Popen([sys.executable,'-m','tools.hierarchical_sharding.resource_guard','--config',str(output/'guard-config.json'),'--output',str(output/'guard')],stdout=stdout,stderr=stderr,start_new_session=True)
            identity=process_identity(process.pid)
            if identity is None:raise RuntimeError('guard exited before identity registration')
            journal(log,'guard-start',identity=identity)
            while process.poll() is None:
                cause=('absolute deadline reached' if time.time()>=deadline else 'driver requested abort' if abort_path.exists() else lease_reason(lease,config['run_id'],ttl,time.time()))
                if cause:break
                time.sleep(.1)
            if cause and same_process(identity):
                journal(log,'stop-guard',reason=cause,identity=identity);os.kill(process.pid,signal.SIGTERM)
                try:process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    stop_groups(writer_identities(output/'guard/journal.jsonl'),log)
                    if same_process(identity):os.kill(process.pid,signal.SIGKILL)
                    process.wait(timeout=5)
            else:process.wait(timeout=5)
        known=writer_identities(output/'guard/journal.jsonl')
        survivors=[row for row in known if group_members(row)]
        if survivors:
            stop_groups(survivors,log)
        reconciled=not any(group_members(row) for row in known)
        report_path=output/'guard/report.json'
        guard_report=json.loads(report_path.read_text()) if report_path.exists() else None
        status='passed' if not cause and process.returncode==0 and reconciled and guard_report and guard_report['status']=='passed' else 'aborted-or-failed'
        terminal=dict(run_id=config['run_id'],status=status,reason=cause,guard_returncode=process.returncode,reconciled=reconciled,known_writer_groups=known,guard_report=guard_report,finished_unix=time.time())
        atomic_json(output/'terminal.json',terminal);return terminal
    except BaseException as error:
        journal(log,'failed',error=str(error))
        try:
            if process and process.poll() is None:
                os.kill(process.pid,signal.SIGTERM)
                try:process.wait(timeout=10)
                except subprocess.TimeoutExpired:pass
            known=writer_identities(output/'guard/journal.jsonl');stop_groups(known,log)
            reconciled=not any(group_members(row) for row in known)
            if process and process.poll() is None:
                current=process_identity(process.pid)
                if current and same_process(identity):os.kill(process.pid,signal.SIGKILL);process.wait(timeout=5)
        finally:
            atomic_json(output/'terminal.json',dict(run_id=config['run_id'],status='failed',error=str(error),reconciled=reconciled,finished_unix=time.time()))
        raise
    finally:
        for signum,handler in previous.items():signal.signal(signum,handler)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config');parser.add_argument('--output');parser.add_argument('--renew-lease');parser.add_argument('--run-id');args=parser.parse_args()
    if args.renew_lease:
        if not args.run_id:parser.error('--run-id required')
        renew_lease(args.renew_lease,args.run_id)
    else:
        if not args.config or not args.output:parser.error('--config and --output required')
        report=supervise_host(json.loads(Path(args.config).read_text()),args.output)
        print(json.dumps(report));raise SystemExit(0 if report['status']=='passed' else 1)

if __name__=='__main__':main()
