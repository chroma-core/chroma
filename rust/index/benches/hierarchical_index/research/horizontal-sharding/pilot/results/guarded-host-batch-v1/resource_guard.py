"""Own local writer process groups and stop a run before declared resource limits.

Run one supervisor on each worker. Configuration declares every owned scope and
all commands; no process is discovered by name and no checkpoint is deleted.
Scope bytes include retained/private objects. Current/retained root breakdowns
require the separate immutable storage audit after writers stop.
"""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import stat as stat_mode
import subprocess
import threading
import time
from .subtree_compaction import journal


def process_identity(pid):
    try:
        # comm can contain spaces or parentheses; fields follow its final ')'.
        text=Path('/proc/'+str(pid)+'/stat').read_text(); fields=text[text.rfind(')')+2:].split()
        return dict(state=fields[0],pid=int(pid),pgid=int(fields[2]),session=int(fields[3]),start_ticks=int(fields[19]),rss_bytes=int(fields[21])*os.sysconf('SC_PAGE_SIZE'))
    except (FileNotFoundError,ProcessLookupError):
        return None


def same_process(identity):
    current=process_identity(identity['pid'])
    return current is not None and all(current[k]==identity[k] for k in ('pid','pgid','session','start_ticks'))


def group_members(identity):
    members=[]
    for path in Path('/proc').iterdir():
        if path.name.isdigit():
            row=process_identity(int(path.name))
            if row and row.get('state')!='Z' and row['pgid']==identity['pgid'] and row['session']==identity['session']:
                members.append(row)
    return members


def stop_groups(identities, log, grace_s=2):
    """Signal only groups whose recorded process identity is still verified."""
    pending=[]; failures=[]
    for identity in identities:
        if not same_process(identity):
            if group_members(identity):
                failures.append(dict(identity=identity,error='leader identity unavailable; group ownership cannot be verified'))
            continue
        members=group_members(identity)
        journal(log,'terminate-group',leader=identity,members=members)
        try:os.killpg(identity['pgid'],signal.SIGTERM)
        except ProcessLookupError:continue
        pending.append((identity,members))
    deadline=time.monotonic()+grace_s
    while time.monotonic()<deadline and any(any(same_process(m) for m in members) for _,members in pending):
        time.sleep(.05)
    for identity,members in pending:
        if any(same_process(member) for member in members):
            # A surviving recorded member anchors ownership after leader exit.
            try:os.killpg(identity['pgid'],signal.SIGKILL)
            except ProcessLookupError:pass
            journal(log,'kill-group',leader=identity)
    deadline=time.monotonic()+2
    while time.monotonic()<deadline and any(group_members(identity) for identity,_ in pending):
        time.sleep(.05)
    for identity,_ in pending:
        survivors=group_members(identity)
        if survivors:failures.append(dict(identity=identity,error='writer group survives termination',survivors=survivors))
    journal(log,'abort-reconciliation',ownership_failures=failures)
    if failures:raise RuntimeError('some process groups cannot be safely reconciled')


def inventory(scopes,max_entries):
    """Measure only explicit owned roots; reject symlinks and overlapping roots."""
    roots=[]
    for scope in scopes:
        path=Path(scope['path'])
        if not path.is_absolute() or path.is_symlink() or not path.is_dir() or path.resolve()!=path:
            raise ValueError('scope must be an existing canonical nonsymlink directory')
        if not scope.get('role'):
            raise ValueError('scope role required')
        if any(path==other or path in other.parents or other in path.parents for other in roots):
            raise ValueError('scope roots overlap')
        roots.append(path)
    seen=set(); total=0; allocated_total=0; count=0; result=[]; devices={}
    for scope,root in zip(scopes,roots):
        logical=unique=allocated=0
        for directory,dirs,files in os.walk(root,followlinks=False):
            for name in dirs+files:
                path=Path(directory)/name
                try:stat=path.lstat()
                except FileNotFoundError:continue  # Atomic rename/deletion; next sample sees the destination.
                count+=1
                if count>max_entries:raise ValueError('scope inventory entry limit exceeded')
                if stat_mode.S_ISLNK(stat.st_mode):raise ValueError('symlink in declared scope')
                if stat_mode.S_ISREG(stat.st_mode):
                    logical+=stat.st_size; key=(stat.st_dev,stat.st_ino)
                    if key not in seen:
                        seen.add(key);unique+=stat.st_size;total+=stat.st_size
                        blocks=stat.st_blocks*512;allocated+=blocks;allocated_total+=blocks
        stat=root.stat(); fs=os.statvfs(root)
        devices[str(stat.st_dev)]=dict(available_bytes=fs.f_bavail*fs.f_frsize,path=str(root))
        result.append(dict(path=str(root),role=scope['role'],logical_bytes=logical,unique_inode_bytes=unique,unique_inode_allocated_bytes=allocated))
    return dict(scopes=result,total_unique_inode_bytes=total,total_unique_inode_allocated_bytes=allocated_total,inventory_entries=count,filesystems=devices)


def violations(sample,limits):
    reasons=[]
    if sample['total_unique_inode_bytes'] >= limits['scope_bytes_cap']-limits['scope_stop_margin_bytes']:
        reasons.append('owned scope byte stop threshold reached')
    if any(fs['available_bytes'] <= limits['disk_free_floor_bytes']+limits['disk_stop_margin_bytes'] for fs in sample['filesystems'].values()):
        reasons.append('filesystem free-space stop threshold reached')
    if sample.get('aggregate_writer_rss_bytes',0) >= limits['rss_cap_bytes']-limits['rss_stop_margin_bytes']:
        reasons.append('aggregate writer RSS stop threshold reached')
    return reasons


def supervise(config,output):
    if not Path('/proc/self/stat').exists():raise ValueError('Linux /proc is required')
    output=Path(output);output.mkdir(parents=True,exist_ok=False);log=output/'journal.jsonl'
    required=('scope_bytes_cap','scope_stop_margin_bytes','disk_free_floor_bytes','disk_stop_margin_bytes','rss_cap_bytes','rss_stop_margin_bytes')
    limits=config['limits']
    if any(type(limits.get(k)) is not int or limits[k]<=0 for k in required):raise ValueError('explicit positive limits and stop margins required')
    if limits['scope_stop_margin_bytes']>=limits['scope_bytes_cap'] or limits['rss_stop_margin_bytes']>=limits['rss_cap_bytes']:
        raise ValueError('stop margin must be below cap')
    interval=config.get('sample_interval_s',1)
    if not .05<=interval<=10:raise ValueError('sample interval must be between .05 and ten seconds')
    if not config.get('commands') or not config.get('scopes'):raise ValueError('commands and complete owned scopes required')
    if config.get('abort_plan') != 'verified-process-groups-no-detach':raise ValueError('explicit verified-process-groups-no-detach abort plan required')
    for command in config['commands']:
        if not isinstance(command,list) or not command or any(not isinstance(x,str) for x in command):raise ValueError('commands must be argv lists')
    started=time.monotonic(); stop=threading.Event(); active={}; lock=threading.Lock(); outcomes=[]; reasons=[]
    (output/'config.json').write_text(json.dumps(config,indent=2)+'\n')
    def sample():
        row=inventory(config['scopes'],config.get('max_inventory_entries',1000000))
        with lock:identities=list(active.values())
        members={}
        for identity in identities:
            if same_process(identity):
                for member in group_members(identity):members[member['pid']]=member
        row['aggregate_writer_rss_bytes']=sum(p['rss_bytes'] for p in members.values());row['writer_processes']=list(members.values())
        journal(log,'resource-sample',sample=row);return row
    initial=sample();reasons.extend(violations(initial,limits))
    if reasons:
        journal(log,'admission-rejected',reasons=reasons);raise RuntimeError('; '.join(reasons))
    def run(index):
        if stop.is_set():return dict(index=index,status='not-started')
        with (output/(str(index)+'.stdout')).open('wb') as stdout,(output/(str(index)+'.stderr')).open('wb') as stderr:
            with lock:
                if stop.is_set():return dict(index=index,status='not-started')
                process=subprocess.Popen(config['commands'][index],stdout=stdout,stderr=stderr,start_new_session=True)
                identity=process_identity(process.pid)
                if not identity:
                    process.wait();return dict(index=index,status='failed',error='writer exited before identity registration')
                active[index]=identity
                journal(log,'writer-start',index=index,identity=identity,argv=config['commands'][index])
            code=process.wait()
            with lock:
                survivors=group_members(identity)
                if survivors:
                    journal(log,'writer-left-children',index=index,identity=identity,survivors=survivors)
                    stop.set()
                else:active.pop(index,None)
        row=dict(index=index,status='ok' if code==0 and not survivors else 'failed',returncode=code,unreconciled_children=survivors);journal(log,'writer-finish',**row);return row
    def abort(cause):
        stop.set();journal(log,'abort',reason=cause)
        with lock:identities=list(active.values())
        stop_groups(identities,log,config.get('termination_grace_s',2))
    old_handlers={}
    if threading.current_thread() is threading.main_thread():
        def interrupted(signum,frame):raise KeyboardInterrupt('supervisor received signal '+str(signum))
        for signum in (signal.SIGTERM,signal.SIGINT):
            old_handlers[signum]=signal.signal(signum,interrupted)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.get('concurrency',1)) as pool:
            futures=[pool.submit(run,index) for index in range(len(config['commands']))]
            while not all(f.done() for f in futures):
                if stop.is_set():
                    reasons.append('writer left unreconciled children');abort(reasons);break
                try:
                    reasons=violations(sample(),limits)
                    if time.monotonic()-started >= config.get('timeout_s',1800):reasons.append('run deadline reached')
                    if reasons:abort(reasons);break
                except BaseException as error:
                    abort(str(error));raise
                stop.wait(interval)
            outcomes=[f.result() for f in futures]
            with lock:remaining=list(active.values())
            if remaining:
                reasons.append('unreconciled writer groups remain');abort(reasons)
    except BaseException as error:
        try:abort(str(error))
        finally:journal(log,'failed',error=str(error))
        raise
    finally:
        for signum,handler in old_handlers.items():signal.signal(signum,handler)
    final_sample=sample()
    reasons.extend(violations(final_sample,limits))
    report=dict(status='passed' if not reasons and all(r['status']=='ok' for r in outcomes) else 'aborted-or-failed',reasons=reasons,outcomes=outcomes,final_inventory=final_sample,limits=limits,limitations=['Sampling and abort latency require explicit headroom; thresholds are not hard memory or storage quotas.','RSS sums include shared pages more than once.','Current/retained object reachability requires the immutable storage diagnostic after writers stop.','No deletion or automatic retry occurs.'])
    (output/'report.json').write_text(json.dumps(report,indent=2)+'\n');return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
    print(json.dumps(supervise(json.loads(Path(args.config).read_text()),args.output)))

if __name__=='__main__':main()
