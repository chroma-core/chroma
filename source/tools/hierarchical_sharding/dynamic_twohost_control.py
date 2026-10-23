"""Supervise the tiny two-host correctness scenario through pinned SSH.

Workers are staged into fresh directories first. Host supervisors own actual
agents and Rust children; a driver resource guard owns this controller and its
SSH tunnels. Worker leases bound cleanup if this controller disappears.
"""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import threading
import time
from .dynamic_remote import AgentClient
from .dynamic_ssh import ssh_argv
from .guarded_host_batch import run_batch
from .tree_manifest import canonical_bytes,_atomic_write,file_hash


def admission(config):
    root=Path(config['root'])
    if not root.is_absolute() or root.resolve()!=root or '..' in root.parts:raise ValueError('canonical fresh driver scope required')
    from .dynamic_ssh import authenticated_key_map,ORIGINAL_RECEIPT_SHA256
    authenticated=authenticated_key_map(config['host_key_evidence_directory'])
    original=Path(config['original_host_key_receipt'])
    if file_hash(original)!=ORIGINAL_RECEIPT_SHA256:raise ValueError('original host-key provenance differs')
    original_rows=json.loads(original.read_text())
    if original_rows['driver_instance_id']!='i-00268d1e1dfea0357':raise ValueError('original driver differs')
    for row in original_rows['rows']:
        kind,key=row['actual_ed25519_public_key'].split()[:2]
        if authenticated.get(row['instance_id'])!=dict(account='079819354890',private_ip=row['host'],key_type=kind,key_base64=key):raise ValueError('independent key channels disagree')
    expected_known=''.join(row['host']+' '+' '.join(row['actual_ed25519_public_key'].split()[:2])+'\n' for row in original_rows['rows'])
    if config.get('fault') not in (None,'lease-expiry-before-topology'):raise ValueError('unknown fixture fault mode')
    workers=config['workers']
    if set(workers)!={'worker-a','worker-b'}:raise ValueError('two exact logical workers required')
    identities=[entry['expected_identity'] for entry in workers.values()]
    if {row['instanceId'] for row in identities}!={'i-09b86223b5620b35d','i-011ff180f03584f40'} or any(row['accountId']!='079819354890' or row['region']!='us-east-1' for row in identities):raise ValueError('only the admitted sandbox workers are allowed')
    if config['deadline_unix']<=time.time() or config['deadline_unix']>time.time()+600:raise ValueError('at most ten-minute fixture window required')
    for worker,entry in workers.items():
        proof=authenticated.get(entry['expected_identity']['instanceId'])
        if proof is None or proof['private_ip']!=entry['host'] or Path(entry['known_hosts']).read_text()!=expected_known:raise ValueError('exact authenticated worker key admission differs')
        if entry['host']!=entry['expected_identity']['privateIp']:raise ValueError('worker address differs from actual identity admission')
        for key in ('source_root','config_path','output_path','ready_path'):
            path=Path(entry[key])
            if not path.is_absolute() or path.resolve()!=path or '..' in path.parts or not str(path).startswith('/mnt/spann/dynamic-twohost-v1/'):raise ValueError('remote fixture path escapes fresh owned root')
        if file_hash(entry['known_hosts'])!=entry['known_hosts_sha256']:raise ValueError('admitted strict host keys changed')
        if len(entry['token'])!=64 or any(char not in '0123456789abcdef' for char in entry['token']):raise ValueError('private 256-bit agent token required')
        ssh_argv(entry['host'],entry['ssh_key'],entry['known_hosts'])
    return config


def remote_python(entry,code,timeout):
    result=subprocess.run(ssh_argv(entry['host'],entry['ssh_key'],entry['known_hosts'])+[shlex.join([entry.get('python','python3'),'-c',code])],capture_output=True,text=True,timeout=timeout,check=True)
    if len(result.stdout)>256*1024 or len(result.stderr)>256*1024:raise ValueError('SSH control output exceeds admission')
    return json.loads(result.stdout)


def remote_action(action,entry):
    from .guarded_host_batch import remote_control
    source=Path(entry['source_root'])
    for name,digest in entry['source_sha256'].items():
        relative=Path(name)
        if relative.is_absolute() or '..' in relative.parts or file_hash(source/relative)!=digest:raise ValueError('executed worker source differs')
    if action=='expire-fixture-lease':
        host=json.loads(Path(entry['config_path']).read_text())
        if file_hash(entry['config_path'])!=entry['config_sha256'] or host['run_id']!=entry['run_id']:raise ValueError('fault target configuration differs')
        _atomic_write(Path(host['lease_path']),canonical_bytes(dict(run_id=entry['run_id'],renewed_unix=0)))
        return dict(status='fixture-lease-expired')
    return remote_control(action,entry)


def stage_worker(config):
    """Stage control files inside an already copied fresh fixture source root."""
    from .dynamic_agent import actual_identity
    from .dynamic_runtime import PRODUCER_SOURCE_SHA256
    identity=actual_identity()
    if any(identity.get(key)!=value for key,value in config['expected_identity'].items()):raise ValueError('actual stage worker identity differs')
    root=Path(config['scope_root']);source=Path(config['source_root'])
    if not root.is_absolute() or root.resolve()!=root or not str(root).startswith('/mnt/spann/dynamic-twohost-v1/') or source.parent!=root:raise ValueError('fresh canonical fixture source root required')
    if root.stat().st_uid!=os.geteuid() or root.stat().st_mode&0o077 or source.resolve()!=source:raise ValueError('owned private fixture namespace required')
    actual_sources={str(path.relative_to(source)):file_hash(path) for path in source.rglob('*.py')}
    if actual_sources!=config['source_sha256']:raise ValueError('complete executed Python source set differs')
    if (root/'host-config.json').exists():raise ValueError('stage cannot reuse a partial host')
    if file_hash(config['binary'])!=config['binary_sha256'] or PRODUCER_SOURCE_SHA256!=config['producer_sha256']:raise ValueError('binary or producer differs at stage')
    for name,digest in config['source_sha256'].items():
        relative=Path(name)
        if relative.is_absolute() or '..' in relative.parts or file_hash(source/relative)!=digest:raise ValueError('staged source differs')
    token=root/'token';token_fd=os.open(token,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(token_fd,'w') as stream:stream.write(config['token']);stream.flush();os.fsync(stream.fileno())
    agent=dict(config,token_file=str(token),ready_path=str(root/'agent-ready.json'),port=0)
    agent.pop('token');agent_path=root/'agent-config.json';_atomic_write(agent_path,canonical_bytes(agent))
    guard=dict(commands=[[sys.executable,'-m','tools.hierarchical_sharding.dynamic_agent','--config',str(agent_path)]],concurrency=1,scopes=[dict(path=str(root),role='entire-owned-worker-fixture')],abort_plan='verified-process-groups-no-detach',sample_interval_s=.2,termination_grace_s=1,limits=dict(scope_bytes_cap=512*1024**2,scope_stop_margin_bytes=64*1024**2,disk_free_floor_bytes=512*1024**3,disk_stop_margin_bytes=64*1024**3,rss_cap_bytes=1024**3,rss_stop_margin_bytes=128*1024**2))
    host=dict(run_id=config['run_id'],cpu_affinity='0-1',lease_path=str(root/'lease.json'),lease_ttl_s=30,absolute_deadline_unix=config['deadline_unix'],guard=guard)
    host_path=root/'host-config.json';_atomic_write(host_path,canonical_bytes(host))
    return dict(run_id=config['run_id'],host=identity['privateIp'],expected_identity=config['expected_identity'],config_path=str(host_path),config_sha256=file_hash(host_path),output_path=str(root/'host-supervisor'),ready_path=str(root/'agent-ready.json'),source_root=str(source),source_sha256=config['source_sha256'],python=sys.executable)


def control(action,entry,timeout):
    entry={key:value for key,value in entry.items() if key!='token'}
    code='import sys,json;sys.path.insert(0,'+repr(entry['source_root'])+');from tools.hierarchical_sharding.dynamic_twohost_control import remote_action;print(json.dumps(remote_action('+repr(action)+','+repr(entry)+')))'
    return remote_python(entry,code,timeout)


SOURCE_BOOTSTRAP = r"""
import hashlib,io,json,os,sys,tarfile
from pathlib import Path,PurePosixPath
line=sys.stdin.buffer.readline(262145)
if len(line)>262144:raise ValueError('staging header exceeds admission')
header=json.loads(line);config=header['config'];size=header['bytes']
if type(size) is not int or not 0<size<=32*1024**2:raise ValueError('source archive wire cap')
raw=sys.stdin.buffer.read(size+1)
if len(raw)!=size or hashlib.sha256(raw).hexdigest()!=header['sha256']:raise ValueError('source wire identity')
root=Path(config['scope_root']);source=Path(config['source_root'])
if not root.is_absolute() or root.resolve()!=root or source!=root/'source' or not str(root).startswith('/mnt/spann/dynamic-twohost-v1/'):raise ValueError('fresh sandbox scope')
root.mkdir(mode=0o700,parents=True,exist_ok=False);source.mkdir(mode=0o700)
seen=set();total=0
with tarfile.open(fileobj=io.BytesIO(raw),mode='r:') as stream:
 for member in stream:
  path=PurePosixPath(member.name)
  if not member.isfile() or member.type!=tarfile.REGTYPE or member.pax_headers or path.is_absolute() or '..' in path.parts or str(path)!=member.name or not member.name.startswith('tools/hierarchical_sharding/') or not member.name.endswith('.py') or member.name in seen:raise ValueError('unsafe source member')
  seen.add(member.name);total+=member.size
  if len(seen)>512 or member.size>16*1024**2 or total>64*1024**2:raise ValueError('source expansion cap')
  target=source/member.name;target.parent.mkdir(parents=True,exist_ok=True);data=stream.extractfile(member).read()
  if len(data)!=member.size or hashlib.sha256(data).hexdigest()!=config['source_sha256'].get(member.name):raise ValueError('source file identity')
  with target.open('xb') as out:out.write(data);out.flush();os.fsync(out.fileno())
if seen!=set(config['source_sha256']):raise ValueError('complete source set differs')
sys.path.insert(0,str(source))
from tools.hierarchical_sharding.dynamic_runtime import durable_directory
durable_directory(source)
from tools.hierarchical_sharding.dynamic_twohost_control import stage_worker
print(json.dumps(stage_worker(config)))
"""


def stage_from_driver(config):
    archive=Path(config['source_archive'])
    if archive.stat().st_size>32*1024**2 or file_hash(archive)!=config['source_archive_sha256']:raise ValueError('frozen source archive differs')
    raw=archive.read_bytes();workers={}
    for worker,entry in sorted(config['workers'].items()):
        staged=dict(run_id=entry['run_id'],worker_id=worker,scope_root=str(Path(entry['source_root']).parent),source_root=entry['source_root'],expected_identity=entry['expected_identity'],binary=config['binary'],binary_sha256=config['binary_sha256'],producer_sha256=config['producer_sha256'],source_sha256=config['source_sha256'],token=entry['token'],deadline_unix=config['deadline_unix'])
        header=canonical_bytes(dict(config=staged,bytes=len(raw),sha256=config['source_archive_sha256']))
        if len(header)>256*1024:raise ValueError('staging control metadata exceeds admission')
        argv=ssh_argv(entry['host'],entry['ssh_key'],entry['known_hosts'])+[shlex.join([entry.get('python','python3'),'-c',SOURCE_BOOTSTRAP])]
        result=subprocess.run(argv,input=header+raw,capture_output=True,timeout=20,check=True)
        if len(result.stdout)>256*1024 or len(result.stderr)>256*1024:raise ValueError('bounded stage output exceeded')
        workers[worker]=dict(entry,**json.loads(result.stdout))
    return dict(config,workers=workers)


def local_port():
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));return sock.getsockname()[1]


def execute(config):
    from .dynamic_twohost_scenario import run
    config=admission(config);root=Path(config['root']);root.mkdir(exist_ok=False)
    entries=[dict(entry,worker_id=worker) for worker,entry in sorted(config['workers'].items())]
    tunnels=[];clients={};report=dict(status='starting',run_id=config['run_id']);batch_future=None
    pool=concurrent.futures.ThreadPoolExecutor(max_workers=1);lease_fault_event=threading.Event()
    def batch_control(action,entry,timeout):
        if action=='renew' and entry.get('worker_id')=='worker-b' and lease_fault_event.is_set():return dict(status='renewal-suppressed-for-fixture-fault')
        return control(action,entry,timeout)
    try:
        config=stage_from_driver(config)
        entries=[dict(entry,worker_id=worker) for worker,entry in sorted(config['workers'].items())]
        batch=dict(hosts=[{key:value for key,value in entry.items() if key!='token'} for entry in entries],poll_interval_s=.2,heartbeat_interval_s=5,control_timeout_s=5,timeout_s=max(1,config['deadline_unix']-time.time()))
        batch_future=pool.submit(run_batch,batch,root/'batch',batch_control)
        for entry in entries:
            while True:
                if batch_future.done():raise ValueError('worker batch ended before agent readiness: '+str(batch_future.result()))
                if time.time()>config['deadline_unix']-30:raise TimeoutError('agent startup exhausted fixture deadline')
                code='import json;from pathlib import Path;p=Path('+repr(entry['ready_path'])+');print(p.read_text() if p.exists() else "null")'
                ready=remote_python(entry,code,5)
                if ready is not None:break
                time.sleep(.2)
            if ready.get('run_id')!=entry['run_id'] or ready.get('worker_id')!=entry['worker_id']:raise ValueError('agent ready identity differs')
            port=local_port();remote_port=ready['port']
            if type(remote_port) is not int or not 1<=remote_port<=65535:raise ValueError('invalid loopback worker port')
            log=(root/(entry['worker_id']+'.tunnel.log')).open('xb')
            argv=ssh_argv(entry['host'],entry['ssh_key'],entry['known_hosts']);host=argv.pop()
            process=subprocess.Popen(argv+['-o','ExitOnForwardFailure=yes','-N','-L',f'127.0.0.1:{port}:127.0.0.1:{remote_port}',host],stdin=subprocess.DEVNULL,stdout=log,stderr=log)
            tunnels.append((process,log));client=AgentClient('http://127.0.0.1:'+str(port),entry['token'],entry['run_id'],worker_id=entry['worker_id'],instance_id=entry['expected_identity']['instanceId'])
            while True:
                if process.poll() is not None:raise ValueError('pinned SSH tunnel exited')
                try:info=client.rpc({'op':'info'});break
                except OSError:
                    if time.time()>config['deadline_unix']-30:raise
                    time.sleep(.1)
            if any(info['identity'].get(key)!=value for key,value in entry['expected_identity'].items()) or info['binary_sha256']!=config['binary_sha256'] or info['producer_sha256']!=config['producer_sha256']:raise ValueError('actual agent identity/binary/source differs')
            clients[entry['worker_id']]=client
            report.setdefault('actual_agents',{})[entry['worker_id']]=info
        def health():
            if batch_future.done():raise ValueError('worker batch ended before publication')
            for entry in entries:
                status=control('poll',entry,5)
                if status.get('status')!='running':raise ValueError('observed worker terminal blocks publication')
            if any(process.poll() is not None for process,_ in tunnels):raise ValueError('observed tunnel failure blocks publication')
        def phase_hook(phase,step,pointer,candidate):
            if config.get('fault')=='lease-expiry-before-topology' and phase=='topology-prepared' and step==0:
                report['lease_fault']=dict(pointer=str(pointer),before_sha256=file_hash(pointer),candidate_sha256=__import__('hashlib').sha256(canonical_bytes(candidate)).hexdigest(),target_worker='worker-b')
                _atomic_write(root/'fault-progress.json',canonical_bytes(report['lease_fault']))
                target=next(entry for entry in entries if entry['worker_id']=='worker-b')
                lease_fault_event.set()
                control('expire-fixture-lease',target,5)
                until=time.monotonic()+45
                while not batch_future.done() and time.monotonic()<until:time.sleep(.1)
                if not batch_future.done():raise TimeoutError('lease fault did not reconcile in45seconds')
        report['scenario']=run(clients,config['binary'],root/'scenario',health=health,phase_hook=phase_hook,control_entries=entries)
        if report['scenario']['status']!='passed':raise ValueError('scenario requires cleanup reconciliation')
        for client in clients.values():client.rpc({'op':'shutdown'})
        terminal=batch_future.result(timeout=45)
        report['batch']=terminal
        if terminal.get('complete') is not True or terminal.get('missing_terminal_hosts'):raise ValueError('worker terminal groups are unreconciled')
        report['status']='passed';return report
    except BaseException as error:
        report.update(status='failed',error=type(error).__name__+': '+str(error))
        for entry in entries:
            try:control('abort',entry,5)
            except Exception as abort_error:report.setdefault('abort_errors',[]).append(str(abort_error))
        if batch_future is not None:
            try:report['batch']=batch_future.result(timeout=70)
            except Exception as reconcile_error:report['reconciliation_error']=str(reconcile_error)
        if config.get('fault')=='lease-expiry-before-topology' and lease_fault_passes(report,entries):
            report['status']='passed-expected-lease-failure';return report
        raise
    finally:
        for process,log in tunnels:
            if process.poll() is None:process.terminate()
            try:process.wait(timeout=5)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
            log.close()
        pool.shutdown(wait=True)
        _atomic_write(root/'controller.json',canonical_bytes(report))


def lease_fault_passes(report,entries):
    fault=report.get('lease_fault');batch=report.get('batch',{})
    if not fault or not Path(fault['pointer']).is_file() or file_hash(fault['pointer'])!=fault['before_sha256'] or batch.get('complete') is not False or batch.get('missing_terminal_hosts'):return False
    terminals=batch.get('hosts',{})
    if set(terminals)!={entry['host'] for entry in entries}:return False
    for entry in entries:
        terminal=terminals[entry['host']]
        if terminal.get('reconciled') is not True or terminal.get('status')!='aborted-or-failed' or not terminal.get('known_writer_groups'):return False
        reason=terminal.get('reason') or ''
        if entry['worker_id']=='worker-b' and 'lease' not in reason:return False
        if entry['worker_id']!='worker-b' and reason!='driver requested abort':return False
    return True


def driver_guard(config,path):
    scope=Path(config['driver_scope'])
    return dict(commands=[[sys.executable,'-m','tools.hierarchical_sharding.dynamic_twohost_control','--config',str(path),'--inside-driver-guard']],concurrency=1,scopes=[dict(path=str(scope),role='all-owned-dynamic-driver-files')],abort_plan='verified-process-groups-no-detach',sample_interval_s=.2,termination_grace_s=1,timeout_s=max(1,config['deadline_unix']-time.time()),limits=dict(scope_bytes_cap=512*1024**2,scope_stop_margin_bytes=64*1024**2,disk_free_floor_bytes=8*1024**3,disk_stop_margin_bytes=2*1024**3,rss_cap_bytes=1024**3,rss_stop_margin_bytes=128*1024**2))


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config',required=True);parser.add_argument('--inside-driver-guard',action='store_true');args=parser.parse_args()
    path=Path(args.config).resolve()
    if path.stat().st_uid!=os.geteuid() or path.stat().st_mode&0o077:raise ValueError('driver token-bearing configuration must be private')
    config=json.loads(path.read_text());admission(config)
    source=Path(__file__).resolve().parents[2]
    actual_sources={str(item.relative_to(source)):file_hash(item) for item in (source/'tools'/'hierarchical_sharding').rglob('*.py')}
    if actual_sources!=config['source_sha256']:raise ValueError('executed driver source differs from frozen worker source')
    from .dynamic_runtime import PRODUCER_SOURCE_SHA256
    if PRODUCER_SOURCE_SHA256!=config['producer_sha256'] or file_hash(config['binary'])!=config['binary_sha256']:raise ValueError('driver producer or immutable reader binary differs')
    from .dynamic_agent import actual_identity
    driver=actual_identity()
    if driver.get('instanceId')!='i-00268d1e1dfea0357' or driver.get('accountId')!='079819354890' or driver.get('region')!='us-east-1':raise ValueError('actual sandbox driver identity differs')
    scope=Path(config['driver_scope'])
    if not scope.is_absolute() or scope.resolve()!=scope or scope not in Path(config['root']).parents:raise ValueError('driver scope must contain fresh execution root')
    if scope.stat().st_uid!=os.geteuid() or scope.stat().st_mode&0o077:raise ValueError('driver owned scope must be private')
    for owned in (path,source,Path(config['source_archive']),Path(config['host_key_evidence_directory']),Path(config['original_host_key_receipt'])):
        if owned.resolve()!=owned or (owned!=scope and scope not in owned.parents):raise ValueError('all owned driver source/config/evidence must be inside guard scope')
    if args.inside_driver_guard:
        command=Path('/proc/'+str(os.getppid())+'/cmdline').read_bytes().split(b'\0')
        if b'tools.hierarchical_sharding.resource_guard' not in command or str(scope/'driver-guard.json').encode() not in command:raise ValueError('actual resource-guard parent is required')
        execute(config);return
    os.sched_setaffinity(0,{0,1})
    guard=driver_guard(config,path)
    guard_path=scope/'driver-guard.json';_atomic_write(guard_path,canonical_bytes(guard))
    result=subprocess.run([sys.executable,'-m','tools.hierarchical_sharding.resource_guard','--config',str(guard_path),'--output',str(scope/'driver-guard')],timeout=max(1,config['deadline_unix']-time.time())+90,check=True)
    terminal=json.loads((scope/'driver-guard'/'report.json').read_text())
    if terminal.get('status')!='passed':raise ValueError('driver resource guard failed')


if __name__=='__main__':main()
