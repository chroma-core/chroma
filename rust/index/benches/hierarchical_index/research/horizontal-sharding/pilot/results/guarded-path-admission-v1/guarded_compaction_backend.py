"""Run exact local index argv jobs through bounded, leased host supervisors.

The launcher also supports initial construction. Immutable snapshot inspection
and collection publication remain separate mandatory gates in the caller.
"""
import concurrent.futures
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import time
from .build import command
from .guarded_host_batch import run_batch,remote_control
from .host_supervisor import atomic_json


def covered(path,scopes):
    path=Path(path)
    return path.is_absolute() and '..' not in path.parts and all(Path(s['path']).is_absolute() and '..' not in Path(s['path']).parts for s in scopes) and any(path==Path(s['path']) or Path(s['path']) in path.parents for s in scopes)


def validate_jobs(jobs,batch,mask,concurrency):
    indexed={(job['host'],job['shard_id']):job for job in jobs}
    if len(indexed)!=len(jobs):raise ValueError('duplicate owner job')
    if {e['host'] for e in batch['hosts']}!={j['host'] for j in jobs}:raise ValueError('guard hosts differ from writer jobs')
    used=set()
    for entry in batch['hosts']:
        payload=Path(entry['local_config_path']).read_bytes()
        if hashlib.sha256(payload).hexdigest()!=entry['config_sha256']:raise ValueError('local host config hash differs from staged config')
        config=json.loads(payload);guard=config['guard']
        if config['run_id']!=entry['run_id'] or config['cpu_affinity']!=mask or guard['concurrency']!=concurrency:raise ValueError('guard allocation or run identity differs')
        owners=entry['job_shard_ids']
        if len(owners)!=len(set(owners)):raise ValueError('guard duplicates a subtree job')
        expected=[]
        for sid in owners:
            key=(entry['host'],sid)
            if key not in indexed or key in used:raise ValueError('guard owner differs from declared jobs')
            used.add(key);job=indexed[key]
            if job.get('cpu_affinity')!=mask:raise ValueError('writer argv CPU allocation differs from host supervisor')
            expected.append(command(dict(job,host='local')))
            for field in ('checkpoint_dir','vectors','id_map'):
                if not covered(job[field],guard['scopes']):raise ValueError('guard scopes omit writer '+field)
        if guard['commands']!=expected:raise ValueError('guard argv differs from exact local writer command')
        for path in (entry['output_path'],config['lease_path']):
            if not covered(path,guard['scopes']):raise ValueError('guard scopes omit supervisor metadata or lease')
    if used!=set(indexed):raise ValueError('guard does not cover every writer job')


def validate_remote_paths(paths):
    """Reject symlink ancestry on the owner host before any writer starts."""
    for value in paths:
        path=Path(value)
        if not path.is_absolute() or '..' in path.parts:
            raise ValueError('unsafe remote owned path')
        for part in (path,*path.parents):
            if part.is_symlink():raise ValueError('symlink in remote owned path ancestry: '+str(part))

def admit_remote_paths(jobs,batch):
    for entry in batch['hosts']:
        config=json.loads(Path(entry['local_config_path']).read_bytes())
        paths=[s['path'] for s in config['guard']['scopes']]+[entry['output_path'],config['lease_path']]
        paths += [j[f] for j in jobs if j['host']==entry['host'] for f in ('checkpoint_dir','vectors','id_map')]
        code='import sys\nsys.path.insert(0,'+repr(entry.get('source_root','/mnt/spann/source'))+')\nfrom tools.hierarchical_sharding.guarded_compaction_backend import validate_remote_paths\nvalidate_remote_paths('+repr(paths)+')\n'
        subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5',entry['host'],shlex.join([entry.get('python','python3'),'-c',code])],capture_output=True,text=True,timeout=15,check=True)


def remote_logs(entry,limit=64*1024*1024):
    # Verify config identity again before reading only this owned run's outputs.
    terminal=remote_control('poll',entry)
    if terminal['status']!='terminal':raise ValueError('host lacks terminal reconciliation')
    root=Path(entry['output_path'])/'guard';used=0;logs=[]
    for index,sid in enumerate(entry['job_shard_ids']):
        item=dict(index=index,shard_id=sid)
        for suffix in ('stdout','stderr'):
            path=root/(str(index)+'.'+suffix)
            size=path.stat().st_size if path.exists() else 0;used+=size
            if used>limit:raise ValueError('host writer-log transfer cap exceeded')
            item[suffix]=path.read_text(errors='replace') if size else ''
        logs.append(item)
    path=root/'journal.jsonl';used+=path.stat().st_size
    if used>limit:raise ValueError('host journal transfer cap exceeded')
    journal=path.read_text();events=[json.loads(line) for line in journal.splitlines()]
    return dict(logs=logs,journal=journal,events=events,bytes=used)


def collect_logs(entry):
    code='import sys,json\nsys.path.insert(0,'+repr(entry.get('source_root','/mnt/spann/source'))+')\nfrom tools.hierarchical_sharding.guarded_compaction_backend import remote_logs\nprint(json.dumps(remote_logs('+repr(entry)+')))\n'
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5',entry['host'],shlex.join([entry.get('python','python3'),'-c',code])],capture_output=True,text=True,timeout=60,check=True)
    return json.loads(result.stdout)


def run_guarded_jobs(jobs,batch,log_dir,mask,concurrency,runner=run_batch,collector=collect_logs):
    validate_jobs(jobs,batch,mask,concurrency)
    if runner is run_batch:admit_remote_paths(jobs,batch)
    log_dir=Path(log_dir);log_dir.mkdir(parents=True,exist_ok=False);atomic_json(log_dir/'jobs.json',jobs)
    for number,entry in enumerate(batch['hosts']):
        payload=Path(entry['local_config_path']).read_bytes()
        if hashlib.sha256(payload).hexdigest()!=entry['config_sha256']:raise ValueError('host config changed after admission')
        (log_dir/('host-'+str(number)+'-config.json')).write_bytes(payload)
    started=time.monotonic();result=runner(batch,log_dir/'host-batch');launch_control_s=time.monotonic()-started
    job_indices={(j['host'],j['shard_id']):i for i,j in enumerate(jobs)};records=[];spans={};failures=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8,len(batch['hosts']))) as pool:
        futures={pool.submit(collector,entry):entry for entry in batch['hosts']}
        for future in concurrent.futures.as_completed(futures):
            entry=futures[future];host=entry['host']
            try:
                collected=future.result();number=batch['hosts'].index(entry)
                (log_dir/('host-'+str(number)+'-journal.jsonl')).write_text(collected['journal'])
                starts={e['index']:e['time_unix'] for e in collected['events'] if e['phase']=='writer-start'}
                finishes={e['index']:e['time_unix'] for e in collected['events'] if e['phase']=='writer-finish'}
                if starts and finishes:spans[host]=dict(first_start_unix=min(starts.values()),last_finish_unix=max(finishes.values()),writer_span_s=max(finishes.values())-min(starts.values()),clock='worker wall clock; no cross-host synchronization claim')
                outcomes={r['index']:r for r in (result['hosts'].get(host,{}).get('guard_report') or {}).get('outcomes',[])}
                if [(item['index'],item['shard_id']) for item in collected['logs']]!=list(enumerate(entry['job_shard_ids'])):raise ValueError('collected logs differ from exact owner order')
                for item in collected['logs']:
                    index=item['index'];sid=item['shard_id'];target=job_indices[(host,sid)]
                    for suffix in ('stdout','stderr'):(log_dir/(str(target)+'.'+suffix)).write_text(item[suffix])
                    outcome=outcomes.get(index,dict(status='unknown',returncode=None))
                    row=dict(shard_id=sid,host=host,generation=jobs[target]['generation'],status=outcome['status'],returncode=outcome.get('returncode'),started_unix=starts.get(index),finished_unix=finishes.get(index),elapsed_s=finishes[index]-starts[index] if index in starts and index in finishes else None)
                    records.append(row);atomic_json(log_dir/(str(target)+'.job.json'),row)
            except Exception as error:failures.append(dict(host=host,error=str(error)))
    report=dict(complete=result['complete'] and not failures and len(records)==len(jobs) and all(r['status']=='ok' and r['elapsed_s'] is not None and r['elapsed_s']>=0 for r in records),jobs=records,jobs_per_host=concurrency,elapsed_s=launch_control_s,driver_launch_control_s=launch_control_s,metadata_log_collection_s=time.monotonic()-started-launch_control_s,host_writer_spans=spans,host_batch=result,collection_failures=failures,construction_scope='Caller construction time includes launch/control and bounded per-owner log collection; host writer spans and metadata collection are reported separately.',scope='Local supervisor pools; writer spans use each worker clock. Driver launch/control includes leases, resource checks and reconciliation. Full snapshot audit/publication remains required.')
    atomic_json(log_dir/'build.json',report);return report


def inspect_parallel(entries,inspect,pull,destination,max_hosts=8):
    """Finish verification on every owner host before returning to publication."""
    if type(max_hosts) is not int or not 1<=max_hosts<=8:raise ValueError('verification host concurrency must be between one and eight')
    grouped={host:[e for e in entries.values() if e['host']==host] for host in {e['host'] for e in entries.values()}}
    results=[];failures=[]
    def one(host,rows):
        verified=inspect(host,rows);pull(host,verified,destination);return verified
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_hosts,len(grouped))) as pool:
        futures={pool.submit(one,host,rows):host for host,rows in grouped.items()}
        for future in concurrent.futures.as_completed(futures):
            try:results.extend(future.result())
            except Exception as error:failures.append(dict(host=futures[future],error=str(error)))
    if failures:raise RuntimeError('host snapshot verification failed; publication blocked: '+json.dumps(failures))
    return results
