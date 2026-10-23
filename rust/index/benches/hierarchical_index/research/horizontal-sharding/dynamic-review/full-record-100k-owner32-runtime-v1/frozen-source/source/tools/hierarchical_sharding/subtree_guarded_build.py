"""Construct one shared tree through leased host-local writer pools.

Input staging and checksum verification precede construction timing. Every owner
must finish its bounded local command and pass an actual pinned reopen before
an atomic global manifest is published. Failed cases retain private output and
cannot be reused.
"""
import argparse
import hashlib
import json
from pathlib import Path
import time
from .guarded_compaction_backend import run_guarded_jobs, inspect_parallel
from .subtree_compaction import journal
from .subtree_orchestrate import ssh, inspect_host, pull_metadata, publish_checked
from .tree_manifest import assemble_tree, canonical_bytes, content_hash


def jobs_for(prepared, config):
    if prepared['start_id'] != 0 or prepared['input_row_count'] != prepared['global_row_count']:
        raise ValueError('initial construction requires a complete canonical prefix')
    slots = prepared['subtrees']
    if set(config['entries']) != {s['subtree_id'] for s in slots}:
        raise ValueError('configured owners differ from prepared ownership')
    checkpoint = config['checkpoint_rows']
    if type(checkpoint) is not int or checkpoint <= 0:
        raise ValueError('checkpoint_rows must be positive')
    jobs, entries = [], {}
    destinations=set()
    for slot in slots:
        if slot['local_start'] != 0 or slot['append_row_count'] != slot['vector_count'] or slot['vector_count'] <= 0:
            raise ValueError('initial owner must contain only new rows')
        sid = slot['subtree_id']; entry = dict(config['entries'][sid])
        for field in ('index','vectors','id_map'):
            path=Path(entry[field])
            if not path.is_absolute() or '..' in path.parts:
                raise ValueError('worker paths must be absolute without parent traversal')
        identity=(entry['host'],entry['index'])
        if identity in destinations:raise ValueError('duplicate checkpoint destination')
        destinations.add(identity)
        entry.update(subtree_id=sid, generation=1, vector_count=slot['vector_count'])
        entries[sid] = entry
        jobs.append(dict(host=entry['host'], binary=config['measured_binary'], vectors=entry['vectors'], id_map=entry['id_map'], checkpoint_dir=entry['index'], shard_id=sid, source_id=prepared['source_id'], dimension=prepared['dimension'], row_count=slot['vector_count'], global_id_start=0, id_stride=1, generation=1, metric=prepared['metric'], threads=config.get('threads',1), runtime_threads=2, cpu_affinity=config['cpu_affinity'], checkpoint_rows=min(checkpoint,slot['vector_count']), boundary_hash=slot['routing_boundary_hash']))
    if sum(s['vector_count'] for s in slots) != prepared['global_row_count']:
        raise ValueError('owner counts do not cover the canonical prefix')
    return jobs, entries


def remote_preflight(rows, binary, binary_sha256, measured_binary, measured_sha256):
    def digest(path):
        value=hashlib.sha256()
        with Path(path).open('rb') as stream:
            for block in iter(lambda:stream.read(1024*1024),b''):value.update(block)
        return value.hexdigest()
    if digest(binary) != binary_sha256 or digest(measured_binary) != measured_sha256:
        raise ValueError('worker binary or measurement wrapper differs')
    verified=[]
    for row in rows:
        if Path(row['index']).exists() or Path(row['index']).is_symlink():
            raise ValueError('initial construction requires a fresh absent checkpoint')
        for field in ('vectors','id_map'):
            path=Path(row[field]);desc=row[field+'_artifact']
            if path.is_symlink() or not path.is_file() or path.stat().st_size != desc['bytes'] or digest(path) != desc['sha256']:
                raise ValueError('staged '+field+' differs from prepared artifact')
        verified.append(dict(subtree_id=row['subtree_id'],input_bytes=row['vectors_artifact']['bytes']+row['id_map_artifact']['bytes']))
    return verified


def construct(prepared_path, config_path, case_path):
    prepared_path=Path(prepared_path);prepared=json.loads(prepared_path.read_text());config=json.loads(Path(config_path).read_text());case=Path(case_path);case.mkdir(parents=True,exist_ok=False);log=case/'journal.jsonl'
    try:
        jobs,entries=jobs_for(prepared,config);batch=json.loads(Path(config['guarded_batch_config']).read_text());(case/'jobs.json').write_bytes(canonical_bytes(jobs));(case/'config.json').write_bytes(canonical_bytes(config))
        slots={s['subtree_id']:s for s in prepared['subtrees']};verification_started=time.monotonic()
        for host in sorted({e['host'] for e in entries.values()}):
            rows=[dict(e,vectors_artifact=slots[sid]['vectors'],id_map_artifact=slots[sid]['local_to_global']) for sid,e in entries.items() if e['host']==host]
            args=(rows,config['binary'],config['binary_sha256'],config['measured_binary'],config['measured_sha256'])
            code='import sys,json\nsys.path.insert(0,"/mnt/spann/source")\nfrom tools.hierarchical_sharding.subtree_guarded_build import remote_preflight\nprint(json.dumps(remote_preflight(*'+repr(args)+')))\n'
            journal(log,'input-preflight',host=host,verified=json.loads(ssh(host,code)))
        input_verification_s=time.monotonic()-verification_started
        started=time.monotonic();journal(log,'construction-start')
        report=run_guarded_jobs(jobs,batch,case/'build',config['cpu_affinity'],config['jobs_per_host'])
        construction_s=time.monotonic()-started;journal(log,'construction-finish',elapsed_s=construction_s,complete=report['complete'])
        if not report['complete']:
            raise RuntimeError('guarded initial construction did not reconcile successfully')
        warnings={p.name:p.read_text() for p in (case/'build').glob('*.stderr') if 'without full convergence' in p.read_text()}
        if warnings:
            (case/'balance-warnings.json').write_bytes(canonical_bytes(warnings));raise RuntimeError('owner balancing did not converge')
        inspections=inspect_parallel(entries,lambda host,rows:inspect_host(host,rows,config['binary'],prepared),pull_metadata,case/'copies',config.get('verification_max_hosts',8))
        tree=assemble_tree(prepared_path,[case/'copies'/s['subtree_id']/'manifest-1.json' for s in prepared['subtrees']],1,case);(case/'tree-1.json').write_bytes(canonical_bytes(tree));(case/'remote-reopen.json').write_bytes(canonical_bytes(inspections))
        publication_started=time.monotonic();pointer=publish_checked(case/'current.json',tree,inspections,case);publication_s=time.monotonic()-publication_started
        result=dict(status='passed',execution_backend='guarded-host-pools',host_order=config['host_order'],entries=entries,tree=str((case/'tree-1.json').resolve()),tree_sha256=content_hash(tree),binary=config['binary'],measured_binary=config['measured_binary'],prepared=str(prepared_path),cpu_affinity=config['cpu_affinity'],jobs_per_host=config['jobs_per_host'],checkpoint_rows=config['checkpoint_rows'],input_verification_s=input_verification_s,construction_s=construction_s,publication_s=publication_s,through_publication_s=time.monotonic()-started,index_build=report,pointer=pointer,fully_converged=True,balance_warnings={},scope='Construction includes host launch/control, local writer pools and bounded log collection. Private periodic flushes precede the final audited immutable snapshot; publication prep and CAS are distinct. No cleanup or automatic retry.')
        (case/'case.json').write_bytes(canonical_bytes(result));journal(log,'complete',report=result);return result
    except BaseException as error:
        journal(log,'failed',error=str(error),retry_policy='Preserve this case and use fresh owned outputs for any retry.');raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('prepared','config','case'):p.add_argument('--'+name,required=True)
    a=p.parse_args();print(json.dumps(construct(a.prepared,a.config,a.case)))
if __name__=='__main__':main()
