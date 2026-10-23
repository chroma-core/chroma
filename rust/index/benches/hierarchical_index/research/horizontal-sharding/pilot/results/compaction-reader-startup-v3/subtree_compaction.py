"""Append a frozen ownership preparation while monitoring the same stores' old readers.

Inputs and fresh checkpoint clones are staged before this command.
The configuration contains binary, measured_binary, cpu_affinity, jobs_per_host,
and entries keyed by subtree ID. Each entry supplies host, index, vectors, and
id_map; the last two name pre-staged delta vectors and the full target map.
Optional checkpoint_rows bounds each private checkpoint; the default flushes
once per owner delta. The generation is fixed to two for this experiment. This is a
correctness monitor and compaction timer, not a serving throughput benchmark.
A failed case is terminal: retry from a fresh verified generation-one clone.
"""
import argparse
import concurrent.futures
import hashlib
import json
import mmap
import os
from pathlib import Path
import shlex
import threading
import time
import uuid
from .build import run_jobs
from .server import Reader
from .subtree_pilot import ssh
from .subtree_orchestrate import inspect_host, pull_metadata, publish_checked
from .subtree_transport import TreeCoordinator, load_model
from .tree_manifest import (assemble_tree, canonical_bytes, checked_artifact,
                            content_hash, load_tree, publish)


def journal(path, phase, **fields):
    with Path(path).open('a') as stream:
        stream.write(json.dumps(dict(time_unix=time.time(), phase=phase, **fields), allow_nan=False)+'\n')
        stream.flush()
        os.fsync(stream.fileno())


def preflight_owner(entry, slot, old_slot, generation):
    """Run remotely before any writer; reject wrong parents and partial attempts."""
    from .tree_manifest import file_hash
    directory = Path(entry['index'])
    head = directory/'manifest.json'
    old_path = directory/('manifest-'+str(old_slot['generation'])+'.json')
    expected = old_slot['snapshot']['sha256']
    if file_hash(head) != expected or file_hash(old_path) != expected:
        raise ValueError('current head is not the intended immutable parent')
    if (directory/'build.lock').exists():
        raise ValueError('writer lock exists')
    for name in ('manifest-'+str(generation)+'.json', 'id-map-'+str(generation)+'.u64le', 'manifest.json.tmp'):
        if (directory/name).exists():
            raise ValueError('partial target generation exists; use a fresh verified clone')
    manifest = json.loads(old_path.read_text())
    mapping = manifest['id_mapping']
    if Path(mapping['path']).name != mapping['path']:
        raise ValueError('parent map must be an owned filename')
    old_map = directory/mapping['path']
    if file_hash(old_map) != old_slot['local_to_global']['sha256'] or mapping['sha256'] != old_slot['local_to_global']['sha256']:
        raise ValueError('immutable parent mapping differs')
    if manifest['vector_count'] != slot['local_start'] or mapping['count'] != slot['local_start']:
        raise ValueError('append local start differs from parent count')
    if slot['vector_count'] != slot['local_start'] + slot['append_row_count']:
        raise ValueError('target owner count differs from old plus appended rows')
    if slot['routing_boundary_hash'] != manifest['routing_boundary_hash']:
        raise ValueError('frozen boundary changed')
    if slot['append_row_count']:
        target = Path(entry['id_map'])
        vectors = Path(entry['vectors'])
        if file_hash(target) != slot['local_to_global']['sha256'] or target.stat().st_size != 8*slot['vector_count']:
            raise ValueError('staged target map differs')
        with old_map.open('rb') as a, target.open('rb') as b:
            for block in iter(lambda:a.read(1024*1024), b''):
                if b.read(len(block)) != block:
                    raise ValueError('old map prefix changed')
        if file_hash(vectors) != slot['vectors']['sha256'] or vectors.stat().st_size != slot['vectors']['bytes']:
            raise ValueError('staged delta vectors differ')
    return dict(subtree_id=slot['subtree_id'], parent_sha256=expected,
                old_map_sha256=mapping['sha256'], old_count=manifest['vector_count'])


def validate_preparation(old, prepared, base):
    for key in ('construction_epoch', 'dimension', 'metric', 'upper_root'):
        if old[key] != prepared[key]:
            raise ValueError('prepared ownership boundary changed: '+key)
    if old['source']['source_id'] != prepared['source_id'] or old['source']['row_count'] != prepared['start_id']:
        raise ValueError('delta does not extend the canonical source prefix')
    if old['model']['sha256'] != prepared['model']['sha256']:
        raise ValueError('routing model changed')
    checked_artifact(prepared['model'], base)
    previous = {s['subtree_id']:s for s in old['subtrees']}
    if len(prepared['subtrees']) != len(previous) or {s['subtree_id'] for s in prepared['subtrees']} != set(previous):
        raise ValueError('owner slots differ')
    total = prepared['global_row_count']
    if total != prepared['start_id'] + prepared['input_row_count']:
        raise ValueError('global target count differs')
    # Disk-backed bitmap bounds memory even when maps grow beyond the pilot.
    import tempfile
    import struct
    seen = appended = 0
    with tempfile.TemporaryFile() as temporary:
        temporary.truncate((total+7)//8)
        with mmap.mmap(temporary.fileno(), 0) as bitmap:
            for slot in prepared['subtrees']:
                parent = previous[slot['subtree_id']]
                if slot['local_start'] != parent['vector_count'] or slot['vector_count'] != slot['local_start']+slot['append_row_count']:
                    raise ValueError('owner target count differs')
                if slot['centroid_index'] != parent['centroid_index'] or slot['routing_boundary_hash'] != parent['routing_boundary_hash']:
                    raise ValueError('owner boundary changed')
                mapping = checked_artifact(slot['local_to_global'], base)
                vectors = checked_artifact(slot['vectors'], base)
                if vectors.stat().st_size != slot['append_row_count']*prepared['dimension']*4 or mapping.stat().st_size != slot['vector_count']*8:
                    raise ValueError('prepared artifact row counts differ')
                with mapping.open('rb') as stream:
                    local = 0
                    for block in iter(lambda:stream.read(8*65536), b''):
                        for (ident,) in struct.iter_unpack('<Q', block):
                            if ident >= total or (local >= slot['local_start'] and ident < prepared['start_id']):
                                raise ValueError('mapped ID outside canonical old/delta range')
                            byte, bit = divmod(ident, 8)
                            if bitmap[byte] & (1 << bit):
                                raise ValueError('duplicate global ownership')
                            bitmap[byte] |= 1 << bit
                            seen += 1
                            local += 1
                appended += slot['append_row_count']
    if seen != total or appended != prepared['input_row_count']:
        raise ValueError('incomplete canonical coverage')


class RemoteBackend:
    """Persistent pinned readers, each confined to its writer's CPU allocation."""
    def __init__(self, tree, config):
        self.slots = {s['subtree_id']:s for s in tree['subtrees']}
        self.readers = {}; self.startup_responses = {}; cancelled = threading.Event()
        def launch(sid):
            if cancelled.is_set():raise RuntimeError('reader startup cancelled after peer failure')
            entry = config['entries'][sid]; slot = self.slots[sid]
            command = ['env', 'TOKIO_WORKER_THREADS=2', 'taskset', '-c', config['cpu_affinity'], config['binary'],
                       'read', '--checkpoint-dir', entry['index'], '--generation', str(slot['generation'])]
            if entry['host'] != 'local':
                command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ControlMaster=no', '-o', 'ControlPath=none', entry['host'], shlex.join(command)]
            reader = Reader(command, 30)
            try:
                request_id = str(uuid.uuid4())
                response = reader.query(dict(op='describe_root',request_id=request_id,generation=slot['generation']))
                if response.get('error') or response.get('request_id')!=request_id or response.get('generation')!=slot['generation'] or response.get('shard_id')!=sid:
                    raise ValueError('reader startup returned another immutable identity')
                root=slot['root_ref'];actual=response.get('root',{})
                if actual.get('node_id')!=root['id'] or actual.get('is_leaf')!=(root['kind']=='leaf') or response.get('vector_count')!=slot['vector_count'] or response.get('id_mapping',{}).get('sha256')!=slot['local_to_global']['sha256']:
                    raise ValueError('reader startup root/map/count differs from pinned snapshot')
                if cancelled.is_set():raise RuntimeError('reader startup cancelled after peer failure')
                return sid,reader,response
            except BaseException:
                reader.close()
                raise
        failure = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(launch,sid) for sid in self.slots]
            for future in concurrent.futures.as_completed(futures):
                if future.cancelled():continue
                try:
                    sid,reader,response = future.result()
                    self.readers[sid]=reader;self.startup_responses[sid]=response
                except BaseException as error:
                    if failure is None:failure=error
                    cancelled.set()
                    for pending in futures:pending.cancel()
        if failure is not None:
            self.close()
            raise failure

    def call_many(self, items, tree_hash, timeout):
        started = time.monotonic()
        rows = []
        for item in items:
            sid = item['subtree_id']; slot = self.slots[sid]
            if item['snapshot_sha256'] != slot['snapshot']['sha256']:
                raise ValueError('monitor snapshot mismatch')
            request = dict(item['payload'], generation=slot['generation'], request_id=str(uuid.uuid4()))
            result = self.readers[sid].query(request, timeout=max(.001, timeout-(time.monotonic()-started)))
            if result.get('error') or result.get('request_id') != request['request_id'] or result.get('generation') != slot['generation'] or result.get('shard_id') != sid:
                raise ValueError('old pinned reader failed or changed identity')
            rows.append(dict(subtree_id=sid, snapshot_sha256=slot['snapshot']['sha256'], result=result))
        return rows, []

    def close(self):
        for reader in self.readers.values():
            reader.close()


def trace_signature(row):
    # Timing and transport observations can vary; search decisions must not.
    return {k:row[k] for k in ('tree_sha256', 'generation', 'ids', 'distances', 'levels', 'selected_candidates', 'reached_leaves', 'global_candidate_count') if k in row}


def monitored_publication(coordinator, queries, build, publication, log, *, interval=.1):
    baseline = [trace_signature(coordinator.query(**q)) for q in queries]
    stop = threading.Event(); ready = threading.Event(); errors = []; count = [0]; observations = []
    def check(index):
        started = time.time()
        actual = trace_signature(coordinator.query(**queries[index]))
        finished = time.time()
        if actual != baseline[index]:
            raise AssertionError('old immutable reader search trace changed')
        journal(log, 'old-reader', query_index=index, started_unix=started, finished_unix=finished, signature=actual)
        observations.append((started,finished))
        count[0] += 1
    def monitor():
        while not stop.is_set():
            try:
                for index in range(len(queries)):
                    check(index)
                ready.set()
            except BaseException as error:
                errors.append(str(error)); stop.set(); ready.set(); break
            stop.wait(interval)
    thread = threading.Thread(target=monitor); thread.start()
    try:
        if not ready.wait(35):
            raise TimeoutError('old-reader monitor did not complete its initial query pass')
        if errors:
            raise AssertionError(errors[0])
        result = build()
        if errors:
            raise AssertionError(errors[0])
        if isinstance(result,dict) and 'writer_started_unix' in result:
            assert_mutation_overlap(observations,result['writer_started_unix'],result['writer_finished_unix'])
        pointer = publication(result)
        for index in range(len(queries)):
            check(index)
        if errors:
            raise AssertionError(errors[0])
        return result, pointer, count[0]
    finally:
        stop.set(); thread.join(35)
        if thread.is_alive():
            raise RuntimeError('old-reader monitor did not stop')


def assert_mutation_overlap(observations, started, finished):
    # Require both a request admitted during mutation and a response completed
    # during mutation; before/after-only observations cannot pass this gate.
    if not any(started <= a <= finished for a,b in observations) or not any(started <= b <= finished for a,b in observations):
        raise AssertionError('old-reader monitor has no verified requests during writer mutation')


def compact(old_case_path, prepared_path, config_path, case_path, query_path):
    old_case_path, prepared_path = Path(old_case_path), Path(prepared_path)
    old_case = json.loads(old_case_path.read_text()); old_path = Path(old_case['tree'])
    old = load_tree(old_path); prepared = json.loads(prepared_path.read_text()); config = json.loads(Path(config_path).read_text())
    case = Path(case_path); case.mkdir(parents=True, exist_ok=False); log = case/'journal.jsonl'
    try:
        validate_preparation(old, prepared, prepared_path.parent)
        generation = config.get('generation', 2)
        if old['tree_generation'] != 1 or generation != 2 or set(config['entries']) != {s['subtree_id'] for s in old['subtrees']}:
            raise ValueError('this experiment requires exact generation-one clones and generation two')
        previous = {s['subtree_id']:s for s in old['subtrees']}; jobs = []; entries = {}
        for slot in prepared['subtrees']:
            sid = slot['subtree_id']; entry = dict(config['entries'][sid]); entry.update(subtree_id=sid, vector_count=slot['vector_count'], generation=2 if slot['append_row_count'] else 1)
            # Mutating the original benchmark store would invalidate the paired arms.
            if entry['host'] == old_case['entries'][sid]['host'] and entry['index'] == old_case['entries'][sid]['index']:
                raise ValueError('append requires a fresh clone, not the original store')
            code = 'from tools.hierarchical_sharding.subtree_compaction import preflight_owner\nimport json\nprint(json.dumps(preflight_owner('+repr(entry)+','+repr(slot)+','+repr(previous[sid])+',2)))\n'
            journal(log, 'preflight', result=json.loads(ssh(entry['host'], 'import sys\nsys.path.insert(0,"/mnt/spann/source")\n'+code)))
            entries[sid] = entry
            if slot['append_row_count']:
                jobs.append(dict(host=entry['host'], binary=config['measured_binary'], vectors=entry['vectors'], id_map=entry['id_map'], checkpoint_dir=entry['index'], shard_id=sid, source_id=prepared['source_id'], dimension=prepared['dimension'], row_count=slot['append_row_count'], global_id_start=slot['local_start'], id_stride=1, generation=2, metric=prepared['metric'], threads=1, runtime_threads=2, cpu_affinity=config['cpu_affinity'], checkpoint_rows=min(config.get('checkpoint_rows',slot['append_row_count']),slot['append_row_count']), boundary_hash=slot['routing_boundary_hash'], resume=True))
        (case/'jobs.json').write_bytes(canonical_bytes(jobs))
        # The old tree uses absolute artifact references so this case owns its CAS pointer.
        old_local = json.loads(json.dumps(old))
        for descriptor in [old_local['model']] + [s[key] for s in old_local['subtrees'] for key in ('snapshot','local_to_global')]:
            descriptor['path'] = str(checked_artifact(descriptor, old_path.parent).resolve())
        publish(case/'current.json', old_local, base=case)
        queries = [json.loads(line) for line in Path(query_path).read_text().splitlines() if line.strip()]
        queries = [dict(query=q.get('vector',q.get('query')), k=q.get('k',10), beam_width=q.get('beam_width',64), candidate_budget=q.get('candidate_budget',1000)) for q in queries]
        if not queries:
            raise ValueError('old-reader query corpus is empty')
        mappings = {}
        for slot in old['subtrees']:
            file = checked_artifact(slot['local_to_global'],old_path.parent).open('rb')
            mappings[slot['subtree_id']] = (file,mmap.mmap(file.fileno(),0,access=mmap.ACCESS_READ))
        coordinator = TreeCoordinator(old, load_model(old,old_path.parent), RemoteBackend(old,config), mappings)
        try:
            (case/'old-reader-startup.json').write_bytes(canonical_bytes(coordinator.backend.startup_responses))
            coordinator.verify_roots()
            def build():
                started = time.monotonic(); writer_started = time.time(); journal(log,'writers-start')
                report = run_jobs(jobs,case/'build',max_hosts=len({j['host'] for j in jobs}),jobs_per_host=config.get('jobs_per_host',16),timeout=config.get('timeout',1800))
                writer_finished = time.time(); construction_s = time.monotonic()-started
                journal(log,'writers-finish',elapsed_s=construction_s,report=report)
                if not report['complete']:
                    raise RuntimeError('append task graph incomplete')
                if any('without full convergence' in p.read_text() for p in (case/'build').glob('*.stderr')):
                    raise RuntimeError('append balancing did not converge')
                inspections = []
                for host in sorted({e['host'] for e in entries.values()}):
                    rows = inspect_host(host,[e for e in entries.values() if e['host']==host],config['binary'],prepared)
                    pull_metadata(host,rows,case/'copies'); inspections.extend(rows)
                tree = assemble_tree(prepared_path,[case/'copies'/s['subtree_id']/('manifest-'+str(entries[s['subtree_id']]['generation'])+'.json') for s in prepared['subtrees']],2,case)
                (case/'tree-2.json').write_bytes(canonical_bytes(tree)); (case/'remote-reopen.json').write_bytes(canonical_bytes(inspections))
                return dict(tree=tree,inspections=inspections,index_build=report,writer_started_unix=writer_started,writer_finished_unix=writer_finished,construction_s=construction_s,through_publication_started_monotonic=started)
            def publication(result):
                started = time.monotonic()
                pointer = publish_checked(case/'current.json',result['tree'],result['inspections'],case,expected_old_hash=content_hash(old_local))
                result['publication_s'] = time.monotonic()-started
                result['through_publication_s'] = time.monotonic()-result['through_publication_started_monotonic']
                journal(log,'published',elapsed_s=result['publication_s'],through_publication_s=result['through_publication_s'],tree_sha256=content_hash(result['tree']))
                return pointer
            result,pointer,count = monitored_publication(coordinator,queries,build,publication,log)
            for host in sorted({e['host'] for e in entries.values()}):
                old_entries = [dict(e,generation=previous[sid]['generation'],vector_count=previous[sid]['vector_count']) for sid,e in entries.items() if e['host']==host]
                reopened = inspect_host(host,old_entries,config['binary'],prepared)
                for row in reopened:
                    parent = previous[row['subtree_id']]
                    if row['snapshot_sha256'] != parent['snapshot']['sha256'] or row['mapping_sha256'] != parent['local_to_global']['sha256']:
                        raise ValueError('old immutable snapshot or map changed after append')
                journal(log,'old-generation-reopened',host=host,owners=len(reopened))
        finally:
            coordinator.close()
        report = dict(status='passed',tree=str((case/'tree-2.json').resolve()),tree_sha256=content_hash(result['tree']),entries=entries,binary=config['binary'],index_build=result['index_build'],pointer=pointer,construction_s=result['construction_s'],publication_s=result['publication_s'],through_publication_s=result['through_publication_s'],old_reader_checks=count,reader_ssh_policy='ControlMaster=no; ControlPath=none; at most eight concurrent launches.',monitor='Persistent SSH readers on the same cloned stores; correctness only, not serving throughput.',generation=2)
        (case/'case.json').write_bytes(canonical_bytes(report)); journal(log,'complete',report=report)
        return report
    except BaseException as error:
        journal(log,'failed',error=str(error),retry_policy='Never reuse this case; stage a fresh clone from the verified generation-one snapshot.')
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('old-case','prepared','config','case','queries'):
        parser.add_argument('--'+name,required=True)
    args = parser.parse_args()
    print(json.dumps(compact(args.old_case,args.prepared,args.config,args.case,args.queries)))

if __name__ == '__main__':
    main()
