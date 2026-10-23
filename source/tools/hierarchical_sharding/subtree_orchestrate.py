"""Build pre-staged owner inputs and publish metadata after pinned remote reopen checks.

Large indexes stay on worker disks. The driver retains only manifests, explicit
ID maps, the shared model, and run journals. Input delivery precedes this module.
"""
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import tarfile
import time
import uuid
from .build import run_jobs
from .subtree_pilot import ssh
from .tree_manifest import assemble_tree, canonical_bytes, content_hash, publish


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def validate_binding(manifest, mapping_hash):
    """Reject stale coverage proofs, even when a worker exits successfully."""
    proof = manifest.get('integrity_audit') or {}
    coverage = proof.get('coverage') or {}
    if proof.get('generation') != manifest['generation'] or proof.get('block_ids') != manifest['block_ids']:
        raise ValueError('coverage proof references another generation or block set')
    if mapping_hash != manifest['id_mapping']['sha256'] or proof.get('id_map_sha256') != mapping_hash:
        raise ValueError('coverage proof references another explicit ID map')
    if coverage.get('expected_rows') != manifest['vector_count'] or coverage.get('unique_live_rows') != manifest['vector_count']:
        raise ValueError('coverage proof does not cover every committed row')
    return proof


def remote_inspect(entries, binary, dimension, metric, source_id):
    """Execute on a worker; reopen the exact immutable snapshot and return bindings."""
    output = []
    for entry in entries:
        directory = Path(entry['index'])
        path = directory / ('manifest-' + str(entry['generation']) + '.json')
        manifest_hash = digest(path)
        manifest = json.loads(path.read_text())
        if manifest['generation'] != entry['generation'] or manifest['shard_id'] != entry['subtree_id']:
            raise ValueError('committed owner identity differs from the requested snapshot')
        if manifest['source_id'] != source_id or manifest['dimension'] != dimension or manifest['metric'] != metric:
            raise ValueError('committed source or metric differs from the requested tree')
        if manifest['vector_count'] != entry['vector_count']:
            raise ValueError('committed owner count differs from prepared ownership')
        mapping = Path(manifest['id_mapping']['path'])
        mapping = mapping if mapping.is_absolute() else directory / mapping
        mapping_hash = digest(mapping)
        if mapping.stat().st_size != manifest['vector_count'] * 8:
            raise ValueError('explicit ID map does not cover every local row')
        validate_binding(manifest, mapping_hash)
        request_id = str(uuid.uuid4())
        request = dict(op='describe_root', request_id=request_id, generation=entry['generation'])
        process = subprocess.run([binary, 'read', '--checkpoint-dir', str(directory), '--generation', str(entry['generation'])], input=json.dumps(request)+'\n', text=True, capture_output=True, timeout=60, check=True, env=dict(os.environ,TOKIO_WORKER_THREADS='2'))
        responses = [json.loads(line) for line in process.stdout.splitlines() if line.strip()]
        if len(responses) != 1:
            raise ValueError('pinned reopen returns an incomplete response')
        result = responses[0]
        if result.get('error') or result.get('request_id') != request_id or result.get('generation') != entry['generation'] or result.get('shard_id') != entry['subtree_id']:
            raise ValueError('pinned reopen response identity differs')
        root = manifest['root_ref']
        if result.get('root',{}).get('node_id') != root['id'] or result.get('root',{}).get('is_leaf') != (root['kind']=='leaf'):
            raise ValueError('opened root differs from the committed root')
        for field in ('dimension', 'metric', 'vector_count'):
            if result.get(field) != manifest[field]:
                raise ValueError('opened owner metadata differs: ' + field)
        if digest(path) != manifest_hash or digest(mapping) != mapping_hash:
            raise ValueError('immutable snapshot or map changes during reopen')
        output.append(dict(subtree_id=entry['subtree_id'], generation=entry['generation'], snapshot_sha256=manifest_hash, mapping_sha256=mapping_hash, manifest=manifest, root_response=result,
                           metadata_files=[dict(source=str(path),name=entry['subtree_id']+'/'+path.name,sha256=manifest_hash,bytes=path.stat().st_size),dict(source=str(mapping),name=entry['subtree_id']+'/'+mapping.name,sha256=mapping_hash,bytes=mapping.stat().st_size)]))
    return output


def inspect_host(host, entries, binary, prepared):
    code = 'import json\nfrom tools.hierarchical_sharding.subtree_orchestrate import remote_inspect\nprint(json.dumps(remote_inspect('+repr(entries)+','+repr(binary)+','+repr(prepared['dimension'])+','+repr(prepared['metric'])+','+repr(prepared['source_id'])+')))\n'
    # The worker source is explicitly deployed before the run.
    code = 'import sys\nsys.path.insert(0,"/mnt/spann/source")\n' + code
    return json.loads(ssh(host, code))


def pull_metadata(host, inspections, destination):
    files = [item for row in inspections for item in row['metadata_files']]
    expected = {item['name']: item for item in files}
    code = 'import sys,tarfile\nwith tarfile.open(fileobj=sys.stdout.buffer,mode="w|") as tar:\n for row in '+repr(files)+':tar.add(row["source"],arcname=row["name"],recursive=False)\n'
    process = subprocess.Popen(['ssh','-o','BatchMode=yes',host,'python3','-c',shlex.quote(code)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    destination = Path(destination);destination.mkdir(parents=True,exist_ok=True);received=set()
    try:
        with tarfile.open(fileobj=process.stdout,mode='r|') as archive:
            for member in archive:
                if member.name not in expected or not member.isfile() or member.size != expected[member.name]['bytes']:
                    raise ValueError('unexpected remote metadata archive entry')
                relative=Path(member.name)
                if relative.is_absolute() or '..' in relative.parts:
                    raise ValueError('unsafe metadata archive path')
                target=destination/relative;target.parent.mkdir(parents=True,exist_ok=True)
                source=archive.extractfile(member)
                with target.open('xb') as output:
                    while True:
                        block=source.read(8*1024*1024)
                        if not block:break
                        output.write(block)
                if digest(target)!=expected[member.name]['sha256']:
                    raise ValueError('transferred metadata digest differs')
                received.add(member.name)
        if process.wait(timeout=60) or received!=set(expected):
            raise ValueError('remote metadata transfer is incomplete')
    finally:
        if process.poll() is None:process.kill();process.wait()


def publication_gate(tree, inspections):
    expected={slot['subtree_id']:slot for slot in tree['subtrees']}
    if len(inspections)!=len(expected) or {row['subtree_id'] for row in inspections}!=set(expected):
        raise ValueError('remote verification does not cover the exact tree')
    for row in inspections:
        slot=expected[row['subtree_id']]
        if row['generation']!=slot['generation'] or row['snapshot_sha256']!=slot['snapshot']['sha256'] or row['mapping_sha256']!=slot['local_to_global']['sha256']:
            raise ValueError('remote verification references another snapshot or map')
        manifest=row['manifest'];result=row['root_response']
        if manifest['generation']!=slot['generation'] or manifest['vector_count']!=slot['vector_count'] or manifest['root_ref']!=slot['root_ref']:
            raise ValueError('remote committed root or count differs from the tree')
        if manifest['dimension']!=tree['dimension'] or manifest['metric']!=tree['metric'] or manifest['source_id']!=tree['source']['source_id']:
            raise ValueError('remote committed source differs from the tree')
        if result.get('generation')!=slot['generation'] or result.get('shard_id')!=slot['subtree_id'] or result.get('root',{}).get('node_id')!=slot['root_ref']['id'] or result.get('root',{}).get('is_leaf')!=(slot['root_ref']['kind']=='leaf'):
            raise ValueError('pinned reader response differs from the tree')
        if result.get('id_mapping',{}).get('sha256')!=row['mapping_sha256']:
            raise ValueError('pinned reader opens another ID map')
        validate_binding(manifest,row['mapping_sha256'])


def publish_checked(pointer, tree, inspections, base, expected_old_hash=None):
    publication_gate(tree, inspections)
    return publish(pointer, tree, expected_old_hash=expected_old_hash, base=base)


def construct(prepared_path, case_path, hosts, staged_root, *, binary, measured_binary, cpu_affinity, jobs_per_host, threads=1, runtime_threads=2):
    started=time.monotonic();prepared_path=Path(prepared_path);prepared=json.loads(prepared_path.read_text());case=Path(case_path);case.mkdir(parents=True,exist_ok=False)
    loads={host:0 for host in hosts};entries={};jobs=[]
    for slot in sorted(prepared['subtrees'],key=lambda row:(-row['vector_count'],row['subtree_id'])):
        sid=slot['subtree_id']
        if not sid or '/' in sid or sid in ('.','..'):raise ValueError('unsafe subtree ID')
        host=min(hosts,key=lambda h:(loads[h],hosts.index(h)));loads[host]+=slot['vector_count']
        index='/mnt/spann/'+case.name+'/indexes/'+sid
        entries[sid]=dict(host=host,index=index,generation=1,subtree_id=sid,vector_count=slot['vector_count'],slot=slot)
        paths={}
        for field in ('vectors','local_to_global'):
            relative=Path(slot[field]['path'])
            if relative.is_absolute() or '..' in relative.parts:raise ValueError('staged artifact must be relative to its prepared root')
            paths[field]=str(Path(staged_root)/relative)
        jobs.append(dict(host=host,binary=measured_binary,vectors=paths['vectors'],checkpoint_dir=index,shard_id=sid,source_id=prepared['source_id'],dimension=prepared['dimension'],row_count=slot['vector_count'],global_id_start=0,id_stride=1,generation=1,metric=prepared['metric'],threads=threads,runtime_threads=runtime_threads,cpu_affinity=cpu_affinity,checkpoint_rows=slot['vector_count'],id_map=paths['local_to_global'],boundary_hash=slot['routing_boundary_hash']))
    verify_started=time.monotonic()
    for host in hosts:
        files=[dict(remote=job[field],sha256=entries[job['shard_id']]['slot'][artifact]['sha256'],bytes=entries[job['shard_id']]['slot'][artifact]['bytes']) for job in jobs if job['host']==host for field,artifact in (('vectors','vectors'),('id_map','local_to_global'))]
        code='import hashlib,pathlib\nfor row in '+repr(files)+':\n p=pathlib.Path(row["remote"]);assert p.stat().st_size==row["bytes"]\n with p.open("rb") as stream:assert hashlib.file_digest(stream,"sha256").hexdigest()==row["sha256"]\n'
        ssh(host,code)
    input_verification_s=time.monotonic()-verify_started
    (case/'jobs.json').write_text(json.dumps(jobs,indent=2));build_started=time.time();report=run_jobs(jobs,case/'build',max_hosts=len(hosts),timeout=3600,jobs_per_host=jobs_per_host)
    warnings={p.name:p.read_text() for p in (case/'build').glob('*.stderr') if 'without full convergence' in p.read_text()}
    outcome=dict(complete=report['complete'],fully_converged=not warnings,report=report,balance_warnings=warnings)
    (case/'build-outcome.json').write_text(json.dumps(outcome,indent=2))
    if not report['complete'] or warnings:raise RuntimeError('construction does not pass completion and convergence gates')
    inspected_started=time.monotonic();inspections=[]
    failures=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(hosts)) as pool:
        futures={pool.submit(inspect_host,host,[row for row in entries.values() if row['host']==host],binary,prepared):host for host in hosts}
        for number,future in enumerate(concurrent.futures.as_completed(futures)):
            host=futures[future]
            try:
                rows=future.result()
                (case/('remote-host-'+str(number)+'.json')).write_text(json.dumps(dict(host=host,status='reopened',inspections=rows),indent=2))
                pull_metadata(host,rows,case/'copies');inspections.extend(rows)
            except Exception as error:
                failures.append(dict(host=host,error=str(error)))
                (case/('remote-host-'+str(number)+'-failure.json')).write_text(json.dumps(failures[-1],indent=2))
    if failures:
        (case/'publication-failure.json').write_text(json.dumps(dict(status='failed',phase='remote reopen and metadata transfer',failures=failures,verified_owners=len(inspections)),indent=2))
        raise RuntimeError('remote verification failed; no global publication attempted')
    paths=[case/'copies'/slot['subtree_id']/'manifest-1.json' for slot in prepared['subtrees']]
    tree=assemble_tree(prepared_path,paths,1,case);tree_path=case/'tree-1.json';tree_path.write_bytes(canonical_bytes(tree));pointer=publish_checked(case/'current.json',tree,inspections,case)
    (case/'remote-reopen.json').write_text(json.dumps(inspections,indent=2))
    result=dict(host_order=hosts,placement_policy='Largest vector_count first; lexical subtree-ID ties; least assigned rows; host-order ties.',assigned_rows=loads,cpu_affinity=cpu_affinity,jobs_per_host=jobs_per_host,threads=threads,runtime_threads=runtime_threads,case=str(case),prepared=str(prepared_path),tree=str(tree_path),tree_sha256=content_hash(tree),binary=binary,measured_binary=measured_binary,entries=entries,index_build=report,index_build_started_unix=build_started,fully_converged=True,balance_warnings={},input_verification_s=input_verification_s,remote_reopen_and_metadata_s=time.monotonic()-inspected_started,through_publication_s=time.monotonic()-started,local_reference_available=False,pointer=pointer)
    (case/'case.json').write_text(json.dumps(result,indent=2));return result


def checkpoint_inventory(directory, generation, expected_hash):
    directory=Path(directory)
    if digest(directory/('manifest-'+str(generation)+'.json'))!=expected_hash:
        raise ValueError('transfer source is another immutable snapshot')
    files=[]
    for path in sorted(directory.rglob('*')):
        if path.is_symlink():raise ValueError('checkpoint transfer excludes symlinks')
        if path.is_file():files.append(dict(path=str(path.relative_to(directory)),bytes=path.stat().st_size,sha256=digest(path)))
    return files


def transfer_checkpoint(source_host, source, destination_host, destination, generation, expected_hash):
    started=time.monotonic()
    code='import sys,json\nsys.path.insert(0,"/mnt/spann/source")\nfrom tools.hierarchical_sharding.subtree_orchestrate import checkpoint_inventory\nprint(json.dumps(checkpoint_inventory('+repr(source)+','+repr(generation)+','+repr(expected_hash)+')))\n'
    files=json.loads(ssh(source_host,code));inventory_s=time.monotonic()-started
    temporary=destination+'.partial-'+uuid.uuid4().hex
    ssh(destination_host,'from pathlib import Path\np=Path('+repr(destination)+');assert not p.exists();p.parent.mkdir(parents=True,exist_ok=True)\n')
    copy_started=time.monotonic()
    subprocess.run(['scp','-3','-q','-r',source_host+':'+source,destination_host+':'+temporary],check=True,timeout=7200)
    copy_s=time.monotonic()-copy_started;verify_started=time.monotonic()
    code='from pathlib import Path\nimport hashlib,os\nroot=Path('+repr(temporary)+')\nexpected='+repr(files)+'\nactual={str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()}\nassert actual=={row["path"] for row in expected}\nfor row in expected:\n p=root/row["path"];assert p.stat().st_size==row["bytes"]\n with p.open("rb") as stream:assert hashlib.file_digest(stream,"sha256").hexdigest()==row["sha256"]\nassert not Path('+repr(destination)+').exists()\nos.rename(root,'+repr(destination)+')\n'
    ssh(destination_host,code)
    return dict(source_host=source_host,source=source,destination_host=destination_host,destination=destination,snapshot_sha256=expected_hash,generation=generation,bytes=sum(row['bytes'] for row in files),inventory_s=inventory_s,copy_s=copy_s,target_verification_s=time.monotonic()-verify_started,total_s=time.monotonic()-started,transport='SCP worker-to-worker through driver relay; no driver checkpoint file',files=files)


def serve_case(case_path, port, *, cohost=False, cpu_affinity=None, adapters_per_host=1, read_binary=None, profile_reader=False, profile_memory=False, source_root='/mnt/spann/source', owner_candidate_reduction=False):
    from .subtree_transport import TreeCoordinator,TreeError
    case_path=Path(case_path);case=json.loads((case_path/'case.json').read_text());tree=json.loads(Path(case['tree']).read_text());first=case['host_order'][0]
    if adapters_per_host<1:raise ValueError('adapters_per_host must be positive')
    if adapters_per_host>1 and cpu_affinity:raise ValueError('Use fixed four-adapter CPU groups instead of one shared mask')
    if adapters_per_host not in (1,4):raise ValueError('Only declared one/four adapter policies are supported')
    assigned={};adapter_assignment={}
    for slot in sorted(tree['subtrees'],key=lambda slot:(-slot['vector_count'],slot['subtree_id'])):
        host=first if cohost else case['entries'][slot['subtree_id']]['host'];loads=assigned.setdefault(host,[0]*adapters_per_host);group=min(range(adapters_per_host),key=lambda g:(loads[g],g));loads[group]+=slot['vector_count'];adapter_assignment[slot['subtree_id']]=group
    groups={};transfers=[];placement=dict(format_version=1,tree_sha256=case['tree_sha256'],placement_epoch=2 if cohost else 1,owners={})
    for slot in tree['subtrees']:
        sid=slot['subtree_id'];entry=case['entries'][sid];host=first if cohost else entry['host'];index=entry['index']
        if cohost and host!=entry['host']:
            destination='/mnt/spann/'+case_path.name+'/cohost/'+sid
            try:
                transfers.append(transfer_checkpoint(entry['host'],index,host,destination,slot['generation'],slot['snapshot']['sha256']))
            except Exception as error:
                (case_path/'placement-transfer-failure.json').write_text(json.dumps(dict(subtree_id=sid,error=str(error),completed_transfers=transfers),indent=2));raise
            (case_path/'placement-transfers-progress.json').write_text(json.dumps(transfers,indent=2));index=destination
        group=adapter_assignment[sid];endpoint_port=port+group;owner='owner-'+host.split('@')[-1]+'-adapter-'+str(group)
        placement['owners'][sid]=dict(owner_id=owner,endpoint='http://'+host.split('@')[-1]+':'+str(endpoint_port))
        config=groups.setdefault((host,group),dict(tree_sha256=case['tree_sha256'],owner_id=owner,timeout=30,subtrees={}))
        if owner_candidate_reduction:config['owner_candidate_reduction']=True
        config['subtrees'][sid]=dict(manifest=index+'/manifest-'+str(slot['generation'])+'.json',checkpoint_dir=index,binary=read_binary or case['binary'],snapshot_sha256=slot['snapshot']['sha256'],runtime_threads=2,profile_reader=profile_reader,profile_memory=profile_memory)
    label='cohost' if cohost else 'two-hosts';placement_path=case_path/('placement-'+label+'.json');placement_path.write_text(json.dumps(placement,indent=2));(case_path/('transfers-'+label+'.json')).write_text(json.dumps(transfers,indent=2))
    servers=[]
    for (host,group),config in groups.items():
        endpoint_port=port+group
        allocation=','.join(str(v) for v in (2*group,2*group+1,32+2*group,33+2*group)) if adapters_per_host==4 else cpu_affinity
        directory='/mnt/spann/results/'+case_path.name;config_path=directory+'/host-'+str(endpoint_port)+'.json';log=directory+'/host-'+str(endpoint_port)+'.log';command=['python3','-m','tools.hierarchical_sharding.subtree_server','--config',config_path,'--host','0.0.0.0','--port',str(endpoint_port)]
        if allocation:command=['taskset','-c',allocation]+command
        code='import pathlib,json,subprocess\np=pathlib.Path('+repr(directory)+');p.mkdir(parents=True,exist_ok=True)\npathlib.Path('+repr(config_path)+').write_text('+repr(json.dumps(config))+')\nf=open('+repr(log)+',"w")\np=subprocess.Popen('+repr(command)+',cwd='+repr(source_root)+',stdout=f,stderr=f,start_new_session=True)\nprint(p.pid)\n'
        servers.append(dict(host=host,pid=int(ssh(host,code)),config=config_path,log=log,cpu_affinity=allocation,adapter_group=group,binary=read_binary or case['binary']))
    (case_path/('servers-'+label+'.json')).write_text(json.dumps(servers,indent=2));started=time.monotonic();deadline=started+120;coordinator=TreeCoordinator.from_files(case['tree'],placement_path)
    try:
        while True:
            try:verification=coordinator.verify_roots(timeout=min(60,max(.001,deadline-time.monotonic())));break
            except TreeError:
                if time.monotonic()>=deadline:raise
                time.sleep(.1)
    finally:coordinator.close()
    (case_path/('ready-'+label+'.json')).write_text(json.dumps(dict(status='passed',startup_s=time.monotonic()-started,verification=verification,cpu_affinity=cpu_affinity),indent=2));return placement_path
