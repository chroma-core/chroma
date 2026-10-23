"""Register global writes in private writer forks and publish their saved leaves.

The existing compactor launches one bounded local Rust task over private owner
clones. It commits the selected writer state directly, audits exact content and
leaf membership, and publishes every changed snapshot/map through one CAS.
"""
import copy
import json
import math
import os
import re
import time
from pathlib import Path
import shutil
import struct
import subprocess
from .dynamic_deployment import (validate_record, resolved_tree, verify_canonical_append,
                                 make_record, run_transaction)
from .dynamic_runtime import seal_owner, durable_directory, inventory
from .dynamic_topology import owned_ids
from .tree_manifest import (artifact, checked_artifact, load_model, content_hash,
                            canonical_bytes, _atomic_write, file_hash)

# Exact planner binaries require source/build review before publication use.
VERIFIED_WRITE_PLANNERS = {
    'd26ae940c9e0d918fde681e0fc7b9e2a6b16bcda0c2355bcdd534ccbd1a1cc12',
    # Independently admitted strict single-copy native epoch, global-write-v5.
    'ff46b44e63a37e67a90c10d17975263813471803176a6c47d14f83ad80edabfa',
}


def validate_plan(plan, tree, old_maps, parent_hash, append_sha, total):
    if (plan.get('status')!='saved-private-global-writes' or plan.get('durable') is not True
            or plan.get('parent_sha256')!=parent_hash or plan.get('append_sha256')!=append_sha):
        raise ValueError('write plan is not bound to this private transaction')
    owners=plan.get('owners');routes=plan.get('routes');old_count=tree['source']['row_count']
    if not isinstance(owners,list) or len(owners)!=len(old_maps) or not isinstance(routes,list) or len(routes)!=total-old_count:
        raise ValueError('write plan owner/route coverage differs')
    maps={};seen=set()
    for row in owners:
        if not isinstance(row,dict):raise ValueError('invalid write owner')
        sid=row.get('subtree_id');ids=row.get('ids')
        if sid not in old_maps or sid in maps or not isinstance(ids,list) or ids[:len(old_maps[sid])]!=old_maps[sid]:
            raise ValueError('write plan changes an existing owner prefix')
        for ident in ids:
            if type(ident) is not int or not 0<=ident<total or ident in seen:raise ValueError('duplicate, missing or invalid global write ownership')
            seen.add(ident)
        maps[sid]=ids
    if len(seen)!=total:raise ValueError('write plan does not cover complete target corpus')
    new_owners={ident:(sid,local) for sid,ids in maps.items() for local,ident in enumerate(ids) if local>=len(old_maps[sid])}
    for ident,route in enumerate(routes,old_count):
        if (not isinstance(route,dict) or type(route.get('id')) is not int or route['id']!=ident
                or type(route.get('local_id')) is not int or (route.get('subtree_id'),route['local_id'])!=new_owners.get(ident) or type(route.get('leaf_id')) is not int
                or not 0<=route['leaf_id']<=2**32-1 or type(route.get('distance')) not in (int,float) or not math.isfinite(route['distance'])):
            raise ValueError('route differs from sole new owner or has invalid leaf score')
    return maps


def prepare_global_append(planner_binary, reader_binary, parent, canonical_vectors, output,
                          timeout=60, max_rows=100000, max_bytes=512*1024**2):
    validate_record(parent)
    if file_hash(planner_binary) not in VERIFIED_WRITE_PLANNERS:
        raise ValueError('global write planner binary is not an admitted source epoch')
    tree=resolved_tree(parent)
    if any(not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,95}',s['subtree_id']) for s in tree['subtrees']):raise ValueError('safe logical owner names required')
    if any(owner.get('remote') for owner in parent['owners'].values()):
        raise ValueError('local integration cannot claim remote compactor execution')
    source=verify_canonical_append(parent['canonical_vectors'],artifact(canonical_vectors),tree['dimension'])
    row_bytes=tree['dimension']*4;total=source.stat().st_size//row_bytes;old_count=tree['source']['row_count']
    if (type(max_rows) is not int or not 0<max_rows<=100000 or total<=old_count or total>max_rows
            or source.stat().st_size>max_bytes):raise ValueError('bounded growing canonical source required')
    output=Path(output).resolve();output.mkdir(mode=0o700,parents=True,exist_ok=False)
    canonical=output/'canonical.f32';before=artifact(source)
    with source.open('rb') as src,canonical.open('xb') as dst:
        shutil.copyfileobj(src,dst,1024*1024);dst.flush();os.fsync(dst.fileno())
    if artifact(source)!=before or file_hash(canonical)!=before['sha256']:raise ValueError('canonical source changed while sealing')
    canonical.chmod(0o400)
    append=output/'append.f32'
    with canonical.open('rb') as src,append.open('xb') as dst:
        src.seek(old_count*row_bytes);shutil.copyfileobj(src,dst,1024*1024);dst.flush();os.fsync(dst.fileno())
    append.chmod(0o400);model=load_model(tree);old_maps={s['subtree_id']:list(owned_ids(s)) for s in tree['subtrees']}
    owners=[]
    for subtree in sorted(tree['subtrees'],key=lambda s:s['subtree_id']):
        sid=subtree['subtree_id']
        original=Path(subtree['snapshot']['path']).parent
        clone=output/('index-'+sid)
        rows=inventory(original)
        if sum(row['bytes'] for row in rows.values())>max_bytes:raise ValueError('parent clone exceeds declared byte admission')
        shutil.copytree(original,clone)
        if inventory(clone)!=rows:raise ValueError('private parent clone differs')
        _atomic_write(clone/'global-write-private.json',canonical_bytes(dict(parent_sha256=content_hash(parent),subtree_id=sid,manifest_sha256=subtree['snapshot']['sha256'])))
        owners.append(dict(subtree_id=sid,checkpoint_dir=str(clone),
                           generation=subtree['generation'],manifest_sha256=subtree['snapshot']['sha256'],
                           centroid=model['centroids'][subtree['centroid_index']]))
    spec=dict(parent_sha256=content_hash(parent),source_id=tree['source']['source_id'],dimension=tree['dimension'],
              metric=tree['metric'],old_count=old_count,canonical_vectors=str(canonical),canonical_sha256=file_hash(canonical),append_vectors=str(append),append_sha256=file_hash(append),
              owners=owners,max_rows=max_rows)
    spec_path=output/'write-plan-spec.json';_atomic_write(spec_path,canonical_bytes(spec));spec_path.chmod(0o400)
    spec_sha=file_hash(spec_path)
    from . import dynamic_runtime
    producer_sha=dynamic_runtime.PRODUCER_SOURCE_SHA256
    if dynamic_runtime.producer_bundle_sha256()!=producer_sha:raise ValueError('compactor producer changed before execution')
    planner_hash=file_hash(planner_binary)
    started=time.time()
    with (output/'write-plan.stdout').open('xb') as stdout,(output/'write-plan.stderr').open('xb') as stderr:
        argv=[str(planner_binary),'compact-global-writes','--spec',str(spec_path)]
        process=subprocess.Popen(argv,stdout=stdout,stderr=stderr,env=dict(os.environ,TOKIO_WORKER_THREADS='2'))
        try:returncode=process.wait(timeout=timeout)
        except BaseException:
            if process.poll() is None:process.kill();process.wait(timeout=5)
            raise
    _atomic_write(output/'write-plan-execution.json',canonical_bytes(dict(binary_sha256=planner_hash,argv=argv,pid=process.pid,returncode=returncode,started_unix=started,finished_unix=time.time(),parent_sha256=spec['parent_sha256'],spec=artifact(spec_path),reply=artifact(output/'write-plan.stdout'))))
    if returncode:raise ValueError('private Rust global write planning failed')
    if (file_hash(planner_binary)!=planner_hash or file_hash(append)!=spec['append_sha256'] or file_hash(spec_path)!=spec_sha
            or dynamic_runtime.producer_bundle_sha256()!=producer_sha):
        raise ValueError('executed write planner/input changed')
    if (output/'write-plan.stdout').stat().st_size>32*1024**2:raise ValueError('bounded write-plan reply exceeded')
    validate_record(parent)
    plan=json.loads((output/'write-plan.stdout').read_text());maps=validate_plan(plan,tree,old_maps,spec['parent_sha256'],spec['append_sha256'],total)
    updated=copy.deepcopy(tree);updated['tree_generation']+=1;updated['source']['row_count']=total
    placements=copy.deepcopy(parent['owners'])
    saved=plan.get('saved')
    if not isinstance(saved,list) or len({r.get('subtree_id') for r in saved})!=len(saved):raise ValueError('duplicate or missing saved owner receipt')
    saved={row['subtree_id']:row for row in saved}
    expected_changed={sid for sid,ids in maps.items() if ids!=old_maps[sid]}
    if set(saved)!=expected_changed:raise ValueError('saved snapshots differ from changed owner set')
    from . import dynamic_runtime
    from .dynamic_content import mapped_digest,PROOF_NAME
    from .dynamic_runtime import probe_reader
    for index,previous in enumerate(tree['subtrees']):
        sid=previous['subtree_id']
        if sid not in saved:continue
        directory=output/('index-'+sid);receipt=saved[sid]
        generation=previous['generation']+1;manifest=directory/f'manifest-{generation}.json'
        snapshot=json.loads(manifest.read_text());map_path=directory/snapshot['id_mapping']['path']
        if (receipt.get('parent_sha256')!=content_hash(parent) or receipt.get('parent_manifest_sha256')!=previous['snapshot']['sha256']
                or receipt.get('generation')!=generation or receipt.get('snapshot_sha256')!=file_hash(manifest)
                or receipt.get('exact_vector_rows_verified')!=len(maps[sid])
                or receipt.get('all_selected_leaf_memberships_verified') is not True):raise ValueError('saved execution receipt differs from pinned transaction')
        selected=[route for route in plan['routes'] if route['subtree_id']==sid]
        if receipt.get('selected_leaf_memberships')!=selected:raise ValueError('persisted selected-leaf proof differs from planned routes')
        if list(struct.iter_unpack('<Q',map_path.read_bytes()))!=[(ident,) for ident in maps[sid]]:raise ValueError('persisted map differs from selected ownership')
        subtree=dict(subtree_id=sid,generation=generation,vector_count=len(maps[sid]),snapshot=artifact(manifest),
                     local_to_global=artifact(map_path),root_ref=snapshot['root_ref'],routing_boundary_hash=previous['routing_boundary_hash'],centroid_index=previous['centroid_index'])
        subtree['ownership_revision']=content_hash(dict(source_id=tree['source']['source_id'],subtree_id=sid,map_sha256=subtree['local_to_global']['sha256']))
        # The actual worker reads these full canonical inputs and checks every
        # persisted vector bit and selected live leaf before its receipt exists.
        inputs={}
        for key,name in [('vectors','construction-input.f32'),('map','construction-input.u64le')]:
            desc=artifact(directory/name);inputs[key]=dict(file=name,bytes=desc['bytes'],sha256=desc['sha256']);(directory/name).chmod(0o400)
        shutil.copyfile(spec_path,directory/'write-plan-spec.json');(directory/'write-plan-spec.json').chmod(0o400)
        mapped_sha,count=mapped_digest(directory/'construction-input.f32',directory/'construction-input.u64le',tree['dimension'],tree['metric'])
        if count!=len(maps[sid]):raise ValueError('executed vector input coverage differs')
        files=inventory(directory)
        proof=dict(format_version=1,execution_kind='global-write-compaction',producer_source_sha256=dynamic_runtime.PRODUCER_SOURCE_SHA256,
                   binary_sha256=planner_hash,argv=argv,pid=process.pid,started_unix=started,finished_unix=time.time(),returncode=returncode,
                   inputs=inputs,mapped_normalized_sha256=mapped_sha,snapshot_sha256=subtree['snapshot']['sha256'],map_sha256=subtree['local_to_global']['sha256'],
                   dimension=tree['dimension'],metric=tree['metric'],output_inventory_sha256=content_hash(files),
                   compactor_spec=dict(file='write-plan-spec.json',bytes=spec_path.stat().st_size,sha256=file_hash(spec_path)))
        _atomic_write(directory/PROOF_NAME,canonical_bytes(proof))
        placements[sid]=seal_owner(directory,subtree,parent['owners'][sid]['worker_id'])
        probe_reader(reader_binary,sid,placements[sid],subtree,timeout)
        updated['subtrees'][index]=subtree
    durable_directory(output)
    record=make_record(updated,placements,parent,'compaction',canonical_vectors=artifact(canonical))
    if dynamic_runtime.producer_bundle_sha256()!=producer_sha:raise ValueError('compactor producer changed before sealing')
    validate_record(record)
    return record


def publish_global_append(pointer, parent, planner_binary, reader_binary, canonical_vectors, output, probe, **limits):
    output=Path(output).resolve()
    return run_transaction(pointer,parent,
        lambda pinned:prepare_global_append(planner_binary,reader_binary,pinned,canonical_vectors,output,**limits),
        probe,Path(str(output)+'.transaction.json'),
        parameters=dict(operation='global-write-beam-append',planner_sha256=file_hash(planner_binary),reader_sha256=file_hash(reader_binary),private_output=str(output),canonical_input=str(Path(canonical_vectors).resolve())))
