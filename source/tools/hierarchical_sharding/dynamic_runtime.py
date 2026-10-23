"""Local immutable replica transfer and actual Rust snapshot readiness."""
import json
import os
from pathlib import Path
import shutil
import subprocess
from .tree_manifest import artifact,checked_artifact,file_hash,canonical_bytes,content_hash
from .subtree_transport import PinnedReader


def inventory(directory):
    directory=Path(directory);rows={}
    if directory.is_symlink():raise ValueError('checkpoint root cannot be a symlink')
    for path in sorted(directory.rglob('*')):
        if path.is_symlink():raise ValueError('checkpoint links are not transferable')
        if path.is_dir():continue
        if not path.is_file():raise ValueError('checkpoint contains nonregular object')
        rows[str(path.relative_to(directory))]=dict(bytes=path.stat().st_size,sha256=file_hash(path))
    if not rows:raise ValueError('empty checkpoint')
    return rows


def copy_checkpoint(source,destination,max_bytes):
    """Copy a quiescent immutable store, preserving source bytes and filenames.

    Source and destination inventories must match after copying. A partial
    destination is retained for diagnosis and is never reused automatically.
    """
    source=Path(source);destination=Path(destination)
    before=inventory(source)
    if type(max_bytes) is not int or sum(row['bytes'] for row in before.values())>max_bytes:raise ValueError('replica exceeds admitted byte budget')
    destination.mkdir(parents=True,exist_ok=False)
    for relative in before:
        target=destination/relative;target.parent.mkdir(parents=True,exist_ok=True)
        with (source/relative).open('rb') as src,target.open('xb') as dst:
            shutil.copyfileobj(src,dst,1024*1024);dst.flush();os.fsync(dst.fileno())
    if inventory(source)!=before or inventory(destination)!=before:raise ValueError('checkpoint changed during transfer')
    (destination.parent/(destination.name+'.transfer.json')).write_bytes(canonical_bytes(dict(source=str(source.resolve()),destination=str(destination.resolve()),files=before)))
    return before


def descriptor(binary,checkpoint,subtree):
    checkpoint=Path(checkpoint)
    return dict(binary=str(binary),checkpoint_dir=str(checkpoint),manifest=str(checkpoint/f"manifest-{subtree['generation']}.json"),snapshot_sha256=subtree['snapshot']['sha256'],runtime_threads=2)


def probe_reader(binary,sid,owner,subtree,timeout=30):
    before=inventory(owner['checkpoint_dir'])
    if owner.get('inventory_sha256')!=__import__('hashlib').sha256(canonical_bytes(before)).hexdigest():raise ValueError('destination lacks its sealed complete file inventory')
    reader=PinnedReader(descriptor(binary,owner['checkpoint_dir'],subtree),timeout)
    try:
        row=reader.call(dict(subtree_id=sid,snapshot_sha256=subtree['snapshot']['sha256'],payload=dict(op='describe_root')))['result']
        root=dict(kind=row['root']['kind'],id=row['root']['id'])
        return dict(snapshot_sha256=subtree['snapshot']['sha256'],generation=row['generation'],root_ref=root,vector_count=row['vector_count'],map_sha256=row['id_mapping']['sha256'])
    finally:
        reader.close()
        if inventory(owner['checkpoint_dir'])!=before:raise ValueError('destination changed during readiness probe')


def build_lower(binary,vectors,mapping,directory,sid,generation,dimension,metric,source_id,boundary_hash,timeout=60,checkpoint_rows=10000,construction=None):
    directory=Path(directory)
    if directory.exists():raise ValueError('fresh lower-tree output required')
    directory.parent.mkdir(parents=True,exist_ok=True)
    count=Path(mapping).stat().st_size//8
    if count<1 or Path(mapping).stat().st_size!=count*8 or Path(vectors).stat().st_size!=count*dimension*4:raise ValueError('mapped vector dimensions differ')
    construction=construction or dict(branching_factor=100,split_threshold=2048,merge_threshold=512)
    if set(construction)!={'branching_factor','split_threshold','merge_threshold'} or any(type(v) is not int for v in construction.values()):raise ValueError('explicit supported construction configuration required')
    argv=[str(binary),'build','--vectors',str(vectors),'--id-map',str(mapping),'--checkpoint-dir',str(directory),'--shard-id',sid,'--generation',str(generation),'--dimension',str(dimension),'--metric',metric,'--source-id',source_id,'--boundary-hash',boundary_hash,'--row-count',str(count),'--checkpoint-rows',str(checkpoint_rows),'--threads','1','--branching-factor',str(construction['branching_factor']),'--split-threshold',str(construction['split_threshold']),'--merge-threshold',str(construction['merge_threshold'])]
    with open(str(directory)+'.stdout','xb') as out,open(str(directory)+'.stderr','xb') as err:
        subprocess.run(argv,stdout=out,stderr=err,timeout=timeout,check=True,env=dict(os.environ,TOKIO_WORKER_THREADS='2'))
    with open(str(directory)+'.stderr','rb') as diagnostics:
        if any(b'without full convergence' in line for line in diagnostics):raise ValueError('lower tree balancing did not converge')
    manifest=directory/f'manifest-{generation}.json';snapshot=json.loads(manifest.read_text())
    map_path=directory/snapshot['id_mapping']['path']
    subtree=dict(subtree_id=sid,generation=generation,vector_count=count,snapshot=artifact(manifest),local_to_global=artifact(map_path),root_ref=snapshot['root_ref'],routing_boundary_hash=boundary_hash)
    subtree['ownership_revision']=content_hash(dict(source_id=source_id,subtree_id=sid,map_sha256=subtree['local_to_global']['sha256']))
    owner=seal_owner(directory,subtree,'local')
    ready=probe_reader(binary,sid,owner,subtree,timeout)
    if ready['vector_count']!=count:raise ValueError('built lower tree count differs')
    return subtree


def seal_owner(directory,subtree,worker_id,endpoint=None):
    rows=inventory(directory)
    return dict(worker_id=worker_id,checkpoint_dir=str(Path(directory).resolve()),snapshot_sha256=subtree['snapshot']['sha256'],inventory_sha256=__import__('hashlib').sha256(canonical_bytes(rows)).hexdigest(),endpoint=endpoint)


def prepare_migration(parent,assignments,destinations,max_bytes):
    """Prepare changed worker replicas without touching current serving state.

    assignments maps every logical subtree to its target worker. destinations
    supplies fresh local checkpoint paths only for subtrees that move. Remote
    transport is intentionally not inferred from path strings.
    """
    import copy
    from .dynamic_deployment import make_record
    owners=copy.deepcopy(parent['owners'])
    if set(assignments)!=set(owners):raise ValueError('migration assignment is incomplete')
    slots={s['subtree_id']:s for s in parent['tree']['subtrees']}
    moved={sid for sid in owners if assignments[sid]!=owners[sid]['worker_id']}
    if set(destinations)!=moved:raise ValueError('fresh destination required for exactly each moved subtree')
    for sid in sorted(moved):
        source=owners[sid]['checkpoint_dir']
        # A sealed source must still match its admitted inventory.
        import hashlib
        if hashlib.sha256(canonical_bytes(inventory(source))).hexdigest()!=owners[sid]['inventory_sha256']:raise ValueError('sealed source changed')
        copy_checkpoint(source,destinations[sid],max_bytes)
        owners[sid]=seal_owner(destinations[sid],slots[sid],assignments[sid])
    return make_record(parent['tree'],owners,parent,'placement')


class DeploymentReader:
    """Pin one complete deployment and persistent generation-specific readers."""
    def __init__(self,pointer,binary,timeout=30,backend=None):
        from .dynamic_deployment import load_deployment,validate_record,resolved_tree
        from .subtree_transport import LocalBackend,TreeCoordinator
        from .tree_manifest import load_model
        import mmap
        self.record=load_deployment(pointer);validate_record(self.record)
        tree=self.record['tree'];descriptors={s['subtree_id']:descriptor(binary,self.record['owners'][s['subtree_id']]['checkpoint_dir'],s) for s in tree['subtrees']}
        backend=backend or LocalBackend(descriptors,timeout);mappings={}
        try:
            for s in resolved_tree(self.record)['subtrees']:
                file=checked_artifact(s['local_to_global'],'.').open('rb');mappings[s['subtree_id']]=(file,mmap.mmap(file.fileno(),0,access=mmap.ACCESS_READ))
            self.coordinator=TreeCoordinator(tree,load_model(tree),backend,mappings)
        except BaseException:
            backend.close()
            for file,mapping in mappings.values():mapping.close();file.close()
            raise
    def query(self,*args,**kwargs):return self.coordinator.query(*args,**kwargs)
    def close(self):self.coordinator.close()


class GenerationServing:
    """Keep separate HTTP processes/readers alive for each pinned deployment.

    This local fixture/embedding adapter never reconfigures an existing host.
    Production transport must provide the same generation-specific endpoint
    lifetime guarantee, including authentication and independent supervision.
    """
    def __init__(self,record,binary,timeout=30):
        import copy,threading
        from .subtree_server import Host,make_server
        self.record=copy.deepcopy(record);self.servers=[];self.threads=[]
        tree=record['tree'];slots={s['subtree_id']:s for s in tree['subtrees']}
        grouped={}
        for sid,owner in record['owners'].items():grouped.setdefault(owner['worker_id'],{})[sid]=descriptor(binary,owner['checkpoint_dir'],slots[sid])
        try:
            for worker,descriptors in sorted(grouped.items()):
                host=Host(dict(tree_sha256=__import__('hashlib').sha256(canonical_bytes(tree)).hexdigest(),owner_id=worker,subtrees=descriptors,timeout=timeout,max_inflight_batches=4))
                try:server=make_server(host,port=0)
                except BaseException:host.close();raise
                thread=threading.Thread(target=server.serve_forever,daemon=True)
                try:thread.start()
                except BaseException:server.server_close();host.close();raise
                self.servers.append((server,host));self.threads.append(thread)
                for sid in descriptors:self.record['owners'][sid]['endpoint']='http://127.0.0.1:'+str(server.server_address[1])
        except BaseException:self.close();raise
    def backend(self):
        from .subtree_transport import HttpBackend
        return HttpBackend(dict(owners={sid:dict(owner_id=owner['worker_id'],endpoint=owner['endpoint']) for sid,owner in self.record['owners'].items()}))
    def close(self):
        for server,host in self.servers:server.shutdown();server.server_close();host.close()
        for thread in self.threads:thread.join(timeout=2)
        self.servers=[];self.threads=[]
