"""Local immutable replica transfer and actual Rust snapshot readiness."""
import json
import os
from pathlib import Path
import shutil
import subprocess
from .tree_manifest import artifact,checked_artifact,file_hash,canonical_bytes,content_hash
from .subtree_transport import PinnedReader

def producer_bundle_sha256():
    """Bind construction, content interpretation, persistence, and reader admission."""
    root=Path(__file__).parent
    return content_hash({name:file_hash(root/name) for name in ('dynamic_runtime.py','dynamic_content.py','dynamic_write.py','dynamic_topology.py','dynamic_deployment.py','ownership_pages.py','ownership_trie.py','tree_manifest.py','subtree_transport.py')})


PRODUCER_SOURCE_SHA256=producer_bundle_sha256()


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


def durable_namespace(directory):
    directory=Path(directory)
    for path in (directory,*directory.parents):
        fd=os.open(path,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)


def durable_file(path):
    path=Path(path)
    if path.is_symlink():raise ValueError('durable artifact cannot be a symlink')
    with path.open('rb') as stream:os.fsync(stream.fileno())
    durable_namespace(path.parent)


def durable_directory(directory):
    """Persist file contents and the complete namespace before sealing it."""
    directory=Path(directory)
    # The caller owns this fresh namespace; links cannot redirect fsync or copy.
    inventory(directory)
    directories=[directory]
    for path in directory.rglob('*'):
        if path.is_dir():directories.append(path)
        else:
            with path.open('rb') as stream:os.fsync(stream.fileno())
    # Deepest-first fsync persists child entries, then each newly created parent
    # entry. Existing ancestors are included so a fresh worker root is covered.
    directories.extend(directory.parents)
    for path in sorted(set(directories),key=lambda p:len(p.parts),reverse=True):
        fd=os.open(path,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)

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
    durable_directory(destination)
    from .tree_manifest import _atomic_write
    _atomic_write(destination.parent/(destination.name+'.transfer.json'),canonical_bytes(dict(source=str(source.resolve()),destination=str(destination.resolve()),files=before)))
    return before


def descriptor(binary,checkpoint,subtree):
    checkpoint=Path(checkpoint)
    return dict(binary=str(binary),checkpoint_dir=str(checkpoint),manifest=str(checkpoint/f"manifest-{subtree['generation']}.json"),snapshot_sha256=subtree['snapshot']['sha256'],runtime_threads=2)


def probe_reader(binary,sid,owner,subtree,timeout=30):
    durable_directory(owner['checkpoint_dir'])
    before=inventory(owner['checkpoint_dir'])
    if owner.get('inventory_sha256')!=__import__('hashlib').sha256(canonical_bytes(before)).hexdigest():raise ValueError('destination lacks its sealed complete file inventory')
    reader=PinnedReader(descriptor(binary,owner['checkpoint_dir'],subtree),timeout)
    try:
        row=reader.call(dict(subtree_id=sid,snapshot_sha256=subtree['snapshot']['sha256'],payload=dict(op='describe_root')))['result']
        root=dict(kind=row['root']['kind'],id=row['root']['id'])
        if owner.get('endpoint') is not None:
            from .subtree_transport import HttpBackend
            backend=HttpBackend(dict(owners={sid:dict(owner_id=owner['worker_id'],endpoint=owner['endpoint'])}))
            item=dict(subtree_id=sid,snapshot_sha256=subtree['snapshot']['sha256'],payload=dict(op='describe_root'))
            responses,_=backend.call_many([item],owner['serving_tree_sha256'],timeout)
            response=responses[0];remote=response.get('result',{})
            if response.get('subtree_id')!=sid or response.get('snapshot_sha256')!=subtree['snapshot']['sha256'] or any(remote.get(key)!=row.get(key) for key in ('generation','shard_id','root','dimension','metric','vector_count','id_mapping','routing_boundary_hash')):raise ValueError('serving endpoint does not expose the admitted pinned reader')
        return dict(snapshot_sha256=subtree['snapshot']['sha256'],generation=row['generation'],root_ref=root,vector_count=row['vector_count'],map_sha256=row['id_mapping']['sha256'])
    finally:
        reader.close()
        if inventory(owner['checkpoint_dir'])!=before:raise ValueError('destination changed during readiness probe')


def build_lower(binary,vectors,mapping,directory,sid,generation,dimension,metric,source_id,boundary_hash,timeout=60,checkpoint_rows=10000,construction=None):
    directory=Path(directory)
    if directory.exists():raise ValueError('fresh lower-tree output required')
    directory.parent.mkdir(parents=True,exist_ok=True)
    from .dynamic_content import VERIFIED_WRITERS,mapped_digest,PROOF_NAME
    from .tree_manifest import _atomic_write
    import time
    binary_sha=file_hash(binary);producer_sha=PRODUCER_SOURCE_SHA256
    if producer_bundle_sha256()!=producer_sha:raise ValueError('producer source changed since module import')
    if binary_sha not in VERIFIED_WRITERS:raise ValueError('Rust writer binary is not an admitted immutable epoch')
    original_vectors,original_mapping=Path(vectors),Path(mapping)
    directory.mkdir(mode=0o700)
    if directory.stat().st_uid!=os.geteuid() or directory.stat().st_mode&0o077:raise ValueError('construction namespace must be private to the executing owner')
    input_descriptors={}
    for name,source in (('construction-input.f32',original_vectors),('construction-input.u64le',original_mapping)):
        before=artifact(source)
        with source.open('rb') as src,(directory/name).open('xb') as dst:
            shutil.copyfileobj(src,dst,1024*1024);dst.flush();os.fsync(dst.fileno())
        os.chmod(directory/name,0o400)
        copied=artifact(directory/name)
        if any(before[key]!=copied[key] for key in ('bytes','sha256')) or artifact(source)!=before:raise ValueError('immutable construction input changed while sealing')
        input_descriptors['vectors' if name.endswith('.f32') else 'map']=dict(file=name,bytes=copied['bytes'],sha256=copied['sha256'])
    vectors=directory/'construction-input.f32';mapping=directory/'construction-input.u64le'
    count=Path(mapping).stat().st_size//8
    if count<1 or Path(mapping).stat().st_size!=count*8 or Path(vectors).stat().st_size!=count*dimension*4:raise ValueError('mapped vector dimensions differ')
    construction=construction or dict(branching_factor=100,split_threshold=2048,merge_threshold=512)
    if set(construction)!={'branching_factor','split_threshold','merge_threshold'} or any(type(v) is not int for v in construction.values()):raise ValueError('explicit supported construction configuration required')
    argv=[str(binary),'build','--vectors',str(vectors),'--id-map',str(mapping),'--checkpoint-dir',str(directory),'--shard-id',sid,'--generation',str(generation),'--dimension',str(dimension),'--metric',metric,'--source-id',source_id,'--boundary-hash',boundary_hash,'--row-count',str(count),'--checkpoint-rows',str(checkpoint_rows),'--threads','1','--branching-factor',str(construction['branching_factor']),'--split-threshold',str(construction['split_threshold']),'--merge-threshold',str(construction['merge_threshold'])]
    mapped_sha,mapped_count=mapped_digest(vectors,mapping,dimension,metric)
    started=time.time()
    with open(str(directory)+'.stdout','xb') as out,open(str(directory)+'.stderr','xb') as err:
        process=subprocess.Popen(argv,stdout=out,stderr=err,env=dict(os.environ,TOKIO_WORKER_THREADS='2'))
        try:
            returncode=process.wait(timeout=timeout)
            if returncode:raise subprocess.CalledProcessError(returncode,argv)
        except BaseException:
            if process.poll() is None:process.kill();process.wait(timeout=5)
            raise
    finished=time.time()
    if file_hash(binary)!=binary_sha or producer_bundle_sha256()!=producer_sha:raise ValueError('executed producer changed during construction')
    for desc in input_descriptors.values():
        if file_hash(directory/desc['file'])!=desc['sha256']:raise ValueError('executed writer input changed during construction')
    with open(str(directory)+'.stderr','rb') as diagnostics:
        if any(b'without full convergence' in line for line in diagnostics):raise ValueError('lower tree balancing did not converge')
    manifest=directory/f'manifest-{generation}.json';snapshot=json.loads(manifest.read_text())
    map_path=directory/snapshot['id_mapping']['path']
    subtree=dict(subtree_id=sid,generation=generation,vector_count=count,snapshot=artifact(manifest),local_to_global=artifact(map_path),root_ref=snapshot['root_ref'],routing_boundary_hash=boundary_hash)
    subtree['ownership_revision']=content_hash(dict(source_id=source_id,subtree_id=sid,map_sha256=subtree['local_to_global']['sha256']))
    files=inventory(directory)
    proof=dict(format_version=1,producer_source_sha256=producer_sha,binary_sha256=binary_sha,argv=argv,pid=process.pid,started_unix=started,finished_unix=finished,returncode=returncode,inputs=input_descriptors,mapped_normalized_sha256=mapped_sha,snapshot_sha256=subtree['snapshot']['sha256'],map_sha256=subtree['local_to_global']['sha256'],dimension=dimension,metric=metric,output_inventory_sha256=content_hash(files))
    if mapped_count!=count or input_descriptors['map']['sha256']!=subtree['local_to_global']['sha256']:raise ValueError('actual writer output map differs from executed input')
    _atomic_write(directory/PROOF_NAME,canonical_bytes(proof))
    owner=seal_owner(directory,subtree,'local')
    ready=probe_reader(binary,sid,owner,subtree,timeout)
    if ready['vector_count']!=count:raise ValueError('built lower tree count differs')
    return subtree


def seal_owner(directory,subtree,worker_id,endpoint=None):
    durable_directory(directory)
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
    def __init__(self,pointer,binary,timeout=30,backend=None,mapping_mode='auto',mapper_cache_nodes=32,mapper_cache_bytes=262144,worker_id_mode='global',evict_postings_after_operation=False,profile_memory=False,streaming_limits=None):
        from .dynamic_deployment import load_deployment,validate_record_with_trie,resolved_tree
        from .subtree_transport import LocalBackend,TreeCoordinator,validate_streaming_limits
        from .tree_manifest import load_model
        import mmap
        streaming_limits=validate_streaming_limits(streaming_limits)
        if type(evict_postings_after_operation) is not bool or type(profile_memory) is not bool:raise ValueError('reader policy/profile flags must be boolean')
        if backend is not None and (evict_postings_after_operation or profile_memory or streaming_limits is not None):raise ValueError('external reader policy/profile requires separate admission')
        if mapping_mode not in ('auto','flat-oracle'):raise ValueError('unknown query mapping mode')
        if worker_id_mode not in ('global','local-only'):raise ValueError('unknown worker ID mode')
        self.record=load_deployment(pointer);_,validated_trie=validate_record_with_trie(self.record)
        if worker_id_mode=='local-only' and ('ownership_trie' not in self.record or mapping_mode!='auto'):raise ValueError('local-only workers require committed trie mapper')
        if worker_id_mode=='local-only' and backend is not None:raise ValueError('external local-only backend requires a separate admission contract')
        if backend is not None and getattr(backend,'deployment_sha256',None)!=content_hash(self.record):raise ValueError('injected backend does not pin this complete deployment')
        tree=self.record['tree'];descriptors={s['subtree_id']:descriptor(binary,self.record['owners'][s['subtree_id']]['checkpoint_dir'],s) for s in tree['subtrees']}
        for value in descriptors.values():
            if worker_id_mode=='local-only':value['local_ids_only']=True
            if evict_postings_after_operation:value['evict_postings_after_operation']=True
            if profile_memory:value['profile_memory']=True
            if streaming_limits is not None:value['streaming_limits']=dict(streaming_limits)
        backend=backend or LocalBackend(descriptors,timeout);mappings={};self.id_mapper=None
        try:
            if 'ownership_trie' in self.record and mapping_mode=='auto':
                from .ownership_trie import PinnedReader as TrieReader
                if validated_trie is None or validated_trie.sha256!=content_hash(self.record['ownership_trie']):raise ValueError('validated trie receipt differs from this pinned deployment')
                self.id_mapper=TrieReader(validated_trie,mapper_cache_nodes,mapper_cache_bytes)
            else:
                for s in resolved_tree(self.record)['subtrees']:
                    file=checked_artifact(s['local_to_global'],'.').open('rb');mappings[s['subtree_id']]=(file,mmap.mmap(file.fileno(),0,access=mmap.ACCESS_READ))
            self.coordinator=TreeCoordinator(tree,load_model(tree),backend,mappings,id_mapper=self.id_mapper,local_ids_only=worker_id_mode=='local-only')
        except BaseException:
            backend.close()
            if self.id_mapper is not None:self.id_mapper.close()
            for file,mapping in mappings.values():mapping.close();file.close()
            raise
    def query(self,*args,**kwargs):return self.coordinator.query(*args,**kwargs)
    def mapping_metrics(self):
        if self.id_mapper is None:return dict(mode='flat-map-oracle',mapped_files=len(self.coordinator.mappings))
        mapper=self.id_mapper
        return dict(mode='pinned-trie',lookup_calls=mapper.lookup_calls,pinned_root_owned_bytes=mapper.root_owned_bytes,
                    mapped_flat_files=len(self.coordinator.mappings),**mapper.store.metrics,
                    scope=('Coordinator and local-only workers use trie IDs; flat maps startup/admission oracle only' if self.coordinator.local_ids_only else 'Coordinator mapper only; Rust worker returned-ID flat-map reads remain')+'; cache sizes exclude allocator slack/transient copies/RSS')
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
                for sid in descriptors:
                    self.record['owners'][sid]['endpoint']='http://127.0.0.1:'+str(server.server_address[1])
                    self.record['owners'][sid]['serving_tree_sha256']=content_hash(tree)
        except BaseException:self.close();raise
    def backend(self):
        from .subtree_transport import HttpBackend
        backend=HttpBackend(dict(owners={sid:dict(owner_id=owner['worker_id'],endpoint=owner['endpoint']) for sid,owner in self.record['owners'].items()}))
        backend.deployment_sha256=content_hash(self.record)
        return backend
    def close(self):
        for server,host in self.servers:server.shutdown();server.server_close();host.close()
        for thread in self.threads:thread.join(timeout=2)
        self.servers=[];self.threads=[]
