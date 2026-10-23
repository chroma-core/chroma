"""Tiny cross-host correctness operations within an externally supervised slot.

The controller must admit identities, transfers, guards, and tunnels before
calling this component. This file does not launch hosts or claim a benchmark.
"""
import copy
from pathlib import Path
import struct
from .dynamic_archive import export_checkpoint
from .dynamic_deployment import make_record,publish_deployment
from .dynamic_remote import RemoteBuilder,RemoteGeneration
from .dynamic_runtime import DeploymentReader,seal_owner
from .dynamic_topology import rewrite_topology,split_request,merge_request,reassign_request
from .tree_manifest import artifact,canonical_bytes,content_hash,routing_boundary_hash,_atomic_write


def assert_queries(reader,vectors):
    traces=[]
    for query in ([0.,0.],[17.,2.],[45.,4.]):
        result=reader.query(query,len(vectors),beam_width=None,candidate_budget=len(vectors))
        expected=sorted(range(len(vectors)),key=lambda i:(sum((a-b)**2 for a,b in zip(query,vectors[i])),i))
        if result['ids']!=expected or result['distances']!=[sum((a-b)**2 for a,b in zip(query,vectors[i])) for i in expected]:raise AssertionError('cross-host results differ from complete scalar reference')
        traces.append(dict(ids=result['ids'],distances=result['distances'],levels=result['levels'],selected_candidates=result['selected_candidates']))
    return traces


def owner_for(directory,subtree,worker,replica):
    owner=seal_owner(directory,subtree,worker)
    owner['remote']=dict(worker_id=worker,replica_id=replica)
    return owner


def run(clients,binary,output):
    if set(clients)!={'worker-a','worker-b'}:raise ValueError('exact two fixture workers required')
    output=Path(output);output.mkdir(exist_ok=False);pointer=output/'current.json'
    vectors=[(float(i+1),float(i%5+1)) for i in range(48)]
    corpus=output/'corpus.f32';corpus.write_bytes(b''.join(struct.pack('<ff',*row) for row in vectors))
    model=dict(construction_epoch='dynamic-twohost-v1',dimension=2,metric='l2',centroids=[[8.5,3],[24.5,3],[40.5,3]])
    model_path=output/'model.json';model_path.write_bytes(canonical_bytes(model))
    tree=dict(format_version=1,tree_generation=1,construction_epoch=model['construction_epoch'],dimension=2,metric='l2',source=dict(source_id='dynamic-twohost-tiny',row_count=48),routing_boundary_scheme='centroid-content-v1',model=artifact(model_path),subtrees=[],upper_root=dict(kind='centroid_root',children=[]))
    builder=RemoteBuilder(clients,{sid:'worker-a' for sid in ('s0','s1','s2')});owners={};serving=[];readers=[];report=dict(status='running',steps=[])
    try:
        for slot in range(3):
            sid='s'+str(slot);ids=list(range(slot*16,(slot+1)*16));vp=output/(sid+'.f32');mp=output/(sid+'.u64le')
            vp.write_bytes(b''.join(struct.pack('<ff',*vectors[i]) for i in ids));mp.write_bytes(b''.join(struct.pack('<Q',i) for i in ids))
            subtree=builder(binary,vp,mp,output/('index-'+sid),sid,1,2,'l2',tree['source']['source_id'],routing_boundary_hash(tree,model,slot),construction=dict(branching_factor=2,split_threshold=8,merge_threshold=2));subtree['centroid_index']=slot
            tree['subtrees'].append(subtree);tree['upper_root']['children'].append(dict(subtree_id=sid,centroid_index=slot));owners[sid]=owner_for(output/('index-'+sid),subtree,'worker-a',builder.built[sid]['replica_id'])
        record=make_record(tree,owners,canonical_vectors=artifact(corpus));generation=RemoteGeneration(record,clients);serving.append(generation)
        publish_deployment(pointer,record,None,generation.probe)
        old=DeploymentReader(pointer,binary,backend=generation.backend());readers.append(old);old_trace=assert_queries(old,vectors)
        report['steps'].append(dict(operation='initial',deployment_sha256=content_hash(record)))
        # Copy unchanged bytes to a physically distinct worker, then publish only placement.
        moved=copy.deepcopy(record);subtree=tree['subtrees'][0];source=owners['s0'];archive=output/'move.tar';receipt=export_checkpoint(source['checkpoint_dir'],archive);replica='migrated-s0'
        import urllib.error
        initial_head=pointer.read_bytes()
        ticket=clients['worker-b'].rpc(dict(op='prepare-import',replica_id='truncated-s0',receipt=receipt,subtree_id='s0',generation=subtree['generation'],snapshot_sha256=subtree['snapshot']['sha256']))
        try:
            with clients['worker-b'].request('/import/'+ticket['ticket'],archive.read_bytes()[:-1]):pass
        except urllib.error.HTTPError as error:error.close()
        else:raise AssertionError('truncated transfer is accepted')
        corrupted=copy.deepcopy(receipt);first=next(iter(corrupted['files']));corrupted['files'][first]['sha256']='0'*64
        try:clients['worker-b'].upload('bad-inventory-s0',archive,corrupted,subtree)
        except urllib.error.HTTPError as error:error.close()
        else:raise AssertionError('incorrect sealed file inventory is accepted')
        if pointer.read_bytes()!=initial_head or assert_queries(old,vectors)!=old_trace:raise AssertionError('failed transfer changes publication or old reader')
        report['steps'].append(dict(operation='transfer-failures',truncated_rejected=True,inventory_rejected=True,pointer_unchanged=True))
        clients['worker-b'].upload(replica,archive,receipt,subtree)
        mirror=output/'migrated-s0';clients['worker-b'].mirror(replica,source['inventory_sha256'],mirror,output/'move-mirror.tar')
        moved=make_record(tree,dict(owners,s0=owner_for(mirror,subtree,'worker-b',replica)),record)
        generation=RemoteGeneration(moved,clients);serving.append(generation);publish_deployment(pointer,moved,content_hash(record),generation.probe)
        current=DeploymentReader(pointer,binary,backend=generation.backend());readers.append(current)
        if assert_queries(current,vectors)!=old_trace or assert_queries(old,vectors)!=old_trace:raise AssertionError('placement changes logical traces or old-reader results')
        report['steps'].append(dict(operation='migration',deployment_sha256=content_hash(moved),tree_sha256=moved['tree_sha256']))
        parent=moved
        requests=[split_request(parent,'s0',range(8),'left','right')]
        for step in range(3):
            if step==1:requests.append(merge_request(parent,['left','right'],'merged'))
            if step==2:requests.append(reassign_request(parent,'merged','s1',[0,1,2]))
            assignments={sid:('worker-b' if index%2 else 'worker-a') for index,sid in enumerate(sorted(requests[step]))}
            rebuilder=RemoteBuilder(clients,assignments)
            updated=rewrite_topology(binary,parent,parent['canonical_vectors'],requests[step],output/('topology-'+str(step)),lower_builder=rebuilder)
            next_owners={}
            for row in updated['subtrees']:
                sid=row['subtree_id']
                if sid in rebuilder.built:
                    built=rebuilder.built[sid];next_owners[sid]=owner_for(Path(row['snapshot']['path']).parent,row,built['worker_id'],built['replica_id'])
                else:next_owners[sid]=parent['owners'][sid]
            candidate=make_record(updated,next_owners,parent,'topology');generation=RemoteGeneration(candidate,clients);serving.append(generation)
            publish_deployment(pointer,candidate,content_hash(parent),generation.probe)
            remote=DeploymentReader(pointer,binary,backend=generation.backend());direct=DeploymentReader(pointer,binary);readers.extend([remote,direct])
            if assert_queries(remote,vectors)!=assert_queries(direct,vectors):raise AssertionError('new topology remote/direct traces differ')
            if assert_queries(old,vectors)!=old_trace:raise AssertionError('old generation changes across boundary publication')
            report['steps'].append(dict(operation=('split','merge','reassignment')[step],deployment_sha256=content_hash(candidate)))
            parent=candidate
        try:publish_deployment(pointer,moved,content_hash(record),serving[1].probe)
        except ValueError:report['stale_cas_rejected']=True
        else:raise AssertionError('stale migration publishes over new topology')
        report.update(status='passed',final_deployment_sha256=content_hash(parent))
        return report
    except BaseException as error:
        report.update(status='failed',error=type(error).__name__+': '+str(error));raise
    finally:
        errors=[]
        for reader in reversed(readers):
            try:reader.close()
            except Exception as error:errors.append(str(error))
        for generation in reversed(serving):
            try:generation.close()
            except Exception as error:errors.append(str(error))
        if errors:report.update(status='cleanup-reconciliation-required',cleanup_errors=errors)
        _atomic_write(output/'scenario.json',canonical_bytes(report))
