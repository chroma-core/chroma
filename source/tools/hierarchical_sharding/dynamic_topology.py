"""Rebuild affected ownership boundaries as one immutable topology transaction.

This first version admits a frozen dense canonical corpus. It relocates live
IDs, rather than applying document updates/deletes. Every rebuilt reader uses
exactly those canonical FP32 rows, with fresh local versions in a new snapshot.
"""
import array
import copy
import json
import math
from pathlib import Path
import struct
from .tree_manifest import artifact,checked_artifact,canonical_bytes,load_model,routing_boundary_hash,validate_tree
from .dynamic_runtime import build_lower,durable_directory


def owned_ids(subtree,base='.'):
    path=checked_artifact(subtree['local_to_global'],base)
    with path.open('rb') as stream:
        while data:=stream.read(8*65536):
            if len(data)%8:raise ValueError('truncated global ID map')
            yield from (row[0] for row in struct.iter_unpack('<Q',data))


def prepare_topology_record(binary,parent,replacements,output,timeout=60,lower_builder=None):
    """Prepare affected real lower trees and optional ownership pages together."""
    from .dynamic_deployment import make_record,validate_record,validate_ownership_pages
    from .dynamic_runtime import seal_owner,probe_reader
    validate_record(parent)
    output=Path(output).resolve()
    tree=rewrite_topology(binary,parent,parent['canonical_vectors'],replacements,output,timeout=timeout,lower_builder=lower_builder)
    owners={}
    previous={s['subtree_id']:s for s in parent['tree']['subtrees']}
    for subtree in tree['subtrees']:
        sid=subtree['subtree_id']
        if sid in previous and subtree['snapshot']['sha256']==previous[sid]['snapshot']['sha256'] and subtree['generation']==previous[sid]['generation']:
            owners[sid]=copy.deepcopy(parent['owners'][sid])
        else:
            # Local correctness composer; remote fleet assignment belongs to
            # the existing compactor transport, not an implicit local claim.
            sources=replacements.get(sid,{}).get('from',[])
            workers={parent['owners'][s]['worker_id'] for s in sources}
            if len(workers)>1:raise ValueError('cross-worker rebuild requires explicit compactor execution binding')
            worker=next(iter(workers),'local')
            if any(parent['owners'][s].get('remote') for s in sources):raise ValueError('local topology composer cannot impersonate remote compactor')
            owners[sid]=seal_owner(Path(subtree['snapshot']['path']).parent,subtree,worker)
            probe_reader(binary,sid,owners[sid],subtree,timeout)
    paged=None
    if 'ownership_pages' in parent:
        from . import ownership_pages as pages
        maps={s['subtree_id']:list(owned_ids(s)) for s in tree['subtrees']}
        paged=pages.replace(validate_ownership_pages(parent),maps,output/'ownership-pages')
    record=make_record(tree,owners,parent,'topology',ownership_pages=paged)
    validate_record(record);return record


def publish_topology_record(pointer,parent,binary,replacements,output,probe,**limits):
    """Journal private preparation and publish all topology/page roots in one CAS."""
    from .dynamic_deployment import run_transaction
    output=Path(output).resolve()
    return run_transaction(pointer,parent,
        lambda pinned:prepare_topology_record(binary,pinned,replacements,output,**limits),
        probe,Path(str(output)+'.transaction.json'),
        parameters=dict(operation='topology-with-ownership-pages',private_output=str(output),replacements=replacements))


def rewrite_topology(binary,parent,canonical_vectors,replacements,output,timeout=60,max_rebuild_rows=100000,max_rebuild_bytes=512*1024**2,lower_builder=None):
    """Replace an explicit set of input subtrees with complete output ID lists.

    replacements={new_id:{'from':[old_ids], 'ids':[global IDs]}}. Inputs may
    appear in multiple outputs for a split. Their union must exactly equal the
    output union, with no duplicates. Unaffected slots retain immutable blocks.
    """
    from .dynamic_deployment import resolved_tree
    old=resolved_tree(parent);validate_tree(old)
    checked=checked_artifact(canonical_vectors,'.')
    if canonical_vectors!=parent.get('canonical_vectors'):raise ValueError('canonical corpus is not pinned by deployment')
    dimension=old['dimension'];total=old['source']['row_count']
    if checked.stat().st_size!=total*dimension*4:raise ValueError('canonical corpus size differs')
    if not replacements:raise ValueError('explicit affected ownership required')
    slots={s['subtree_id']:s for s in old['subtrees']}
    affected=set()
    for sid,row in replacements.items():
        if not isinstance(sid,str) or not sid or not row.get('from'):raise ValueError('replacement identities and input ownership required')
        if sid in slots and sid not in row['from']:raise ValueError('replacement collides with unaffected identity')
        affected.update(row['from'])
    if not affected<=set(slots):raise ValueError('unknown affected input subtree')
    rebuilding=affected if old.get('routing_boundary_scheme')=='centroid-content-v1' else set(slots)
    rows=sum(slots[sid]['vector_count'] for sid in rebuilding)
    if type(max_rebuild_rows) is not int or type(max_rebuild_bytes) is not int or rows>max_rebuild_rows or rows*(dimension*4+8)>max_rebuild_bytes:raise ValueError('affected rebuild exceeds declared row/byte admission')
    expected=set()
    for sid in affected:
        for ident in owned_ids(slots[sid]):
            if ident in expected:raise ValueError('input ownership overlaps')
            expected.add(ident)
    assigned=set();groups={}
    for sid,row in sorted(replacements.items()):
        ids=list(row['ids'])
        if not ids or any(type(ident) is not int or ident not in expected for ident in ids):raise ValueError('replacement IDs must be live affected IDs')
        if len(ids)!=len(set(ids)) or assigned.intersection(ids):raise ValueError('duplicate replacement ownership')
        # An output may only claim the input owners explicitly named for it.
        allowed=set().union(*(set(owned_ids(slots[source])) for source in row['from']))
        if not set(ids)<=allowed:raise ValueError('replacement claims an undeclared input owner')
        assigned.update(ids);groups[sid]=sorted(ids)
    if assigned!=expected:raise ValueError('replacement ownership loses live IDs')
    configurations=[]
    for sid in rebuilding:
        snapshot=json.loads(checked_artifact(slots[sid]['snapshot'],'.').read_text())
        configurations.append({key:snapshot['construction'][key] for key in ('branching_factor','split_threshold','merge_threshold')})
    if any(config!=configurations[0] for config in configurations):raise ValueError('affected owners have incompatible construction policies')
    construction=configurations[0]
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    tree=copy.deepcopy(old);tree['tree_generation']+=1;tree['routing_boundary_scheme']='centroid-content-v1'
    model=load_model(old);centers=[];newslots=[]
    # Converting a static model-file boundary to per-centroid boundaries requires
    # rebuilding every lower snapshot once; otherwise unaffected blocks persist.
    if old.get('routing_boundary_scheme')!='centroid-content-v1':
        for sid in sorted(set(slots)-affected):groups[sid]=list(owned_ids(slots[sid]))
    unchanged=[s for s in old['subtrees'] if s['subtree_id'] not in affected and s['subtree_id'] not in groups]
    for subtree in unchanged:
        subtree=copy.deepcopy(subtree);centers.append(model['centroids'][subtree['centroid_index']]);subtree['centroid_index']=len(centers)-1;newslots.append(subtree)
    prepared=[]
    with checked.open('rb') as source:
        for sid,ids in sorted(groups.items()):
            vector_path=output/(sid+'.f32');map_path=output/(sid+'.u64le');center=[0.0]*dimension
            if '/' in sid or '\\' in sid or sid in ('.','..'):raise ValueError('subtree identity is not a safe filename')
            with vector_path.open('xb') as vectors,map_path.open('xb') as mapping:
                for ident in ids:
                    source.seek(ident*dimension*4);raw=source.read(dimension*4)
                    values=struct.unpack('<'+'f'*dimension,raw)
                    if any(not math.isfinite(v) for v in values):raise ValueError('nonfinite canonical vector')
                    vectors.write(raw);mapping.write(struct.pack('<Q',ident))
                    for i,v in enumerate(values):center[i]+=v/len(ids)
            if old['metric']=='cosine' and not any(value!=0 for value in center):raise ValueError('cosine centroid cancels to zero; choose a different ownership partition')
            centers.append(center);prepared.append((sid,vector_path,map_path,len(centers)-1))
    model=dict(model,centroids=centers)
    model_path=output/'model.json';model_path.write_bytes(canonical_bytes(model));tree['model']=artifact(model_path)
    for sid,vectors,mapping,index in prepared:
        boundary=routing_boundary_hash(tree,model,index)
        subtree=(lower_builder or build_lower)(binary,vectors,mapping,output/('index-'+sid),sid,tree['tree_generation'],dimension,tree['metric'],tree['source']['source_id'],boundary,timeout,construction=construction)
        subtree['centroid_index']=index;newslots.append(subtree)
    tree['subtrees']=newslots;tree['upper_root']=dict(kind='centroid_root',children=[dict(subtree_id=s['subtree_id'],centroid_index=s['centroid_index']) for s in newslots])
    validate_tree(tree);(output/'tree.json').write_bytes(canonical_bytes(tree));durable_directory(output)
    return tree


def _planning_tree(value):
    if 'tree' in value:
        from .dynamic_deployment import resolved_tree
        return resolved_tree(value)
    return value


def split_request(tree,sid,left_ids,left_id,right_id):
    tree=_planning_tree(tree)
    current=set(owned_ids(next(s for s in tree['subtrees'] if s['subtree_id']==sid)))
    left=set(left_ids)
    if not left or not left<current or left_id==right_id:raise ValueError('split requires two nonempty distinct owners')
    return {left_id:{'from':[sid],'ids':sorted(left)},right_id:{'from':[sid],'ids':sorted(current-left)}}


def merge_request(tree,inputs,new_id):
    tree=_planning_tree(tree)
    slots={s['subtree_id']:s for s in tree['subtrees']}
    if len(set(inputs))<2 or not set(inputs)<=set(slots):raise ValueError('merge requires distinct known owners')
    return {new_id:{'from':list(inputs),'ids':sorted(ident for sid in inputs for ident in owned_ids(slots[sid]))}}


def reassign_request(tree,source,destination,ids):
    tree=_planning_tree(tree)
    slots={s['subtree_id']:s for s in tree['subtrees']}
    if source==destination:raise ValueError('reassignment needs distinct owners')
    moved=set(ids);a=set(owned_ids(slots[source]));b=set(owned_ids(slots[destination]))
    if not moved or not moved<a:raise ValueError('reassignment requires live IDs and a nonempty source remainder')
    return {source:{'from':[source],'ids':sorted(a-moved)},destination:{'from':[source,destination],'ids':sorted(b|moved)}}


def append_canonical(binary,parent,canonical_vectors,output,timeout=60,max_rebuild_rows=100000,max_rebuild_bytes=512*1024**2,lower_builder=None):
    """Compact an appended canonical prefix under the current routing model.

    Affected lower trees are rebuilt in fresh namespaces. Existing local ID
    order is retained as a prefix; unchanged owners keep their exact snapshots.
    The resulting deployment must publish through the compaction CAS contract.
    """
    from .dynamic_deployment import verify_canonical_append
    from .subtree_transport import distance
    from .dynamic_deployment import resolved_tree
    old=resolved_tree(parent);validate_tree(old)
    if old.get('routing_boundary_scheme')!='centroid-content-v1':raise ValueError('append requires a dynamic centroid ownership contract')
    new_path=verify_canonical_append(parent['canonical_vectors'],canonical_vectors,old['dimension'])
    dimension=old['dimension'];old_count=old['source']['row_count'];count=new_path.stat().st_size//(dimension*4)
    if type(max_rebuild_rows) is not int or type(max_rebuild_bytes) is not int or count-old_count>max_rebuild_rows:raise ValueError('append input exceeds admitted rebuild rows')
    model=load_model(old);slots={s['subtree_id']:s for s in old['subtrees']};additions={}
    with new_path.open('rb') as source:
        source.seek(old_count*dimension*4)
        for ident in range(old_count,count):
            vector=struct.unpack('<'+'f'*dimension,source.read(dimension*4))
            if any(not math.isfinite(v) for v in vector):raise ValueError('nonfinite appended vector')
            slot=min(old['subtrees'],key=lambda s:(distance(vector,model['centroids'][s['centroid_index']],old['metric']),s['subtree_id']))
            additions.setdefault(slot['subtree_id'],[]).append(ident)
    rows=sum(slots[sid]['vector_count']+len(ids) for sid,ids in additions.items())
    if rows>max_rebuild_rows or rows*(dimension*4+8)>max_rebuild_bytes:raise ValueError('append rebuild exceeds admitted row/byte budget')
    output=Path(output);output.mkdir(parents=True,exist_ok=False);tree=copy.deepcopy(old);tree['tree_generation']+=1;tree['source']['row_count']=count
    with new_path.open('rb') as source:
        for index,previous in enumerate(old['subtrees']):
            sid=previous['subtree_id']
            if sid not in additions:continue
            ids=list(owned_ids(previous))+additions[sid];vp=output/(sid+'.f32');mp=output/(sid+'.u64le')
            with vp.open('xb') as vectors,mp.open('xb') as mapping:
                for ident in ids:
                    source.seek(ident*dimension*4);vectors.write(source.read(dimension*4));mapping.write(struct.pack('<Q',ident))
            snapshot=json.loads(checked_artifact(previous['snapshot'],'.').read_text());config={key:snapshot['construction'][key] for key in ('branching_factor','split_threshold','merge_threshold')}
            subtree=(lower_builder or build_lower)(binary,vp,mp,output/('index-'+sid),sid,tree['tree_generation'],dimension,tree['metric'],tree['source']['source_id'],previous['routing_boundary_hash'],timeout,construction=config)
            subtree['centroid_index']=previous['centroid_index'];tree['subtrees'][index]=subtree
    validate_tree(tree);(output/'tree.json').write_bytes(canonical_bytes(tree));durable_directory(output);return tree
