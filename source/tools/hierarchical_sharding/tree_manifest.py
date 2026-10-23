"""Immutable shared-tree bundles and locked, compare-and-swap publication.

Validates local model, ID mappings and snapshot manifest bytes. Lower block
contents are validated only when supplied as explicit local artifacts; otherwise
opening each lower reader is an additional publication gate.
"""
import argparse
import array
from contextlib import contextmanager
import fcntl
import hashlib
import json
import mmap
import os
from pathlib import Path
import struct
import sys
import tempfile


def canonical_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\n').encode()


def content_hash(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def file_hash(path):
    h=hashlib.sha256()
    with open(path,'rb') as source:
        for block in iter(lambda: source.read(8*1024*1024),b''):
            h.update(block)
    return h.hexdigest()


def artifact(path, relative_to=None):
    path=Path(path)
    return dict(path=os.path.relpath(path,relative_to) if relative_to is not None else str(path.resolve()),bytes=path.stat().st_size,sha256=file_hash(path))


def checked_artifact(desc, base):
    if not isinstance(desc,dict) or not isinstance(desc.get('path'),str) or type(desc.get('bytes')) is not int or desc['bytes']<0:
        raise ValueError('invalid artifact descriptor')
    path=Path(desc['path'])
    if not path.is_absolute(): path=Path(base)/path
    if path.stat().st_size != desc['bytes'] or file_hash(path) != desc.get('sha256'):
        raise ValueError('artifact size or digest mismatch: '+str(path))
    return path


def _positive(value):
    return type(value) is int and value>0


def validate_tree(tree, base='.'):
    if tree.get('format_version') != 1 or not _positive(tree.get('tree_generation')):
        raise ValueError('unsupported tree format or generation')
    if not isinstance(tree.get('construction_epoch'),str) or not tree['construction_epoch']:
        raise ValueError('construction epoch required')
    if not _positive(tree.get('dimension')) or tree.get('metric') not in ('l2','cosine','ip'):
        raise ValueError('invalid dimension or metric')
    source=tree.get('source',{})
    if not source.get('source_id') or not _positive(source.get('row_count')):
        raise ValueError('dense global source prefix required')
    model_path=checked_artifact(tree['model'],base)
    model=json.loads(model_path.read_text())
    for key in ('construction_epoch','dimension','metric'):
        if model.get(key)!=tree[key]: raise ValueError('model/tree '+key+' mismatch')
    centers=model.get('centroids',[])
    if not centers or any(len(c)!=tree['dimension'] for c in centers): raise ValueError('invalid frozen centroids')
    import math
    if any(not math.isfinite(x) for c in centers for x in c): raise ValueError('nonfinite centroid')
    children=tree.get('upper_root',{}).get('children')
    subtrees=tree.get('subtrees')
    if tree.get('upper_root',{}).get('kind')!='centroid_root' or not isinstance(subtrees,list) or not subtrees or not isinstance(children,list):
        raise ValueError('shared centroid root and subtree descriptors required')
    expected=[dict(subtree_id=s.get('subtree_id'),centroid_index=s.get('centroid_index')) for s in subtrees]
    if children!=expected or len(subtrees)!=len(centers): raise ValueError('upper boundary slots must match subtrees and centroids')
    ids=[s.get('subtree_id') for s in subtrees]
    if any(not isinstance(i,str) or not i for i in ids) or len(set(ids))!=len(ids): raise ValueError('duplicate/invalid subtree slots')
    if sorted(s.get('centroid_index',-1) for s in subtrees)!=list(range(len(centers))): raise ValueError('centroid slots must be a permutation')
    total=source['row_count']; seen=0; block_contents_complete=True
    # A disk-backed bitmap uses N/8 bytes and bounded row buffers. Unique IDs
    # within [0,N), together with N rows, prove complete dense-prefix coverage.
    with tempfile.TemporaryFile() as bitmap_file:
        bitmap_file.truncate((total+7)//8)
        with mmap.mmap(bitmap_file.fileno(),0) as bitmap:
            for subtree in subtrees:
                count=subtree.get('vector_count')
                if not _positive(count): raise ValueError('empty or invalid owned subtree')
                mapping=checked_artifact(subtree['local_to_global'],base)
                if mapping.stat().st_size!=8*count: raise ValueError('ID map must contain exactly one u64le per local row')
                with mapping.open('rb') as stream:
                    while True:
                        data=stream.read(8*65536)
                        if not data: break
                        values=array.array('Q'); values.frombytes(data)
                        if sys.byteorder!='little': values.byteswap()
                        for ident in values:
                            if ident>=total: raise ValueError('mapped global ID outside dense source prefix')
                            index,bit=divmod(ident,8);mask=1<<bit
                            if bitmap[index]&mask: raise ValueError('duplicate global ownership')
                            bitmap[index]=bitmap[index]|mask;seen+=1
                snapshot_path=checked_artifact(subtree['snapshot'],base)
                snapshot=json.loads(snapshot_path.read_text())
                if not _positive(subtree.get('generation')) or subtree['generation']>tree['tree_generation']: raise ValueError('invalid lower generation')
                if snapshot.get('source_id')!=source['source_id']: raise ValueError('lower source identity mismatch')
                boundary=content_hash(dict(model_sha256=tree['model']['sha256'],centroid_index=subtree['centroid_index']))
                if subtree.get('routing_boundary_hash')!=boundary or snapshot.get('routing_boundary_hash')!=boundary: raise ValueError('lower ownership boundary hash mismatch')
                for key,want in [('generation',subtree.get('generation')),('dimension',tree['dimension']),('metric',tree['metric']),('vector_count',count)]:
                    if snapshot.get(key)!=want: raise ValueError('lower snapshot '+key+' mismatch')
                if snapshot.get('subtree_id',snapshot.get('shard_id'))!=subtree['subtree_id']:
                    raise ValueError('lower snapshot owns a different slot')
                if not isinstance(snapshot.get('block_ids'),dict) or not snapshot['block_ids']:
                    raise ValueError('persisted lower block references required')
                root=subtree.get('root_ref')
                if not isinstance(root,dict) or root.get('kind') not in ('leaf','internal') or type(root.get('id')) is not int or root['id']<0:
                    raise ValueError('explicit lower root kind and ID required')
                if snapshot.get('root_ref')!=root: raise ValueError('lower root differs from persisted snapshot')
                persisted_map=snapshot.get('id_mapping',{})
                if any(persisted_map.get(key)!=subtree['local_to_global'][key] for key in ('sha256','bytes')) or persisted_map.get('count')!=count:
                    raise ValueError('published map must match the persisted lower-reader map')
                blocks=subtree.get('block_artifacts')
                if blocks is None: block_contents_complete=False
                else:
                    if not isinstance(blocks,dict) or set(blocks)!=set(snapshot['block_ids']): raise ValueError('local block artifact set incomplete')
                    for desc in blocks.values(): checked_artifact(desc,base)
    if seen!=total: raise ValueError('ID mappings do not cover complete dense source prefix')
    return dict(tree_sha256=content_hash(tree),vector_count=seen,subtrees=len(subtrees),
                validation_scope='local model/maps/snapshot bytes and global ownership; lower block bytes only if provided',
                lower_block_contents_verified=block_contents_complete,
                lower_reader_open_required=True)


def validate_placement(placement, tree):
    if placement.get('format_version')!=1 or placement.get('tree_sha256')!=content_hash(tree) or not _positive(placement.get('placement_epoch')):
        raise ValueError('placement must reference the exact committed tree')
    owners=placement.get('owners',{})
    if not isinstance(owners,dict) or set(owners)!={s['subtree_id'] for s in tree['subtrees']}:
        raise ValueError('placement must cover every ownership slot exactly')
    for owner in owners.values():
        if not isinstance(owner,dict) or not isinstance(owner.get('owner_id'),str) or not owner['owner_id'] or not isinstance(owner.get('endpoint'),str) or not owner['endpoint'].startswith(('http://','https://')):
            raise ValueError('owner identity and HTTP endpoint required')
    return placement


def assemble_tree(prepared_path,snapshot_paths,tree_generation,base='.'):
    """Bind a prepared shared root to actual persisted reader maps and roots."""
    prepared_path=Path(prepared_path);prepared=json.loads(prepared_path.read_text());base=Path(base)
    snapshots={}
    for path in snapshot_paths:
        path=Path(path);row=json.loads(path.read_text());slot=row.get('subtree_id',row.get('shard_id'))
        if slot in snapshots:raise ValueError('duplicate lower snapshot slot')
        snapshots[slot]=(path,row)
    if set(snapshots)!={s['subtree_id'] for s in prepared['subtrees']}:raise ValueError('snapshot slots differ from prepared boundary')
    subtrees=[]
    for slot in prepared['subtrees']:
        path,row=snapshots[slot['subtree_id']];mapping=row['id_mapping'];map_path=Path(mapping['path'])
        if not map_path.is_absolute():map_path=path.parent/map_path
        map_desc=artifact(map_path,base)
        if map_desc['sha256']!=slot['local_to_global']['sha256']:raise ValueError('persisted map differs from prepared ownership')
        subtrees.append(dict(subtree_id=slot['subtree_id'],centroid_index=slot['centroid_index'],routing_boundary_hash=slot['routing_boundary_hash'],
            generation=row['generation'],vector_count=row['vector_count'],snapshot=artifact(path,base),local_to_global=map_desc,root_ref=row['root_ref']))
    result=dict(format_version=1,tree_generation=tree_generation,construction_epoch=prepared['construction_epoch'],dimension=prepared['dimension'],metric=prepared['metric'],
        source=dict(source_id=prepared['source_id'],row_count=prepared['global_row_count']),model=artifact(checked_artifact(prepared['model'],prepared_path.parent),base),
        upper_root=prepared['upper_root'],subtrees=subtrees)
    validate_tree(result,base)
    return result


def load_tree(path, validate=True):
    """Load a content manifest (not the current pointer); paths resolve at its parent."""
    path=Path(path); tree=json.loads(path.read_text())
    if validate: validate_tree(tree,path.parent)
    return tree


def load_published(pointer_path,validate=True):
    pointer_path=Path(pointer_path);pointer=json.loads(pointer_path.read_text())
    tree=json.loads((pointer_path.parent/pointer['manifest']).read_text())
    if content_hash(tree)!=pointer['tree_sha256'] or tree['tree_generation']!=pointer['tree_generation']:
        raise ValueError('published tree pointer mismatch')
    base=(pointer_path.parent/pointer['artifact_base']).resolve()
    if validate:validate_tree(tree,base)
    return tree,base


def load_placement(path,tree):
    return validate_placement(json.loads(Path(path).read_text()),tree)


def load_model(tree,base='.'):
    return json.loads(checked_artifact(tree['model'],base).read_text())


def _atomic_write(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    fd,temp=tempfile.mkstemp(prefix='.'+path.name+'-',dir=path.parent)
    try:
        with os.fdopen(fd,'wb') as stream:
            stream.write(data);stream.flush();os.fsync(stream.fileno())
        os.replace(temp,path)
        parent=os.open(path.parent,os.O_RDONLY)
        try:os.fsync(parent)
        finally:os.close(parent)
    finally:
        if os.path.exists(temp):os.unlink(temp)


def publish(path,tree,expected_old_hash=None,base='.'):
    """One writer lock serializes CAS; mismatches leave the old pointer intact."""
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with open(str(path)+'.lock','a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        old=json.loads(path.read_text()) if path.exists() else None
        actual=old['tree_sha256'] if old else None
        if actual!=expected_old_hash: raise ValueError('publication compare-and-swap conflict')
        report=validate_tree(tree,base)
        if old:
            oldtree=json.loads((path.parent/old['manifest']).read_text())
            if content_hash(oldtree)!=actual: raise ValueError('previous published manifest digest mismatch')
            if tree['tree_generation']<=oldtree['tree_generation']: raise ValueError('generation must increase')
            if tree['source']['source_id']!=oldtree['source']['source_id'] or tree['source']['row_count']<oldtree['source']['row_count']: raise ValueError('source identity changed or source prefix shrank')
            checked_artifact(oldtree['model'],path.parent/old.get('artifact_base','.'))
            if any(tree['model'][key]!=oldtree['model'][key] for key in ('sha256','bytes')): raise ValueError('ownership boundary model is immutable within construction epoch')
            for key in ('construction_epoch','dimension','metric','upper_root'):
                if tree[key]!=oldtree[key]: raise ValueError('ownership boundary model is immutable within construction epoch')
            oldslots={s['subtree_id']:s for s in oldtree['subtrees']}
            for subtree in tree['subtrees']:
                previous=oldslots.get(subtree['subtree_id'])
                if previous is None: raise ValueError('ownership slots cannot change')
                oldmap=checked_artifact(previous['local_to_global'],path.parent/old.get('artifact_base','.'))
                newmap=checked_artifact(subtree['local_to_global'],base)
                if newmap.stat().st_size<oldmap.stat().st_size: raise ValueError('ID map cannot shrink')
                with oldmap.open('rb') as a,newmap.open('rb') as b:
                    for block in iter(lambda:a.read(8*1024*1024),b''):
                        if b.read(len(block))!=block: raise ValueError('append must preserve local-to-global mapping prefix')
        digest=report['tree_sha256'];name='tree-'+digest+'.json';target=path.parent/name
        payload=canonical_bytes(tree)
        if target.exists() and target.read_bytes()!=payload: raise ValueError('immutable manifest collision')
        if not target.exists():_atomic_write(target,payload)
        pointer=dict(tree_sha256=digest,manifest=name,tree_generation=tree['tree_generation'],artifact_base=os.path.relpath(Path(base).resolve(),path.parent.resolve()),validation=report)
        _atomic_write(path,canonical_bytes(pointer))
        return pointer


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('command',choices=('validate','publish'));p.add_argument('--tree',required=True);p.add_argument('--pointer');p.add_argument('--expected-old-hash');a=p.parse_args()
    source=Path(a.tree);tree=json.loads(source.read_text())
    if a.command=='publish':
        if not a.pointer:p.error('--pointer required')
        result=publish(a.pointer,tree,a.expected_old_hash,source.parent)
    else:result=validate_tree(tree,source.parent)
    print(json.dumps(result,indent=2))

if __name__=='__main__':main()
