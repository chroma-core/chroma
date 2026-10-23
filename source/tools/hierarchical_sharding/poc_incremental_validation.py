"""Fixed-immutable-input POC: validate owner deltas, not a publication fast path.

The session begins with a separately completed full parent oracle. It keeps
owner descriptors, not all row maps. Legacy page/trie and record sidecars remain
separate full-oracle costs; this module returns an experimental ownership view.
"""
import copy
import json
import math
import struct
from pathlib import Path
from .tree_manifest import canonical_bytes,content_hash,checked_artifact,load_model,routing_boundary_hash


def payload(slot):
    return {k:slot[k] for k in ('subtree_id','generation','vector_count','snapshot','local_to_global','root_ref','routing_boundary_hash','ownership_revision')}


def mapped(slot):
    count=slot['vector_count']
    if type(count) is not int or not 0<count<=128:raise ValueError('bounded owner required')
    data=checked_artifact(slot['local_to_global'],'.').read_bytes()
    if len(data)!=count*8:raise ValueError('owner map count differs')
    values=[x[0] for x in struct.iter_unpack('<Q',data)]
    if len(values)!=len(set(values)):raise ValueError('duplicate owner ID')
    return values


def slots(record):return {s['subtree_id']:s for s in record['tree']['subtrees']}


class FixedInputSession:
    """Internal experiment state; no serialized receipt or production entry point."""
    def __init__(self,parent,full_oracle):
        if full_oracle.get('tree_sha256')!=parent['tree_sha256'] or full_oracle.get('vector_count')!=parent['tree']['source']['row_count']:
            raise ValueError('exact bootstrap oracle required')
        self.parent=copy.deepcopy(parent)

    def transition(self,candidate,kind,affected,audit):
        parent=self.parent;old=slots(parent);new=slots(candidate);affected=set(affected)
        if kind not in ('append','topology') or not affected or not affected<=set(old):raise ValueError('explicit affected parent owners required')
        if candidate['parent_sha256']!=content_hash(parent) or candidate['fence']!=parent['fence']+1:raise ValueError('candidate parent differs')
        before=parent['tree'];after=candidate['tree'];n=before['source']['row_count'];total=after['source']['row_count']
        if not 0<n<=total<=128 or any(before[k]!=after[k] for k in ('dimension','metric','construction_epoch')) or before['source']['source_id']!=after['source']['source_id']:raise ValueError('fixed source/configuration differs')
        old_model=load_model(before);new_model=load_model(after)
        for tree,model in ((before,old_model),(after,new_model)):
            if tree['upper_root']['children']!=[dict(subtree_id=s['subtree_id'],centroid_index=s['centroid_index']) for s in tree['subtrees']]:raise ValueError('routing slots differ')
            if any(len(center)!=tree['dimension'] or any(not math.isfinite(v) for v in center) for center in model['centroids']):raise ValueError('invalid boundary centroid')
            for slot in tree['subtrees']:
                if slot['routing_boundary_hash']!=routing_boundary_hash(tree,model,slot['centroid_index']):raise ValueError('boundary content hash differs')
            indices=[s['centroid_index'] for s in tree['subtrees']]
            if sorted(indices)!=list(range(len(model['centroids']))):raise ValueError('routing permutation differs')
        unchanged=set(old)-affected
        for sid in unchanged:
            if sid not in new or canonical_bytes(payload(old[sid]))!=canonical_bytes(payload(new[sid])):raise ValueError('unchanged owner payload differs')
            if canonical_bytes(old_model['centroids'][old[sid]['centroid_index']])!=canonical_bytes(new_model['centroids'][new[sid]['centroid_index']]):raise ValueError('unchanged boundary centroid differs')
        changed=set(new)-unchanged
        old_maps={sid:mapped(old[sid]) for sid in sorted(affected)}
        new_maps={sid:mapped(new[sid]) for sid in sorted(changed)}
        old_ids=[i for values in old_maps.values() for i in values];new_ids=[i for values in new_maps.values() for i in values]
        if len(old_ids)!=len(set(old_ids)) or len(new_ids)!=len(set(new_ids)):raise ValueError('affected ownership overlaps')
        if kind=='append':
            if total<=n or set(new)!=set(old) or changed!=affected:raise ValueError('append owners/range differ')
            additions=[]
            for sid in affected:
                if new_maps[sid][:len(old_maps[sid])]!=old_maps[sid]:raise ValueError('append changes old local prefix')
                additions+=new_maps[sid][len(old_maps[sid]):]
            if sorted(additions)!=list(range(n,total)):raise ValueError('append new range differs')
        elif total!=n or set(old_ids)!=set(new_ids):raise ValueError('topology affected union differs')
        results={sid:audit(sid,new[sid],candidate['owners'][sid],new_maps[sid]) for sid in sorted(changed)}
        if sum(s['vector_count'] for s in new.values())!=total:raise ValueError('complete candidate count differs')
        self.parent=copy.deepcopy(candidate)
        return dict(tree_sha256=candidate['tree_sha256'],vector_count=total,subtrees=len(new),changed_owners=sorted(changed),unchanged_owners=sorted(unchanged),affected_old_rows=len(old_ids),audited_new_rows=len(new_ids),owner_payloads={sid:payload(s) for sid,s in new.items()},native=results,scope='experimental ownership view; legacy pages/trie/record oracle separate; fixed inputs assumed immutable')
