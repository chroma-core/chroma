"""Reduce validated subtree contributions to an owner's exact global-budget prefix.

A row outside an owner's first M has M rows ahead of it under the global
(distance, external ID) order, so it cannot enter the collection's first M.
The published tree proves disjoint map ownership; every returned candidate is
checked against those pinned maps before any contribution can be discarded.
"""
import hashlib
import math
import mmap
import os
from pathlib import Path
import stat
import struct


class PinnedMap:
    def __init__(self,directory,snapshot):
        descriptor=snapshot['id_mapping'];relative=Path(descriptor['path'])
        if relative.is_absolute() or len(relative.parts)!=1:raise ValueError('map must be one owned filename')
        self.generation=snapshot['generation'];self.metric=snapshot['metric'];self.dimension=snapshot['dimension'];self.source_id=snapshot['source_id']
        self.count=snapshot['vector_count'];self.bytes=self.count*8;self.file=None;self.mapping=None
        if descriptor['count']!=self.count or descriptor['bytes']!=self.bytes:raise ValueError('pinned map count differs')
        try:
            fd=os.open(Path(directory)/relative,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0));self.file=os.fdopen(fd,'rb')
            metadata=os.fstat(self.file.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size!=self.bytes:raise ValueError('pinned map file length differs')
            digest=hashlib.sha256()
            for block in iter(lambda:self.file.read(1024*1024),b''):digest.update(block)
            if digest.hexdigest()!=descriptor['sha256']:raise ValueError('pinned map checksum differs')
            self.mapping=mmap.mmap(self.file.fileno(),0,access=mmap.ACCESS_READ)
        except BaseException:
            self.close();raise

    def get(self,local):
        if type(local) is not int or not 0<=local<self.count:raise ValueError('candidate outside pinned local ID range')
        return struct.unpack_from('<Q',self.mapping,local*8)[0]

    def close(self):
        if self.mapping is not None:self.mapping.close();self.mapping=None
        if self.file is not None:self.file.close();self.file=None


def result_generation(response):
    return response.get('result',{}).get('generation')


def reduce_owner_batch(requests,responses,mappings):
    operations=[item['payload'].get('op') for item in requests]
    if 'score_selected_leaves' not in operations:return responses,None
    if any(op!='score_selected_leaves' for op in operations):raise ValueError('candidate reduction requires one homogeneous score phase')
    budget=requests[0]['payload'].get('candidate_budget');query=requests[0]['payload'].get('query')
    if type(budget) is not int or not 1<=budget<=1000000:raise ValueError('invalid global candidate budget')
    if len(requests)!=len(responses):raise ValueError('incomplete owner contribution set')
    best={};counts=[];scored=0;policy=None
    for index,(request,response) in enumerate(zip(requests,responses)):
        sid=request['subtree_id'];payload=request['payload']
        if payload.get('candidate_budget')!=budget or payload.get('query')!=query:raise ValueError('owner batch mixes query policies')
        if response.get('subtree_id')!=sid or response.get('snapshot_sha256')!=request['snapshot_sha256']:raise ValueError('owner batch mixes immutable snapshots')
        mapping=mappings[sid];current=(mapping.metric,mapping.dimension,mapping.source_id)
        if policy is None:policy=current
        if current!=policy or result_generation(response)!=mapping.generation or payload.get('generation',mapping.generation)!=mapping.generation:raise ValueError('owner batch mixes pinned generation or metric/source identity')
        if not isinstance(query,list) or len(query)!=mapping.dimension or any(type(v) not in (int,float) or not math.isfinite(v) for v in query):raise ValueError('invalid common query')
        result=response['result'];rows=result.get('candidates');unique=result.get('unique_candidates_scanned')
        if not isinstance(rows,list) or len(rows)>budget or result.get('actual_candidate_count')!=len(rows) or result.get('candidate_budget')!=budget:raise ValueError('invalid subtree contribution count')
        if type(unique) is not int or unique<0:raise ValueError('live unique scoring count required')
        local_seen=set()
        for row in rows:
            local=row.get('local_id');ident=mappings[sid].get(local);distance=row.get('distance')
            if type(row.get('id')) is not int or row['id']!=ident or type(distance) not in (int,float) or not math.isfinite(distance):raise ValueError('candidate differs from pinned ownership or has nonfinite distance')
            local_seen.add(local)
            previous=best.get(ident)
            if previous is not None and previous[0]!=index:raise ValueError('duplicate global ownership across subtrees')
            if previous is None or distance<previous[1]['distance']:best[ident]=(index,row)
        if unique<len(local_seen):raise ValueError('live unique count understates returned IDs')
        counts.append(len(rows));scored+=unique
    selected=sorted(best.values(),key=lambda pair:(pair[1]['distance'],pair[1]['id']))[:budget]
    buckets=[[] for _ in responses]
    for index,row in selected:buckets[index].append(row)
    output=[]
    for index,response in enumerate(responses):
        result=dict(response['result'],candidates=buckets[index],actual_candidate_count=len(buckets[index]),candidate_contributions_before_owner_reduction=counts[index],owner_reduction_version=1)
        output.append(dict(response,result=result))
    return output,dict(version=1,scope='adapter owner',candidate_budget=budget,subtree_contributions_before_reduction=sum(counts),owner_unique_candidates_before_reduction=len(best),wire_candidates_returned=len(selected),live_unique_candidates_scored=scored,tie_order='approximate_distance_then_external_global_id',raw_physical_postings_scored=None)
