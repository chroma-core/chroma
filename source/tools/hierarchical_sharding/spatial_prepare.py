"""Freeze a sampled centroid root, materialize owned subtrees, and route appends.

A centroid child is a real shared-tree boundary slot, not an independent search.
The coordinator pools frontier nodes globally after crossing these slots.
"""
import argparse
import json
import math
from pathlib import Path
import time
import numpy as np
from .tree_manifest import artifact, checked_artifact, canonical_bytes, file_hash, content_hash


def distances(vectors,centers,metric):
    x=np.asarray(vectors,dtype=np.float64);c=np.asarray(centers,dtype=np.float64)
    dots=x@c.T
    if metric=='l2': return np.maximum(np.einsum('ij,ij->i',x,x)[:,None]+np.einsum('ij,ij->i',c,c)[None,:]-2*dots,0)
    if metric=='ip': return 1-dots
    if metric!='cosine': raise ValueError('unknown metric')
    norms=np.linalg.norm(x,axis=1);cnorms=np.linalg.norm(c,axis=1)
    if np.any(norms==0) or np.any(cnorms==0): raise ValueError('cosine requires nonzero vectors and centroids')
    return 1-dots/(norms[:,None]*cnorms[None,:])


def fit_model(source,dimension,children,epoch,metric='l2',seed=1729,sample_size=8192,iterations=12):
    if min(dimension,children,sample_size,iterations)<1 or not epoch or metric not in ('l2','ip','cosine'):
        raise ValueError('invalid construction settings')
    source=Path(source);size=source.stat().st_size
    if size%(4*dimension): raise ValueError('truncated source row')
    n=size//(4*dimension)
    if n<children or sample_size<children: raise ValueError('at least one sample per boundary child required')
    vectors=np.memmap(source,dtype='<f4',mode='r',shape=(n,dimension))
    rng=np.random.default_rng(seed);indices=np.sort(rng.choice(n,size=min(n,sample_size),replace=False))
    sample=np.asarray(vectors[indices],dtype=np.float64)
    if not np.isfinite(sample).all(): raise ValueError('nonfinite sample vector')
    centers=sample[rng.choice(len(sample),size=children,replace=False)].copy()
    if metric in ('cosine','ip'):
        norms=np.linalg.norm(centers,axis=1)
        if np.any(norms==0): raise ValueError('zero initial centroid')
        centers/=norms[:,None]
    # Assignment ties choose the lowest slot. Empty sampled clusters retain
    # their previous centroid; empty full-corpus ownership is rejected later.
    for _ in range(iterations):
        assignment=np.argmin(distances(sample,centers,metric),axis=1)
        new=centers.copy()
        for slot in range(children):
            selected=sample[assignment==slot]
            if len(selected):
                mean=selected.mean(axis=0)
                norm=np.linalg.norm(mean)
                if metric in ('cosine','ip'):
                    if norm==0:continue
                    mean/=norm
                new[slot]=mean
        centers=new
    # Freeze actual float32 centroids; both routing and upper-tree traversal
    # read these same values, rather than an unrecorded float64 model.
    centers=centers.astype('<f4')
    return dict(format_version=1,construction_epoch=epoch,dimension=dimension,metric=metric,
                seed=seed,sample_size=len(sample),sample_row_ids=indices.tolist(),iterations=iterations,
                algorithm='seeded Lloyd centroid clustering; spherical centroids for cosine/IP',
                tie_policy='lowest centroid slot',centroids=centers.tolist())


def prepare(source,output,dimension,children=2,construction_epoch='initial',metric='l2',source_id=None,
            seed=1729,sample_size=8192,iterations=12,previous=None,model_path=None,start_id=0,block_rows=4096):
    started=time.monotonic()
    if not source_id or type(start_id) is not int or start_id<0 or block_rows<1: raise ValueError('source identity and valid range required')
    source,output=Path(source),Path(output)
    size=source.stat().st_size
    if dimension<1 or not size or size%(4*dimension): raise ValueError('complete nonempty float32 corpus required')
    n=size//(4*dimension);old=None;previous_base=None
    if previous:
        previous=Path(previous);previous_base=previous.parent;old=json.loads(previous.read_text())
        if start_id!=old['global_row_count'] or source_id!=old['source_id']: raise ValueError('append must extend the same dense source prefix')
        original_model=checked_artifact(old['model'],previous_base)
        if model_path is not None and file_hash(model_path)!=old['model']['sha256']: raise ValueError('append cannot change frozen boundary model')
        model=json.loads(original_model.read_text())
        if (dimension,metric,construction_epoch)!=(model['dimension'],model['metric'],model['construction_epoch']): raise ValueError('append model settings differ')
        children=len(model['centroids'])
    else:
        if start_id!=0 or model_path is not None: raise ValueError('initial epoch starts at zero and fits its own model')
        model=fit_model(source,dimension,children,construction_epoch,metric,seed,sample_size,iterations)
    output.mkdir(parents=True,exist_ok=False)
    (output/'model.json').write_bytes(canonical_bytes(model))
    vector_streams=[];map_streams=[];counts=[0]*children;oldcounts=[0]*children
    try:
        for slot in range(children):
            folder=output/f'subtree-{slot}';folder.mkdir()
            vector_streams.append((folder/'vectors.f32').open('wb'))
            maps=(folder/'local-to-global.u64le').open('wb');map_streams.append(maps)
            if old:
                prior=old['subtrees'][slot]
                if prior['subtree_id']!=str(slot): raise ValueError('ownership slot order changed')
                oldmap=checked_artifact(prior['local_to_global'],previous_base)
                oldcounts[slot]=prior['vector_count']
                if oldmap.stat().st_size!=8*oldcounts[slot]:raise ValueError('prior mapping count mismatch')
                with oldmap.open('rb') as stream:
                    for block in iter(lambda:stream.read(8*1024*1024),b''):maps.write(block)
        vectors=np.memmap(source,dtype='<f4',mode='r',shape=(n,dimension))
        for offset in range(0,n,block_rows):
            x=np.asarray(vectors[offset:offset+block_rows])
            if not np.isfinite(x).all():raise ValueError('nonfinite corpus vector')
            assigned=np.argmin(distances(x,model['centroids'],metric),axis=1)
            ids=np.arange(start_id+offset,start_id+offset+len(x),dtype='<u8')
            for slot in range(children):
                selected=assigned==slot
                vector_streams[slot].write(x[selected].astype('<f4',copy=False).tobytes())
                map_streams[slot].write(ids[selected].tobytes());counts[slot]+=int(selected.sum())
        if any(oldcounts[i]+counts[i]==0 for i in range(children)):raise ValueError('empty owned subtree; choose fewer centroid children')
    finally:
        for stream in vector_streams+map_streams:stream.close()
    descriptors=[]
    for slot in range(children):
        folder=output/f'subtree-{slot}'
        descriptors.append(dict(subtree_id=str(slot),centroid_index=slot,routing_boundary_hash=content_hash(dict(model_sha256=file_hash(output/'model.json'),centroid_index=slot)),vector_count=oldcounts[slot]+counts[slot],
            append_row_count=counts[slot],local_start=oldcounts[slot],
            vectors=artifact(folder/'vectors.f32',output),local_to_global=artifact(folder/'local-to-global.u64le',output)))
    result=dict(format_version=1,construction_epoch=construction_epoch,dimension=dimension,metric=metric,source_id=source_id,
        source=artifact(source),start_id=start_id,input_row_count=n,global_row_count=start_id+n,
        model=artifact(output/'model.json',output),upper_root=dict(kind='centroid_root',children=[dict(subtree_id=str(i),centroid_index=i) for i in range(children)]),
        subtrees=descriptors,preparation_wall_s=time.monotonic()-started,
        routing_policy='nearest frozen shared-root centroid; ties lowest slot; no cross-owner reassignment',
        previous_preparation=artifact(previous) if previous else None)
    (output/'input.json').write_bytes(canonical_bytes(result))
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('source','output','source-id','construction-epoch'):p.add_argument('--'+key,required=True)
    p.add_argument('--dimension',type=int,required=True);p.add_argument('--metric',choices=('l2','ip','cosine'),default='l2')
    for key,default in [('children',2),('seed',1729),('sample-size',8192),('iterations',12),('start-id',0),('block-rows',4096)]:p.add_argument('--'+key,type=int,default=default)
    p.add_argument('--previous');p.add_argument('--model-path');a=p.parse_args()
    print(json.dumps(prepare(a.source,a.output,a.dimension,a.children,a.construction_epoch,a.metric,a.source_id,a.seed,a.sample_size,a.iterations,a.previous,a.model_path,a.start_id,a.block_rows)))

if __name__=='__main__':main()
