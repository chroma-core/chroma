"""Chunked FineWeb preparation on worker NVMe, with verified ingest resumption.

Ingest resumes at committed source cursors. UUID audit, model fitting, and owner
routing restart as bounded local stages. No source range cache is retained.
"""
import argparse
import json
import os
from pathlib import Path
import random
import resource
import re
import sqlite3
import time
import numpy as np
from .fineweb_prepare import DATASET,REVISION,DIMENSION,RangeReader,decode_batch,DECODE_EPOCH
from .spatial_prepare import distances,fit_model
from .tree_manifest import artifact,canonical_bytes,checked_artifact,content_hash,file_hash,_atomic_write


def _json(path,value):_atomic_write(path,canonical_bytes(value))


def _verify_chunk(root,chunk,expected_start):
    if chunk['start_id']!=expected_start or chunk['row_count']<1:raise ValueError('chunk coverage is not a dense prefix')
    vectors=checked_artifact(chunk['vectors'],root);checked_artifact(chunk['uuids'],root)
    if vectors.stat().st_size!=chunk['row_count']*DIMENSION*4:raise ValueError('chunk vector length mismatch')
    return expected_start+chunk['row_count']


def _source_batches(manifest,cursor,budget,batch_rows,open_source,audit):
    import pyarrow as pa
    import pyarrow.parquet as pq
    for file_index in range(cursor['file_index'],len(manifest['files'])):
        entry=manifest['files'][file_index];skip=cursor['row_offset'] if file_index==cursor['file_index'] else 0
        source=open_source(entry) if open_source else RangeReader(entry['url'],entry['bytes'],budget=budget)
        timings=dict(metadata_s=0.,read_decode_including_http_s=0.,primitive_conversion_s=0.,yielded_rows=0);metadata_started=time.monotonic()
        try:
            parquet=pq.ParquetFile(source);schema=parquet.schema_arrow;t=schema.field('dense_embedding').type
            if 'parquet_rows' in entry and parquet.metadata.num_rows!=entry['parquet_rows']:raise ValueError('pinned parquet row count changed')
            if not (pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t)) or not pa.types.is_float16(t.value_type):raise ValueError('source dense values must be float16 lists')
            if not (pa.types.is_string(schema.field('id').type) or pa.types.is_large_string(schema.field('id').type)):raise ValueError('source UUIDs must be strings')
            if skip>parquet.metadata.num_rows:raise ValueError('resume source cursor exceeds file')
            timings['metadata_s']=time.monotonic()-metadata_started
            file_row=0
            for group in range(parquet.num_row_groups):
                population=parquet.metadata.row_group(group).num_rows
                if file_row+population<=skip:file_row+=population;continue
                read_started=time.monotonic()
                for batch in parquet.iter_batches(batch_size=batch_rows,row_groups=[group],columns=['dense_embedding','id'],use_threads=False):
                    timings['read_decode_including_http_s']+=time.monotonic()-read_started
                    conversion_started=time.monotonic();trim=max(0,skip-file_row);original=batch.num_rows
                    if trim<original:
                        x,ids=decode_batch(batch.slice(trim))
                        timings['primitive_conversion_s']+=time.monotonic()-conversion_started;timings['yielded_rows']+=len(x)
                        yield x,ids,dict(file_index=file_index,row_offset=file_row+trim),parquet.metadata.num_rows
                    file_row+=original;read_started=time.monotonic()
        finally:
            audit.write(json.dumps(dict(file_index=file_index,source=entry,ranges=getattr(source,'ranges',None),timings=timings))+'\n');audit.flush();source.close()


def _audit_uuids(root,chunks,max_bytes=None):
    # This index is reconstructible from committed UUID chunks. A failed audit
    # removes only its owned temporary database; committed chunks stay intact.
    path=root/'uuid-to-row.sqlite.partial'
    if path.exists():path.unlink()
    db=sqlite3.connect(path)
    try:
        db.execute('PRAGMA cache_size=-65536')
        if max_bytes is not None:
            if max_bytes<4096:raise ValueError('insufficient disk budget for UUID audit')
            db.execute('PRAGMA journal_mode=OFF')  # Owned partial index is rebuilt after failure.
            db.execute('PRAGMA page_size=4096')
            db.execute(f'PRAGMA max_page_count={int(max_bytes)//4096}')
        db.execute('CREATE TABLE documents (source_uuid TEXT PRIMARY KEY, global_id INTEGER NOT NULL UNIQUE) WITHOUT ROWID')
        for chunk in chunks:
            cursor=chunk['start_id'];rows=[]
            with checked_artifact(chunk['uuids'],root).open() as stream:
                for line in stream:
                    row=json.loads(line)
                    if row['global_id']!=cursor or not isinstance(row['source_uuid'],str) or not row['source_uuid']:raise ValueError('UUID chunk row ordering mismatch')
                    rows.append((row['source_uuid'],cursor));cursor+=1
                    if len(rows)==4096:db.executemany('INSERT INTO documents VALUES (?,?)',rows);db.commit();rows=[]
            if rows:db.executemany('INSERT INTO documents VALUES (?,?)',rows);db.commit()
            if cursor!=chunk['start_id']+chunk['row_count']:raise ValueError('UUID chunk count mismatch')
    finally:db.close()
    with path.open('rb') as completed:os.fsync(completed.fileno())
    os.replace(path,root/'uuid-to-row.sqlite')
    return artifact(root/'uuid-to-row.sqlite',root)


def _whole_prefix_model(root,chunks,config):
    count=config['count'];indices=sorted(random.Random(config['seed']).sample(range(count),min(count,config['sample_size'])))
    sample_path=root/'sample.f32';slot=0
    with sample_path.open('wb') as output:
        for chunk in chunks:
            end=chunk['start_id']+chunk['row_count']
            with checked_artifact(chunk['vectors'],root).open('rb') as source:
                while slot<len(indices) and indices[slot]<end:
                    source.seek((indices[slot]-chunk['start_id'])*DIMENSION*4);row=source.read(DIMENSION*4)
                    if len(row)!=DIMENSION*4:raise ValueError('truncated sampled row')
                    output.write(row);slot+=1
    if slot!=len(indices):raise ValueError('whole-prefix sample coverage failed')
    model=fit_model(sample_path,DIMENSION,config['children'],config['epoch'],config['metric'],config['seed'],len(indices),config['iterations'])
    model.update(sample_row_ids=indices,sample_selection='Python seeded sample without replacement across complete global prefix',global_prefix_count=count,sample_artifact=artifact(sample_path,root))
    _json(root/'model.json',model)
    return model


def _route(root,chunks,config,model,batch_rows):
    if (root/'owners').exists():
        # Preserve failed local route output rather than discarding prepared
        # data. A user can inspect/remove these named attempts after validation.
        number=0
        while (root/f'owners-incomplete-{number}').exists():number+=1
        if number>=2:raise ValueError('two incomplete routing attempts retained; inspect storage before another restart')
        os.replace(root/'owners',root/f'owners-incomplete-{number}')
    owners=root/'owners';owners.mkdir()
    needed=2*config['children']+64;soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft<needed:
        if hard<needed:raise ValueError('open-file limit cannot accommodate requested ownership slots')
        resource.setrlimit(resource.RLIMIT_NOFILE,(needed,hard))
    streams=[];maps=[];counts=[0]*config['children']
    try:
        for child in range(config['children']):
            folder=owners/str(child);folder.mkdir();streams.append((folder/'vectors.f32').open('wb',buffering=131072));maps.append((folder/'local-to-global.u64le').open('wb',buffering=131072))
        for chunk in chunks:
            with checked_artifact(chunk['vectors'],root).open('rb') as source:
                offset=0
                while True:
                    raw=source.read(batch_rows*DIMENSION*4)
                    if not raw:break
                    x=np.frombuffer(raw,dtype='<f4').reshape(-1,DIMENSION);assignment=np.argmin(distances(x,model['centroids'],config['metric']),axis=1)
                    ids=np.arange(chunk['start_id']+offset,chunk['start_id']+offset+len(x),dtype='<u8')
                    for child in np.unique(assignment):
                        selected=assignment==child;streams[child].write(x[selected].tobytes());maps[child].write(ids[selected].tobytes());counts[child]+=int(selected.sum())
                    offset+=len(x)
        if any(n==0 for n in counts):raise ValueError('empty owned subtree; model/slot capacity gate failed')
    finally:
        for stream in streams+maps:stream.flush();os.fsync(stream.fileno());stream.close()
    model_desc=artifact(root/'model.json',root)
    return [dict(subtree_id=str(i),centroid_index=i,routing_boundary_hash=content_hash(dict(model_sha256=model_desc['sha256'],centroid_index=i)),vector_count=n,append_row_count=n,local_start=0,
        vectors=artifact(owners/str(i)/'vectors.f32',root),local_to_global=artifact(owners/str(i)/'local-to-global.u64le',root)) for i,n in enumerate(counts)]


def prepare(manifest,output,count,children,epoch,source_id,metric='ip',chunk_rows=1000000,batch_rows=2048,sample_size=8192,seed=1729,iterations=12,
            resume=False,max_source_bytes=4*1024**3,max_wall_s=1800,open_source=None,stop_after_chunks=None):
    if manifest.get('dataset')!=DATASET or manifest.get('revision')!=REVISION or manifest.get('dimension')!=DIMENSION:raise ValueError('pinned FineWeb source required')
    names=[r['path'] for r in manifest['files']]
    if not names or names!=sorted(names) or len(set(names))!=len(names):raise ValueError('source order must be pinned and unique')
    for row in manifest['files']:
        if not re.fullmatch('[0-9a-f]{64}',row.get('publisher_sha256','')) or row.get('bytes',0)<1:raise ValueError('pinned publisher file identity required')
        if row.get('url')!=f"https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/{row['path']}":raise ValueError('source URL must pin revision and path')
    if max_source_bytes<1 or max_wall_s<=0:raise ValueError('positive source byte and wall limits required')
    if min(count,children,chunk_rows,batch_rows,sample_size,iterations)<1 or children>sample_size or children>count or not source_id or not epoch or metric not in ('l2','ip','cosine'):raise ValueError('invalid preparation configuration')
    root=Path(output);config=dict(decoder_epoch=DECODE_EPOCH,dataset=DATASET,revision=REVISION,manifest_sha256=content_hash(manifest),count=count,children=children,epoch=epoch,source_id=source_id,metric=metric,chunk_rows=chunk_rows,sample_size=sample_size,seed=seed,iterations=iterations)
    started=time.monotonic();budget=dict(max_bytes=max_source_bytes,bytes_read=0,deadline=started+max_wall_s)
    if root.exists():
        if not resume:raise FileExistsError(root)
        state=json.loads((root/'checkpoint.json').read_text())
        if state['config']!=config:raise ValueError('resume configuration changed')
        if (root/'complete.json').exists():raise ValueError('completed preparation is immutable; consume its input.json')
    else:
        root.mkdir(parents=True);(root/'chunks').mkdir();_json(root/'source-manifest.json',manifest)
        state=dict(config=config,chunks=[],committed_rows=0,cursor=dict(file_index=0,row_offset=0));_json(root/'checkpoint.json',state)
    expected=0
    for chunk in state['chunks']:expected=_verify_chunk(root,chunk,expected)
    if expected!=state['committed_rows']:raise ValueError('checkpoint row count differs from verified chunks')
    # Adopt only complete, digest-verified orphan chunks from the narrow crash
    # window between chunk publication and checkpoint-pointer replacement.
    while True:
        path=root/'chunks'/f'{len(state["chunks"]):06d}.json'
        if not path.exists():break
        chunk=json.loads(path.read_text());end=_verify_chunk(root,chunk,state['committed_rows'])
        if chunk['start_cursor']!=state['cursor']:raise ValueError('orphan source cursor mismatch')
        state['chunks'].append(chunk);state.update(committed_rows=end,cursor=chunk['end_cursor']);_json(root/'checkpoint.json',state)
    attempts=len(list(root.glob('attempt-*.json')));attempt_path=root/f'attempt-{attempts:04d}.json'
    stage_times={};stage_started=time.monotonic()
    try:
        if state['committed_rows']<count:
            with (root/f'source-ranges-{attempts:04d}.jsonl').open('w') as audit:
                iterator=_source_batches(manifest,state['cursor'],budget,batch_rows,open_source,audit)
                vector_stream=uuid_stream=None;current=None
                try:
                    for x,uuids,cursor,file_rows in iterator:
                        position=0
                        while position<len(x) and state['committed_rows']<count:
                            if current is None:
                                number=len(state['chunks']);prefix=root/'chunks'/f'{number:06d}'
                                vector_stream=Path(str(prefix)+'.f32.partial').open('wb');uuid_stream=Path(str(prefix)+'.uuid.jsonl.partial').open('w')
                                current=dict(start_id=state['committed_rows'],row_count=0,start_cursor=dict(cursor),prefix=prefix)
                            take=min(len(x)-position,chunk_rows-current['row_count'],count-current['start_id']-current['row_count'])
                            vector_stream.write(x[position:position+take].tobytes())
                            for j in range(take):uuid_stream.write(json.dumps(dict(global_id=current['start_id']+current['row_count']+j,source_uuid=uuids[position+j],source_file=manifest['files'][cursor['file_index']]['path'],source_row=cursor['row_offset']+j))+'\n')
                            current['row_count']+=take;position+=take;cursor=dict(file_index=cursor['file_index'],row_offset=cursor['row_offset']+take)
                            end_cursor=dict(file_index=cursor['file_index']+1,row_offset=0) if cursor['row_offset']==file_rows else dict(cursor)
                            if current['row_count']==chunk_rows or current['start_id']+current['row_count']==count:
                                for stream in (vector_stream,uuid_stream):stream.flush();os.fsync(stream.fileno());stream.close()
                                vector_stream=uuid_stream=None;prefix=current.pop('prefix')
                                vectors=Path(str(prefix)+'.f32');ids=Path(str(prefix)+'.uuid.jsonl')
                                os.replace(str(prefix)+'.f32.partial',vectors);os.replace(str(prefix)+'.uuid.jsonl.partial',ids)
                                chunk=dict(**current,end_cursor=end_cursor,vectors=artifact(vectors,root),uuids=artifact(ids,root))
                                _json(str(prefix)+'.json',chunk);state['chunks'].append(chunk);state.update(committed_rows=chunk['start_id']+chunk['row_count'],cursor=end_cursor);_json(root/'checkpoint.json',state);current=None
                                if stop_after_chunks is not None and len(state['chunks'])>=stop_after_chunks:raise InterruptedError('requested test interruption after verified chunk')
                        if state['committed_rows']==count:break
                finally:
                    iterator.close()
                    for stream in (vector_stream,uuid_stream):
                        if stream is not None:stream.close()
            if state['committed_rows']!=count:raise ValueError('source exhausted before complete prefix')
        if time.monotonic()>budget['deadline']:raise TimeoutError('preparation deadline reached before local stages')
        stage_times['ingest_and_checkpoint_s']=time.monotonic()-stage_started
        stage_started=time.monotonic();uuid_index=_audit_uuids(root,state['chunks']);stage_times['uuid_audit_s']=time.monotonic()-stage_started
        stage_started=time.monotonic();model=_whole_prefix_model(root,state['chunks'],config);stage_times['sample_and_model_s']=time.monotonic()-stage_started
        stage_started=time.monotonic();subtrees=_route(root,state['chunks'],config,model,batch_rows);stage_times['routing_and_output_hashes_s']=time.monotonic()-stage_started
        if time.monotonic()>budget['deadline']:raise TimeoutError('preparation deadline exceeded; completed chunks remain resumable')
        result=dict(format_version=1,construction_epoch=epoch,dimension=DIMENSION,metric=metric,source_id=source_id,start_id=0,input_row_count=count,global_row_count=count,
            source=dict(kind='verified_contiguous_chunks',manifest=artifact(root/'source-manifest.json',root),chunks=state['chunks']),model=artifact(root/'model.json',root),
            upper_root=dict(kind='centroid_root',children=[dict(subtree_id=str(i),centroid_index=i) for i in range(children)]),subtrees=subtrees,
            uuid_index=uuid_index,stage_wall_s=stage_times,preparation_wall_s=time.monotonic()-started,routing_policy='nearest frozen root centroid; original global IDs; no cross-owner reassignment',
            resource_scope='bounded batch/sample arrays; retained prepared chunks and owner files on worker storage; no source range cache; local stages check wall deadline at stage boundaries, use an external process timeout for a hard deadline',
            resume_policy='verify completed chunk hashes and cursors; restart local UUID/model/routing stages; retain at most two incomplete route directories')
        _json(root/'input.json',result);_json(root/'complete.json',dict(status='passed',input=artifact(root/'input.json',root),config=config))
        return result
    finally:_json(attempt_path,dict(source_bytes_fetched=budget['bytes_read'],wall_s=time.monotonic()-started,committed_rows=state['committed_rows'],complete=(root/'complete.json').exists(),max_source_bytes=max_source_bytes,max_wall_s=max_wall_s,stage_wall_s=stage_times))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('manifest','output','source-id','epoch'):p.add_argument('--'+name,required=True)
    for name,default in [('count',1000000),('children',16),('chunk-rows',1000000),('batch-rows',2048),('sample-size',8192),('seed',1729),('iterations',12),('max-source-bytes',4*1024**3)]:p.add_argument('--'+name,type=int,default=default)
    p.add_argument('--max-wall-s',type=float,default=1800);p.add_argument('--metric',choices=('l2','ip','cosine'),default='ip');p.add_argument('--resume',action='store_true');a=p.parse_args()
    r=prepare(json.loads(Path(a.manifest).read_text()),a.output,a.count,a.children,a.epoch,a.source_id,a.metric,a.chunk_rows,a.batch_rows,a.sample_size,a.seed,a.iterations,a.resume,a.max_source_bytes,a.max_wall_s)
    print(json.dumps(dict(status='passed',rows=r['global_row_count'],subtrees=len(r['subtrees']),wall_s=r['preparation_wall_s'])))

if __name__=='__main__':main()
