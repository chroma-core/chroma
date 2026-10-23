"""Bounded per-source-file ingestion with canonical IDs and verified resume.

This produces verified corpus chunks, not an index or a fitted routing model.
Each source file has one writer; only a coordinator publishes the completion.
"""
from concurrent.futures import ThreadPoolExecutor
import fcntl
import json
from pathlib import Path
import re
import threading
import time
import urllib.request
import numpy as np
from .fineweb_prepare import DATASET,REVISION,DIMENSION,RangeReader
from .fineweb_stream import _source_batches,_verify_chunk,_audit_uuids
from .tree_manifest import artifact,canonical_bytes,content_hash,_atomic_write


def _json(path,value):_atomic_write(path,canonical_bytes(value))


class Limits:
    def __init__(self,source_bytes,output_bytes,wall_s,existing_output=0):
        if min(source_bytes,output_bytes,wall_s)<=0:raise ValueError('positive shared caps required')
        self.lock=threading.Lock();self.source_cap=source_bytes;self.disk_cap=output_bytes;self.charged=0;self.disk=existing_output;self.deadline=time.monotonic()+wall_s
        if self.disk>output_bytes:raise ValueError('existing owned output exceeds cap')
    def check(self):
        if time.monotonic()>self.deadline:raise TimeoutError('shared ingestion deadline exceeded')
    def reserve_output(self,amount):
        with self.lock:
            self.check()
            if self.disk+amount>self.disk_cap:raise ValueError('shared prepared-output byte cap exceeded')
            self.disk+=amount
    def open_http(self,request,timeout=120):
        length=request.get_header('Range');match=re.fullmatch(r'bytes=(\d+)-(\d+)',length or '')
        if not match:raise ValueError('only bounded range requests are supported')
        amount=int(match[2])-int(match[1])+1
        with self.lock:
            self.check()
            if self.charged+amount>self.source_cap:raise ValueError('shared source request-byte cap exceeded')
            # Failed requests remain charged. Thus retries cannot evade the cap.
            self.charged+=amount;remaining=self.deadline-time.monotonic()
        return urllib.request.urlopen(request,timeout=min(timeout,remaining))


def ingest(manifest,output,count,workers=4,chunk_rows=100000,batch_rows=2048,max_source_bytes=4*1024**3,max_output_bytes=8*1024**3,max_wall_s=1200,retries=1,resume=False,open_source=None,before_file=None):
    import pyarrow.parquet as pq
    if manifest.get('dataset')!=DATASET or manifest.get('revision')!=REVISION or manifest.get('dimension')!=DIMENSION:raise ValueError('pinned FineWeb manifest required')
    files=manifest.get('files',[]);names=[f['path'] for f in files]
    if not names or names!=sorted(names) or len(names)!=len(set(names)):raise ValueError('canonical source order required')
    for f in files:
        if not re.fullmatch('[0-9a-f]{64}',f.get('publisher_sha256','')) or f.get('bytes',0)<1 or f.get('url')!=f"https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/{f['path']}":raise ValueError('pinned source file identity required')
    if min(count,workers,chunk_rows,batch_rows)<1 or workers>16 or retries not in (0,1,2):raise ValueError('invalid bounded concurrency settings')
    root=Path(output);config=dict(manifest_sha256=content_hash(manifest),count=count,chunk_rows=chunk_rows,batch_rows=batch_rows)
    if root.exists() and not resume:raise FileExistsError(root)
    root.mkdir(parents=True,exist_ok=True)
    lock=(root/'publisher.lock').open('a+b');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    start=time.monotonic();attempt=len(list(root.glob('attempt-*.json')))
    try:limits=Limits(max_source_bytes,max_output_bytes,max_wall_s,sum(p.stat().st_size for p in root.rglob('*') if p.is_file()))
    except Exception:
        fcntl.flock(lock,fcntl.LOCK_UN);lock.close();raise
    audit_lock=threading.Lock();network=[];finished=[]
    def reader(entry):return open_source(entry) if open_source else RangeReader(entry['url'],entry['bytes'],opener=limits.open_http)
    def probe(entry):
        for attempt in range(retries+1):
            try:return probe_once(entry)
            except (OSError,TimeoutError):
                if attempt==retries:raise
                limits.check();time.sleep(min(2**attempt,max(0,limits.deadline-time.monotonic())))
    def probe_once(entry):
        with reader(entry) as source:
            p=pq.ParquetFile(source);rows=p.metadata.num_rows;schema=str(p.schema_arrow)
            if rows<1:raise ValueError('empty source file')
            with audit_lock:network.append(dict(phase='metadata',path=entry['path'],ranges=getattr(source,'ranges',None)))
            return dict(file=entry,parquet_rows=rows,schema=schema,etag=getattr(source,'etag',None))
    try:
        if (root/'complete.json').exists():raise ValueError('completed ingestion is immutable')
        if (root/'plan.json').exists():
            plan=json.loads((root/'plan.json').read_text())
            if plan['config']!=config:raise ValueError('resume configuration or source changed')
        else:
            planned=[];offset=0
            # Bound speculative metadata to one window. Offsets use actual
            # parquet row counts, never unverified source metadata row counts.
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for first in range(0,len(files),workers):
                    for meta in pool.map(probe,files[first:first+workers]):
                        if offset>=count:break
                        take=min(meta['parquet_rows'],count-offset);planned.append(dict(**meta,start_id=offset,row_count=take));offset+=take
                    if offset==count:break
            if offset!=count:raise ValueError('source exhausted before requested prefix')
            plan=dict(config=config,jobs=planned);_json(root/'plan.json',plan);_json(root/'source-manifest.json',manifest)
        def job(index,entry):
            folder=root/'files'/f'{index:06d}';folder.mkdir(parents=True,exist_ok=True);receipt=folder/'complete.json'
            if receipt.exists():
                result=json.loads(receipt.read_text());expected=entry['start_id']
                if result['source']!=entry:raise ValueError('completed source binding changed')
                for chunk in result['chunks']:expected=_verify_chunk(root,chunk,expected)
                if expected!=entry['start_id']+entry['row_count']:raise ValueError('source chunk coverage mismatch')
                return result
            # Failed attempts stay inspectable and count toward the disk cap.
            for retry in range(retries+1):
                limits.check();number=len(list(folder.glob('try-*')))
                if number>=3:raise ValueError('three incomplete file attempts retained; inspect storage before restart')
                trial=folder/f'try-{number}';trial.mkdir();chunks=[];global_row=entry['start_id'];buffer_x=[];buffer_ids=[];buffer_rows=0;cursor=0
                try:
                    def bound_reader(file):
                        source=reader(file)
                        if entry.get('etag') is not None:source.etag=entry['etag']
                        actual=pq.ParquetFile(source)
                        if actual.metadata.num_rows!=entry['parquet_rows'] or str(actual.schema_arrow)!=entry['schema']:source.close();raise ValueError('source parquet row count or schema changed')
                        source.seek(0);return source
                    def commit():
                        nonlocal buffer_rows,global_row,buffer_x,buffer_ids
                        prefix=trial/f'{len(chunks):06d}';vectors=Path(str(prefix)+'.f32');uuids=Path(str(prefix)+'.uuid.jsonl')
                        raw=b''.join(buffer_x);text=''.join(buffer_ids).encode();limits.reserve_output(len(raw)+len(text))
                        _atomic_write(vectors,raw);_atomic_write(uuids,text)
                        c=dict(start_id=global_row,row_count=buffer_rows,vectors=artifact(vectors,root),uuids=artifact(uuids,root));chunks.append(c);_json(str(prefix)+'.json',c);global_row+=buffer_rows;buffer_x=[];buffer_ids=[];buffer_rows=0
                    if before_file is not None:before_file(index,retry)
                    with (trial/'source-ranges.jsonl').open('w') as audit:
                        iterator=_source_batches(dict(files=[entry['file']]),dict(file_index=0,row_offset=0),None,batch_rows,bound_reader,audit)
                        try:
                            for x,ids,_,_ in iterator:
                                limits.check();position=0
                                while position<len(x) and cursor<entry['row_count']:
                                    take=min(len(x)-position,chunk_rows-buffer_rows,entry['row_count']-cursor)
                                    buffer_x.append(x[position:position+take].tobytes())
                                    for j in range(take):buffer_ids.append(json.dumps(dict(global_id=entry['start_id']+cursor+j,source_uuid=ids[position+j],source_file=entry['file']['path'],source_row=cursor+j))+'\n')
                                    cursor+=take;position+=take;buffer_rows+=take
                                    if buffer_rows==chunk_rows or cursor==entry['row_count']:commit()
                                if cursor==entry['row_count']:break
                        finally:iterator.close()
                    if cursor!=entry['row_count']:raise ValueError('source file ended before planned rows')
                    result=dict(source=entry,chunks=chunks);_json(receipt,result);return result
                except (OSError,TimeoutError) as error:
                    _json(trial/'failure.json',dict(error_type=type(error).__name__,error=str(error)))
                    if retry==retries:raise
                    time.sleep(min(2**retry,max(0,limits.deadline-time.monotonic())))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures=[pool.submit(job,i,e) for i,e in enumerate(plan['jobs'])]
            finished=[f.result() for f in futures]
        chunks=[c for result in finished for c in result['chunks']];expected=0
        for c in chunks:expected=_verify_chunk(root,c,expected)
        if expected!=count:raise ValueError('canonical global prefix coverage mismatch')
        # Across-file UUID uniqueness is separate from per-source ownership.
        limits.check()
        current_used=sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
        uuid_index=_audit_uuids(root,chunks,max_bytes=max_output_bytes-current_used-1024*1024);limits.check()
        used=sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
        if used>max_output_bytes:raise ValueError('completed metadata/index exceeds shared output byte cap')
        result=dict(status='passed',config=config,chunks=chunks,uuid_index=uuid_index,global_row_count=count,source_manifest=artifact(root/'source-manifest.json',root),wall_s=time.monotonic()-start,source_request_bytes_charged=limits.charged,owned_output_bytes=used,network_metadata=network,
                    resume_policy='verify and reuse complete per-file receipts; incomplete file attempts retained and restarted; one writer per file and one coordinator lock',source_cap_scope='requested payload charged before requests, including failures; stricter than successfully fetched bytes')
        _json(root/'ingest.json',result);_json(root/'complete.json',dict(ingest=artifact(root/'ingest.json',root)));return result
    finally:
        _json(root/f'attempt-{attempt:04d}.json',dict(wall_s=time.monotonic()-start,source_request_bytes_charged=limits.charged,output_bytes_reserved=limits.disk,complete=(root/'complete.json').exists(),caps=dict(source=max_source_bytes,output=max_output_bytes,wall_s=max_wall_s)))
        fcntl.flock(lock,fcntl.LOCK_UN);lock.close()
