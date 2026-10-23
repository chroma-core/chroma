"""Pinned FineWeb dense-prefix exporter; preserve float16 values exactly in float32.

No query embeddings are created here. HTTP reads require byte-range support and
have explicit per-read/network limits. Existing or partial outputs are not reused.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import sqlite3
import time
import urllib.request
import numpy as np
from .tree_manifest import artifact, canonical_bytes, content_hash

DECODE_EPOCH='arrow_primitive_f16_v2'
DATASET='Qdrant/FineWeb-10B'
REVISION='3aea0b8d5f98741e343d07f067ce0f49dca1ccd2'
DIMENSION=768


def manifest_from_api(metadata):
    if metadata.get('sha')!=REVISION: raise ValueError('dataset revision differs from the pinned source')
    files=[]
    for row in metadata['siblings']:
        name=row['rfilename']
        if not name.startswith('data/') or not name.endswith('.parquet'):continue
        digest=row.get('lfs',{}).get('sha256')
        if not re.fullmatch('[0-9a-f]{64}',digest or '') or row.get('size',0)<=0:raise ValueError('publisher size and SHA-256 required')
        files.append(dict(path=name,bytes=row['size'],publisher_sha256=digest,url=f'https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/{name}'))
    files.sort(key=lambda row:row['path'])
    if not files or len({f['path'] for f in files})!=len(files):raise ValueError('unique corpus files required')
    return dict(format_version=1,dataset=DATASET,revision=REVISION,dimension=DIMENSION,files=files,
                source_order='lexicographic pinned file path, then parquet row order',
                publisher_metadata=dict(url=f'https://huggingface.co/api/datasets/{DATASET}/revision/{REVISION}?blobs=true',canonical_sha256=content_hash(metadata)),
                value_policy='exact float16 to float32 conversion; no normalization',
                query_provenance=None)


class RangeReader(io.RawIOBase):
    """Seekable public HTTP object with bounded, verified partial responses."""
    def __init__(self,url,size,max_read_bytes=256*1024*1024,range_bytes=8*1024*1024,opener=None,budget=None,cache_path=None):
        super().__init__()
        if size<1 or min(max_read_bytes,range_bytes)<1:raise ValueError('positive object/read sizes required')
        self.url=url;self.size=size;self.position=0;self.max_read_bytes=max_read_bytes;self.range_bytes=range_bytes
        self.opener=opener or urllib.request.urlopen;self.ranges=[];self.etag=None;self.budget=budget
        self.cache=open(cache_path,'x+b') if cache_path else None
        if self.cache:self.cache.truncate(size)
    def readable(self):return True
    def seekable(self):return True
    def tell(self):return self.position
    def seek(self,offset,whence=0):
        target=offset if whence==0 else self.position+offset if whence==1 else self.size+offset if whence==2 else -1
        if target<0 or whence not in (0,1,2):raise ValueError('invalid seek')
        self.position=target;return target
    def read(self,size=-1):
        if self.closed:raise ValueError('read on closed source')
        size=max(0,self.size-self.position) if size is None or size<0 else min(size,max(0,self.size-self.position))
        if size>self.max_read_bytes:raise ValueError('parquet read exceeds configured memory bound; inspect row-group/column sizes')
        result=bytearray()
        while len(result)<size:
            start=self.position;length=min(size-len(result),self.range_bytes);end=start+length-1
            timeout=120
            if self.budget is not None:
                if self.budget['max_bytes'] is not None and self.budget['bytes_read']+length>self.budget['max_bytes']:raise ValueError('total source-byte budget exceeded')
                if self.budget['deadline'] is not None:
                    remaining=self.budget['deadline']-time.monotonic()
                    if remaining<=0:raise TimeoutError('export wall-time budget exceeded')
                    timeout=min(timeout,remaining)
            headers={'Range':f'bytes={start}-{end}','Accept-Encoding':'identity'}
            if self.etag is not None:headers['If-Match']=self.etag
            request=urllib.request.Request(self.url,headers=headers)
            request_started=time.monotonic()
            with self.opener(request,timeout=timeout) as response:
                status=getattr(response,'status',None)
                content_range=response.headers.get('Content-Range')
                if status!=206 or content_range!=f'bytes {start}-{end}/{self.size}':
                    raise ValueError('source did not honor the exact pinned byte range')
                etag=response.headers.get('ETag')
                if self.etag is not None and etag!=self.etag:raise ValueError('source ETag changed during export')
                if self.etag is None:self.etag=etag
                data=response.read(length+1)
            if len(data)!=length:raise ValueError('truncated or oversized range response')
            if self.budget is not None:self.budget['bytes_read']+=len(data)
            if self.cache:self.cache.seek(start);self.cache.write(data)
            self.ranges.append(dict(offset=start,bytes=length,sha256=hashlib.sha256(data).hexdigest(),request_wall_s=time.monotonic()-request_started))
            result.extend(data);self.position+=length
        return bytes(result)
    def close(self):
        if getattr(self,'cache',None) is not None:self.cache.close();self.cache=None
        super().close()
    def readinto(self,buffer):
        data=self.read(len(buffer));buffer[:len(data)]=data;return len(data)


def decode_batch(batch):
    """Preserve half values using Arrow primitive buffers, without Python floats."""
    import pyarrow as pa
    values=batch.column(batch.schema.get_field_index('dense_embedding'))
    if values.null_count:raise ValueError('null source embedding')
    kind=values.type
    if not (pa.types.is_list(kind) or pa.types.is_large_list(kind) or pa.types.is_fixed_size_list(kind)) or not pa.types.is_float16(kind.value_type):
        raise ValueError('source embeddings must be float16 lists')
    if pa.types.is_fixed_size_list(kind):
        if kind.list_size!=DIMENSION:raise ValueError('wrong source embedding dimension')
        start=values.offset*DIMENSION;length=len(values)*DIMENSION
    else:
        offsets=values.offsets.to_numpy(zero_copy_only=False)
        if np.any(np.diff(offsets)!=DIMENSION):raise ValueError('wrong source embedding dimension')
        start=int(offsets[0]);length=int(offsets[-1])-start
    flat=values.values.slice(start,length)
    if flat.null_count:raise ValueError('null source embedding value')
    half=flat.to_numpy(zero_copy_only=False)
    if half.dtype!=np.float16 or len(half)!=len(values)*DIMENSION or not np.isfinite(half).all():raise ValueError('invalid primitive source values')
    ids=batch.column(batch.schema.get_field_index('id')).to_pylist()
    if any(not isinstance(value,str) or not value for value in ids):raise ValueError('null or empty source UUID')
    x=half.reshape(len(values),DIMENSION).astype('<f4')
    if np.any(np.einsum('ij,ij->i',x,x)==0):raise ValueError('zero source embedding')
    return x,ids


def export(manifest,output,count,batch_rows=2048,max_read_bytes=256*1024*1024,range_bytes=8*1024*1024,open_source=None,max_source_bytes=None,max_wall_s=None,range_cache=False):
    import pyarrow as pa
    import pyarrow.parquet as pq
    if manifest.get('format_version')!=1 or manifest.get('dataset')!=DATASET or manifest.get('revision')!=REVISION or manifest.get('dimension')!=DIMENSION:
        raise ValueError('unsupported or unpinned FineWeb manifest')
    files=manifest.get('files',[])
    names=[f['path'] for f in files]
    if not names or names!=sorted(names) or len(set(names))!=len(names):raise ValueError('source files must be unique and lexicographically ordered')
    if max_source_bytes is not None and max_source_bytes<1:raise ValueError('positive source-byte cap required')
    if max_wall_s is not None and max_wall_s<=0:raise ValueError('positive wall-time cap required')
    if type(count) is not int or count<1 or batch_rows<1:raise ValueError('positive prefix and batch sizes required')
    for row in files:
        if not re.fullmatch('[0-9a-f]{64}',row.get('publisher_sha256','')) or row.get('bytes',0)<1:raise ValueError('pinned publisher file identity required')
        if row.get('url')!=f"https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/{row['path']}":raise ValueError('source URL must pin the declared dataset revision and path')
    output=Path(output);output.mkdir(parents=True,exist_ok=False);begun=time.monotonic();rows_written=0;sources=[]
    budget=dict(max_bytes=max_source_bytes,bytes_read=0,deadline=begun+max_wall_s if max_wall_s is not None else None)
    if range_cache:(output/'range-cache').mkdir()
    database=sqlite3.connect(output/'uuid-to-row.sqlite')
    database.execute('CREATE TABLE documents (source_uuid TEXT PRIMARY KEY, global_id INTEGER NOT NULL UNIQUE) WITHOUT ROWID')
    norm_min,norm_max=float('inf'),0.
    try:
        with (output/'corpus.f32').open('wb') as vectors_out,(output/'row-to-uuid.jsonl').open('w') as ids_out:
            for file in files:
                if rows_written==count:break
                if budget['deadline'] is not None and time.monotonic()>budget['deadline']:raise TimeoutError('export wall-time budget exceeded')
                cache_path=output/'range-cache'/f'{len(sources):05d}.parquet' if range_cache else None
                source=open_source(file) if open_source else RangeReader(file['url'],file['bytes'],max_read_bytes,range_bytes,budget=budget,cache_path=cache_path)
                try:
                    parquet=pq.ParquetFile(source);schema=parquet.schema_arrow
                    field=schema.field('dense_embedding').type
                    if not (pa.types.is_list(field) or pa.types.is_large_list(field) or pa.types.is_fixed_size_list(field)) or not pa.types.is_float16(field.value_type):
                        raise ValueError('dense_embedding must be a list of source float16 values')
                    if not pa.types.is_string(schema.field('id').type) and not pa.types.is_large_string(schema.field('id').type):raise ValueError('source id must preserve a string UUID')
                    first_global=rows_written;source_row=0
                    for batch in parquet.iter_batches(batch_size=batch_rows,columns=['dense_embedding','id'],use_threads=False):
                        if budget['deadline'] is not None and time.monotonic()>budget['deadline']:raise TimeoutError('export wall-time budget exceeded')
                        take=min(batch.num_rows,count-rows_written)
                        x,uuids=decode_batch(batch.slice(0,take));norms=np.linalg.norm(x.astype(np.float64),axis=1)
                        if np.any(norms==0):raise ValueError('zero source embedding violates the unit-vector corpus contract')
                        norm_min=min(norm_min,float(norms.min()));norm_max=max(norm_max,float(norms.max()))
                        database.executemany('INSERT INTO documents VALUES (?,?)',((u,rows_written+i) for i,u in enumerate(uuids)))
                        database.commit()
                        vectors_out.write(x.tobytes())
                        for i,u in enumerate(uuids):ids_out.write(json.dumps(dict(global_id=rows_written+i,source_uuid=u,source_file=file['path'],source_row=source_row+i))+'\n')
                        rows_written+=take;source_row+=take
                        if rows_written==count:break
                    sources.append(dict(file=file,source_rows_read=source_row,global_range=[first_global,rows_written],parquet_rows=parquet.metadata.num_rows,
                        projected_columns=['dense_embedding','id'],partial_range_cache=str(cache_path.relative_to(output)) if cache_path else None,source_schema=str(schema),etag=getattr(source,'etag',None),fetched_ranges=getattr(source,'ranges',None),
                        publisher_whole_file_hash_locally_verified=False))
                finally:source.close()
            if rows_written!=count:raise ValueError('source manifest exhausted before prefix completion')
    finally:database.close()
    report=dict(format_version=1,dataset=DATASET,revision=REVISION,dimension=DIMENSION,corpus_rows=rows_written,global_range=[0,rows_written],
                normalization='none; source float16 values preserved exactly in float32',proposed_metric='one_minus_inner_product_pending_publisher_parity_check',
                decoder_epoch=DECODE_EPOCH,query_provenance=None,source_order=manifest.get('source_order'),source_norm_range=[norm_min,norm_max],sources=sources,
                uuid_uniqueness='disk-backed unique SQLite index; exact source UUID strings preserved',
                resource_boundary='range hashes verify fetched bytes; publisher whole-file SHA is metadata, not a locally computed full-file hash',
                preparation_wall_s=time.monotonic()-begun,numpy_version=np.__version__,pyarrow_version=pa.__version__,
                settings=dict(batch_rows=batch_rows,max_read_bytes=max_read_bytes,range_bytes=range_bytes,max_source_bytes=max_source_bytes,max_wall_s=max_wall_s,range_cache=range_cache),source_bytes_fetched=budget['bytes_read'],
                outputs={name:artifact(output/name,output) for name in ('corpus.f32','row-to-uuid.jsonl','uuid-to-row.sqlite')})
    (output/'source-manifest.json').write_bytes(canonical_bytes(manifest))
    report['source_manifest']=artifact(output/'source-manifest.json',output)
    report['preparation_wall_s']=time.monotonic()-begun
    (output/'provenance.json').write_bytes(canonical_bytes(report))
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    pin=sub.add_parser('manifest');pin.add_argument('--metadata',required=True);pin.add_argument('--output',required=True)
    run=sub.add_parser('export');run.add_argument('--manifest',required=True);run.add_argument('--output',required=True);run.add_argument('--count',required=True,type=int)
    run.add_argument('--max-source-bytes',type=int);run.add_argument('--max-wall-s',type=float);run.add_argument('--range-cache',action='store_true')
    run.add_argument('--batch-rows',type=int,default=2048);run.add_argument('--max-read-bytes',type=int,default=256*1024*1024);run.add_argument('--range-bytes',type=int,default=8*1024*1024)
    a=p.parse_args()
    if a.command=='manifest':
        result=manifest_from_api(json.loads(Path(a.metadata).read_text()));path=Path(a.output)
        with path.open('xb') as out:out.write(canonical_bytes(result))
    else:result=export(json.loads(Path(a.manifest).read_text()),a.output,a.count,a.batch_rows,a.max_read_bytes,a.range_bytes,max_source_bytes=a.max_source_bytes,max_wall_s=a.max_wall_s,range_cache=a.range_cache)
    print(json.dumps(dict(dataset=result['dataset'],revision=result['revision'],corpus_rows=result.get('corpus_rows'),files=len(result.get('files',result.get('sources',[]))))))

if __name__=='__main__':main()
