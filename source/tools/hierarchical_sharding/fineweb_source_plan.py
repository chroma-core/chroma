"""Bounded parquet metadata plan for a pinned real FineWeb prefix; no vector reads."""
import re,time
import pyarrow as pa
import pyarrow.parquet as pq
from .fineweb_prepare import DATASET,REVISION,DIMENSION,RangeReader
from .tree_manifest import content_hash


def plan_prefix(manifest,count,max_source_bytes=16*1024**2,max_wall_s=120,max_files=32,max_column_read_bytes=256*1024**2,open_source=None):
    if manifest.get('dataset')!=DATASET or manifest.get('revision')!=REVISION or manifest.get('dimension')!=DIMENSION:raise ValueError('pinned FineWeb manifest required')
    files=manifest.get('files',[]);names=[e['path'] for e in files]
    if type(count) is not int or count<1 or min(max_source_bytes,max_wall_s,max_files,max_column_read_bytes)<=0:raise ValueError('positive metadata bounds required')
    if not names or names!=sorted(names) or len(names)!=len(set(names)):raise ValueError('canonical source order required')
    for entry in files:
        if entry.get('bytes',0)<1 or not re.fullmatch('[0-9a-f]{64}',entry.get('publisher_sha256','')) or entry.get('url')!=f"https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/{entry['path']}":raise ValueError('pinned file identity required')
    started=time.monotonic();budget=dict(max_bytes=max_source_bytes,bytes_read=0,deadline=started+max_wall_s);rows=0;planned=[]
    for entry in files:
        if rows==count:break
        if len(planned)==max_files:raise ValueError('metadata file-count cap reached before prefix complete')
        source=open_source(entry) if open_source else RangeReader(entry['url'],entry['bytes'],budget=budget)
        try:
            parquet=pq.ParquetFile(source);schema=parquet.schema_arrow;field=schema.field('dense_embedding').type
            if not (pa.types.is_list(field) or pa.types.is_large_list(field) or pa.types.is_fixed_size_list(field)) or not pa.types.is_float16(field.value_type):raise ValueError('source primitive schema changed')
            if pa.types.is_fixed_size_list(field) and field.list_size!=DIMENSION:raise ValueError('source dimension changed')
            if not (pa.types.is_string(schema.field('id').type) or pa.types.is_large_string(schema.field('id').type)):raise ValueError('source UUID schema changed')
            actual=parquet.metadata.num_rows
            if actual<1:raise ValueError('empty source file')
            take=min(actual,count-rows);groups=[];offset=0
            for number in range(parquet.num_row_groups):
                group=parquet.metadata.row_group(number)
                if offset<take:
                    columns=[]
                    for j in range(group.num_columns):
                        column=group.column(j)
                        if column.path_in_schema.split('.')[0] not in ('dense_embedding','id'):continue
                        if column.total_compressed_size>max_column_read_bytes:raise ValueError('projected column exceeds decoder read-memory cap')
                        columns.append(dict(path=column.path_in_schema,compressed_bytes=column.total_compressed_size))
                    if len(columns)!=2:raise ValueError('expected dense and UUID primitive columns')
                    groups.append(dict(row_group=number,source_row_start=offset,row_count=group.num_rows,columns=columns))
                offset+=group.num_rows
            if offset!=actual:raise ValueError('parquet row group counts do not match file metadata')
            planned.append(dict(file=entry,parquet_rows=actual,start_id=rows,row_count=take,etag=getattr(source,'etag',None),row_groups=groups,metadata_ranges=getattr(source,'ranges',None),schema=str(schema)));rows+=take
        finally:source.close()
        if time.monotonic()>budget['deadline']:raise TimeoutError('metadata planning deadline exceeded')
    if rows!=count:raise ValueError('manifest exhausted before requested prefix')
    projected=sum(c['compressed_bytes'] for f in planned for g in f['row_groups'] for c in g['columns'])
    return dict(status='passed',dataset=DATASET,revision=REVISION,manifest_sha256=content_hash(manifest),count=count,files=planned,projected_compressed_column_bytes=projected,metadata_payload_bytes=budget['bytes_read'],whole_selected_parquet_bytes=sum(f['file']['bytes'] for f in planned),wall_s=time.monotonic()-started,
                source_identity_scope='pinned revision and publisher metadata; actual parquet row counts/schema checked; projected reads do not recompute whole-file digests',variable_list_dimension_gate='actual768-value rows validated during ingestion, not proven by variable-list metadata')
