"""Admit a metadata-only 100M prefix plan without downloading corpus vectors."""
import math,re
from .fineweb_prepare import DATASET,REVISION


def validate_100m_plan(plan):
    if plan.get('status')!='passed' or plan.get('dataset')!=DATASET or plan.get('revision')!=REVISION or plan.get('count')!=100000000:raise ValueError('passed pinned100M source plan required')
    files=plan['files']
    if not files or len(files)>512:raise ValueError('metadata file cap exceeded')
    names=[f['file']['path'] for f in files]
    if names!=sorted(names) or len(set(names))!=len(names):raise ValueError('canonical source file order required')
    cursor=0;projected=0
    for index,file in enumerate(files):
        identity=file['file'];count=file['row_count'];actual=file['parquet_rows']
        if not re.fullmatch('[0-9a-f]{64}',identity.get('publisher_sha256','')) or type(identity.get('bytes')) is not int or identity['bytes']<1 or identity.get('url')!=f"https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/{identity['path']}":raise ValueError('pinned source file identity required')
        if type(count) is not int or type(actual) is not int or count<1 or count>actual or type(file['start_id']) is not int or file['start_id']!=cursor:raise ValueError('source interval coverage differs')
        if index<len(files)-1 and count!=actual:raise ValueError('only final source file may be truncated')
        group_end=0
        for number,group in enumerate(file['row_groups']):
            if group['row_group']!=number or group['source_row_start']!=group_end or type(group['row_count']) is not int or group['row_count']<1 or group_end>=count:raise ValueError('selected row group coverage differs')
            group_end+=group['row_count']
            if len(group['columns'])!=2 or {c['path'].split('.')[0] for c in group['columns']}!={'dense_embedding','id'}:raise ValueError('selected primitive columns differ')
            for column in group['columns']:
                size=column['compressed_bytes']
                if type(size) is not int or not 0<size<=256*1024**2:raise ValueError('column read-memory gate exceeded')
                projected+=size
        if group_end<count or group_end>actual:raise ValueError('row groups do not cover planned rows')
        cursor+=count
    if cursor!=100000000 or projected!=plan['projected_compressed_column_bytes']:raise ValueError('source count or compressed projection differs')
    metadata=plan['metadata_payload_bytes']
    if type(metadata) is not int or not 0<=metadata<=128*1024**2 or type(plan['wall_s']) not in (int,float) or not math.isfinite(plan['wall_s']) or not 0<=plan['wall_s']<=300:raise ValueError('metadata budget exceeded')
    # Include one complete retry and five percent request/read amplification.
    charged_projection=(projected*21+9)//10+128*1024**2
    if charged_projection>384*1024**3:raise ValueError('projected data and retry requests exceed fixed cap')
    return dict(count=cursor,source_files=len(files),ingest_concurrency=2,chunk_rows=100000,batch_rows=2048,retries=1,source_request_cap_bytes=384*1024**3,charged_request_projection_bytes=charged_projection,absolute_node_budget_s=21600,preservation_reserve_s=1800,bulk_deadline_from_node_start_s=19800,scope_cap_bytes=2*1024**4,scope_stop_margin_bytes=256*1024**3,rss_cap_bytes=32*1024**3,rss_stop_margin_bytes=4*1024**3,disk_free_floor_bytes=512*1024**3,disk_stop_margin_bytes=128*1024**3,launch_allowed=False,scope='Metadata admission only. Actual10M read/capacity decision, concrete identity/headroom/deadline, references and archive/restore gates remain required.')
