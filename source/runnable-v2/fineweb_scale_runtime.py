"""Validate the concrete guarded 100M preparation attempt before bulk reads."""
from pathlib import Path
import math,time
from .fineweb_scale_job import validate_100m_plan
from .fineweb_prepare import DATASET,REVISION
from .tree_manifest import checked_artifact


MINIMUM_STAGE_ESTIMATES={'ingestion':6800,'whole-prefix-model':60,'routing':1200,'full-ID-and-bitwise-routing-audit':1600,'whole-prefix-sample-and-UUID-spotcheck':250,'exact-calibration':1100,'exact-test':1100,'independent-full100m-reference-check':600,'archive-and-restore':6000}

def validate_scale_job(job,plan,base='.',now=None):
    now=time.time() if now is None else now
    admitted=validate_100m_plan(plan)
    if job.get('dataset')!=DATASET or job.get('revision')!=REVISION or type(job.get('dimension')) is not int or job.get('dimension')!=768 or type(job.get('count')) is not int or job.get('count')!=100000000 or job.get('metric')!='ip':raise ValueError('pinned100M IP job required')
    if not __debug__:raise ValueError('optimized Python would disable required audit assertions')
    start,stop=job['started_epoch'],job['stop_epoch']
    if not all(type(x) in (int,float) and math.isfinite(x) for x in (start,stop)) or stop-start!=21600 or not start<=now<stop-1800:raise ValueError('fixed six-hour attempt deadline required')
    estimates=job['stage_estimates_s']
    minimum=MINIMUM_STAGE_ESTIMATES
    if set(estimates)!=set(minimum) or any(type(estimates[k]) is not int or estimates[k]<v for k,v in minimum.items()):raise ValueError('remaining stage projections omit conservative measured work')
    if sum(estimates.values())+1800>stop-now:raise ValueError('complete preparation and restore projection does not fit node deadline')
    root=Path(job['owned_root'])
    if not root.is_absolute() or '..' in root.parts or str(root) in ('/','/mnt','/mnt/spann'):raise ValueError('unique absolute owned root required')
    paths=[job['reports'],job['ingest']['output'],job['spatial']['output'],job['archive']['output'],job['archive']['restore']]
    for value in paths:
        path=Path(value)
        if '..' in path.parts or not path.is_absolute() or root not in path.parents:raise ValueError('all preparation outputs must stay inside owned root')
    if len(set(paths))!=len(paths) or any(Path(a) in Path(b).parents for a in paths for b in paths if a!=b):raise ValueError('output roots must be disjoint')
    ingest=job['ingest'];spatial=job['spatial'];resources=job['resources']
    for key,value in dict(workers=2,chunk_rows=100000,batch_rows=2048,retries=1,max_source_bytes=384*1024**3,max_output_bytes=512*1024**3).items():
        if type(ingest.get(key)) is not int or ingest[key]!=value:raise ValueError('initial instrumented ingestion configuration differs')
    if ingest.get('seed_previous') is not None:raise ValueError('initial100M job uses a fresh source prefix')
    if not 1<=ingest['max_wall_s']<=19800:raise ValueError('ingestion deadline exceeds whole attempt')
    if any(type(spatial.get(k)) is not int for k in ('children','sample_size','batch_rows')):raise ValueError('routing dimensions must be integers')
    if spatial['children']!=128 or spatial['sample_size']!=65536 or spatial['batch_rows']!=4096:raise ValueError('whole-prefix routing configuration differs')
    if resources['hard_timeout_s']!=19800 or resources['max_new_logical_output_bytes']!=2*1024**4 or resources['max_process_rss_bytes']!=32*1024**3:raise ValueError('declared whole-run resource caps differ')
    archive=job['archive']
    if any(type(archive.get(k)) is not int for k in ('concurrency','part_bytes','maximum_part_count')):raise ValueError('archive bounds must be integers')
    if archive['concurrency']!=4 or archive['part_bytes']!=64*1024**2 or archive['maximum_part_count']!=10000:raise ValueError('bounded multipart archive configuration differs')
    scopes=[Path(p) for p in archive['scopes']]
    for scope in scopes:
        if not scope.is_absolute() or '..' in scope.parts or root not in scope.parents:raise ValueError('archive scope escapes owned root')
        if any(scope==Path(p) or scope in Path(p).parents for p in (archive['output'],archive['restore'])):raise ValueError('archive scope includes dynamic outputs')
    if not {Path(job['ingest']['output']),Path(job['spatial']['output']),Path(job['reports'])}.issubset(set(scopes)):raise ValueError('archive omits complete data/failed attempts/reports')
    gate=checked_artifact(job['read_gate'],base)
    import json
    receipt=json.loads(gate.read_text())
    if receipt.get('status')!='passed' or receipt.get('corpus_rows')!=10000000 or receipt.get('allow_100m_preparation') is not True:raise ValueError('10M read/capacity gate is not passed')
    for desc in job['query_pools'].values():checked_artifact(desc,base)
    if set(job['query_pools'])!={'calibration','test'} or any(d['count']!=100 for d in job['query_pools'].values()):raise ValueError('fresh whole-corpus200 references required')
    for desc in job['producer_sources']:
        actual=checked_artifact(desc,base)
        if actual.resolve()!=(Path(__file__).parent/actual.name).resolve():raise ValueError('source binding is not executed module')
    if not job['producer_sources']:raise ValueError('producer source identity required')
    return admitted
