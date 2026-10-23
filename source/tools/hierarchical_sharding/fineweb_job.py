"""Validate the bounded real10M preparation contract without data or network."""
import argparse,json
from pathlib import Path
from .fineweb_prepare import DATASET,REVISION,DIMENSION


def validate_job(job):
    if job.get('dataset')!=DATASET or job.get('revision')!=REVISION or job.get('dimension')!=DIMENSION or job.get('count')!=10000000 or job.get('metric')!='ip':
        raise ValueError('pinned real10M IP job required')
    ingest=job['ingest'];resources=job['resources'];spatial=job['spatial']
    if 'max_wall_s' in spatial:raise ValueError('spatial phase deadline is unsupported; use external whole-run guard')
    numeric=[ingest[k] for k in ('workers','chunk_rows','batch_rows','max_source_bytes','max_output_bytes','max_wall_s')]+[resources[k] for k in ('hard_timeout_s','max_new_logical_output_bytes','minimum_free_allocated_bytes','minimum_start_free_allocated_bytes','max_process_rss_bytes','monitor_interval_s')]+[spatial[k] for k in ('children','sample_size','iterations','batch_rows')]
    if any(type(v) is not int or v<1 for v in numeric) or ingest['workers']>16 or ingest['retries'] not in (0,1,2):raise ValueError('positive integer caps and bounded concurrency/retries required')
    if resources['minimum_start_free_allocated_bytes']<resources['minimum_free_allocated_bytes']+resources['max_new_logical_output_bytes']:raise ValueError('start disk reserve does not cover preparation cap and floor')
    additional=job['projected_compressed_column_bytes']-job['seed_compressed_column_bytes']
    if additional!=job['additional_projected_compressed_column_bytes'] or additional<0:raise ValueError('source projection mismatch')
    if ingest['max_source_bytes']<additional*(1+ingest['retries'])+16*1024**2:raise ValueError('source cap cannot cover projected reads and declared retries')
    dense=job['count']*DIMENSION*4
    if ingest['max_output_bytes']<dense or resources['max_new_logical_output_bytes']<ingest['max_output_bytes']+3*(dense+job['count']*8):raise ValueError('logical cap lacks routed retry headroom')
    if spatial['sample_size']>job['count'] or spatial['children']>spatial['sample_size']:raise ValueError('invalid whole-prefix sample or slot count')
    for path in (ingest['output'],spatial['output'],job['source_plan'],*([ingest['seed_previous']] if ingest.get('seed_previous') else [])):
        if not Path(path).is_absolute() or '..' in Path(path).parts:raise ValueError('absolute bounded preparation paths required')
    if Path(ingest['output'])==Path(spatial['output']):raise ValueError('ingest and spatial outputs must be distinct')
    return dict(status='passed',network_bytes=0,rows=job['count'],projected_additional_source_bytes=additional,preparation_cap_excludes_index_and_checkpoint_writes=True,hardlinked_seed_accounting='count path logical bytes conservatively; report allocated inode bytes separately without double-counting hardlinks')


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--job',required=True);args=parser.parse_args()
    print(json.dumps(validate_job(json.loads(Path(args.job).read_text())),sort_keys=True))

if __name__=='__main__':main()
