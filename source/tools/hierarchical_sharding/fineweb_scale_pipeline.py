"""Run guarded preparation followed by mandatory private archive and full restore."""
import argparse,json,time
from pathlib import Path
from .fineweb_scale_prepare import prepare
from .fineweb_scale_archive import archive_and_restore
from .artifact_store import ArtifactStore
from .tree_manifest import artifact,canonical_bytes,_atomic_write


def run(job_path,store_factory=None):
    job_path=Path(job_path);job=json.loads(job_path.read_text());path=Path(job['owned_root'])/'pipeline.json'
    if path.exists():raise FileExistsError('pipeline output is immutable; materialize a fresh attempt')
    report=dict(status='running',job=artifact(job_path),started_epoch=time.time());_atomic_write(path,canonical_bytes(report))
    try:
        if job.get('run_concurrency_pilot',False):
            from .fineweb_scale_pilot import pilot
            report['concurrency_pilot']=pilot(job_path)
        prepared=prepare(job_path)
        if prepared['status']!='passed':raise ValueError('full preparation/reference checks did not pass')
        report['preparation']=prepared;_atomic_write(path,canonical_bytes(report))
        if store_factory is None:
            import boto3
            from botocore.config import Config
            config=Config(connect_timeout=5,read_timeout=60,retries=dict(max_attempts=2,mode='standard'))
            store=ArtifactStore(job['artifact_store']['bucket'],job['artifact_store']['account'],boto3.client('s3',region_name='us-east-1',config=config),boto3.client('sts',region_name='us-east-1',config=config),job['artifact_store']['role'])
        else:store=store_factory(job)
        report['archive']=archive_and_restore(job,store)
        report.update(status='passed',finished_epoch=time.time());return report
    except BaseException as error:report.update(status='failed',error=repr(error));raise
    finally:_atomic_write(path,canonical_bytes(report))

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--job',required=True);a=p.parse_args();print(json.dumps(run(a.job)))
if __name__=='__main__':main()
