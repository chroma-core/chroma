"""Screen2vs4 source-file concurrency without changing the admitted main job."""
import json,time
from pathlib import Path
from .fineweb_parallel import ingest
from .fineweb_scale_runtime import validate_scale_job
from .preparation_attempt import new_attempt,save_attempt
from .tree_manifest import checked_artifact


def pilot(job_path):
    job_path=Path(job_path);job=json.loads(job_path.read_text());bundle=json.loads(checked_artifact(job['source_plan'],job_path.parent).read_text());plan=bundle['plan'];validate_scale_job(job,plan,job_path.parent)
    first=plan['files'][:4];count=sum(f['row_count'] for f in first)
    if len(first)!=4 or any(f['row_count']!=f['parquet_rows'] for f in first) or count!=3281725:raise ValueError('frozen complete four-file real prefix differs')
    out=new_attempt(Path(job['owned_root'])/'pilot');started=time.monotonic();report=dict(status='running',rows=count,arms=[],configuration_change_admitted=False)
    try:
        manifest=json.loads(checked_artifact(job['source_manifest'],job_path.parent).read_text())
        for workers in (2,4):
            future=sum(job['stage_estimates_s'].values())+1800
            if time.monotonic()-started>=900 or job['stop_epoch']-time.time()<future+420:
                report.update(status='baseline-only-budget-declined' if report['arms'] else 'budget-declined');return report
            began=time.monotonic();epoch=time.time()
            result=ingest(manifest,out/('workers-'+str(workers)),count,workers=workers,chunk_rows=100000,batch_rows=2048,retries=1,max_source_bytes=16*1024**3,max_output_bytes=16*1024**3,max_wall_s=min(420,900-(time.monotonic()-started)))
            report['arms'].append(dict(workers=workers,started_epoch=epoch,finished_epoch=time.time(),wall_s=time.monotonic()-began,receipt=str(out/('workers-'+str(workers))/'ingest.json'),chunks=result['chunks'],charged_source_bytes=result['source_request_bytes_charged']))
            save_attempt(out,report)
        def canonical(chunks):
            return [(c['start_id'],c['row_count'],[(c[k]['bytes'],c[k]['sha256']) for k in ('vectors','uuids')]) for c in chunks]
        if canonical(report['arms'][0]['chunks'])!=canonical(report['arms'][1]['chunks']):raise ValueError('concurrency changes canonical vector or UUID bytes')
        report.update(status='passed',ratio_four_to_two=report['arms'][1]['wall_s']/report['arms'][0]['wall_s'],scope='Screening pair only. Main configuration remains2; independent complete resource samples and repeatability review precede any change.')
        return report
    except BaseException as error:report.update(status='failed',error=repr(error));raise
    finally:report['wall_s']=time.monotonic()-started;save_attempt(out,report)


def main():
    import argparse
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--job',required=True);a=p.parse_args();print(json.dumps(pilot(a.job)))
if __name__=='__main__':main()
