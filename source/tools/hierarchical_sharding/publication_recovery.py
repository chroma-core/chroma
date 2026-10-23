"""Publish already completed immutable snapshots without launching any writers."""
import argparse,json,time
from pathlib import Path
from .guarded_host_batch import accepted_terminal
from .guarded_compaction_backend import run_guarded_jobs,inspect_parallel
from .subtree_guarded_build import jobs_for
from .subtree_orchestrate import inspect_host,pull_metadata,publish_checked
from .subtree_compaction import journal
from .tree_manifest import artifact,canonical_bytes,assemble_tree,content_hash


def load_completed(original,prepared):
    original=Path(original)
    if (original/'current.json').exists():raise ValueError('original case already published')
    events=[json.loads(line) for line in (original/'journal.jsonl').read_text().splitlines()]
    if not events or events[-1]['phase']!='failed':raise ValueError('original failed outcome must be preserved')
    config=json.loads((original/'config.json').read_text());jobs=json.loads((original/'jobs.json').read_text())
    expected,entries=jobs_for(prepared,config)
    if jobs!=expected:raise ValueError('original jobs differ from prepared/configured construction')
    batch=json.loads((original/'build/host-batch/config.json').read_text());report=json.loads((original/'build/host-batch/report.json').read_text())
    if report.get('complete') is not True or set(report['hosts'])!={e['host'] for e in batch['hosts']}:raise ValueError('original hosts are incomplete')
    for entry in batch['hosts']:
        terminal=report['hosts'][entry['host']]
        if terminal.get('run_id')!=entry['run_id'] or not accepted_terminal(terminal,len(entry['job_shard_ids'])):
            raise ValueError('original writer terminal is not successful and reconciled')
    return config,jobs,entries,batch,report


def recover(original,prepared_path,case_path):
    original=Path(original).resolve();prepared_path=Path(prepared_path);case=Path(case_path);case.mkdir(parents=True,exist_ok=False);log=case/'journal.jsonl';started=time.monotonic()
    try:
        prepared=json.loads(prepared_path.read_text());config,jobs,entries,batch,terminal_report=load_completed(original,prepared)
        provenance={name:artifact(original/name) for name in ['config.json','jobs.json','journal.jsonl','build/host-batch/config.json','build/host-batch/report.json']}
        (case/'recovery-inputs.json').write_bytes(canonical_bytes(dict(original=provenance,prepared=artifact(prepared_path))))
        journal(log,'recollection-start',scope='Read-only recovery; no writer or supervisor launch.')
        # The injected runner returns preserved evidence only. The backend collector
        # polls the actual owner terminal before streaming any immutable log.
        build=run_guarded_jobs(jobs,batch,case/'build',config['cpu_affinity'],config['jobs_per_host'],runner=lambda *_:terminal_report)
        if not build['complete']:raise ValueError('bounded recollection is incomplete')
        for number,entry in enumerate(batch['hosts']):
            fresh=json.loads((case/'build'/('host-'+str(number)+'-read-terminal.json')).read_text())
            if fresh!=terminal_report['hosts'][entry['host']]:raise ValueError('fresh terminal differs from original recorded successful writer identity')
        for path in (case/'build').glob('*.raw-log-metadata.json'):
            metadata=json.loads(path.read_text())
            if any(m['classification_occurrences']['balance_nonconvergence'] for m in metadata.values()):raise ValueError('full raw log contains balancing nonconvergence')
        if len(list((case/'build').glob('*.raw-log-metadata.json')))!=len(jobs):raise ValueError('full raw log metadata does not cover every owner')
        inspections=inspect_parallel(entries,lambda host,rows:inspect_host(host,rows,config['binary'],prepared),pull_metadata,case/'copies',config.get('verification_max_hosts',8))
        tree=assemble_tree(prepared_path,[case/'copies'/s['subtree_id']/'manifest-1.json' for s in prepared['subtrees']],1,case)
        (case/'tree-1.json').write_bytes(canonical_bytes(tree));(case/'remote-reopen.json').write_bytes(canonical_bytes(inspections))
        pointer=publish_checked(case/'current.json',tree,inspections,case)
        result=dict(status='passed',execution_backend='publication-only-recovery',original_failed_case=str(original),writer_rerun=False,tree=str((case/'tree-1.json').resolve()),tree_sha256=content_hash(tree),entries=entries,host_order=config['host_order'],binary=config['binary'],measured_binary=config['measured_binary'],prepared=str(prepared_path),checkpoint_rows=config['checkpoint_rows'],cpu_affinity=config['cpu_affinity'],jobs_per_host=config['jobs_per_host'],index_build=build,pointer=pointer,recovery_s=time.monotonic()-started,scope='Recovery duration excludes original writer construction. Original failure and raw logs remain preserved; full raw archive receipts are a separate preservation gate.')
        (case/'case.json').write_bytes(canonical_bytes(result));journal(log,'complete',report=result);return result
    except BaseException as error:journal(log,'failed',error=repr(error),retry_policy='Retain this case and use another fresh recovery output. Never rerun completed writers.');raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('original-case','prepared','case'):p.add_argument('--'+n,required=True)
    a=p.parse_args();print(json.dumps(recover(a.original_case,a.prepared,a.case)))
if __name__=='__main__':main()
