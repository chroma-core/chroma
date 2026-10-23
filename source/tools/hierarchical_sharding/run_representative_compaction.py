"""Run only the reviewed local representative fixture, preserving all evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import unittest

EXPECTED={
    'builder':'ff3dbe73449f96618fb6cebcbf33dac9617ad4e2562c9d3e5ea980830d713666',
    'exporter':'2e49280f32bd05dd6b8fa9026e556cb4c920bf7ee84779044dbf33ee8a4edaa5',
    'audit':'c05166d45165d2f1dfe6608e7411cea78055397e5c6dfb5cc918054c9ed03d93',
}


def main():
    parser=argparse.ArgumentParser()
    for name in ('root','manifest','builder','exporter','audit'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args();repo=Path(__file__).resolve().parents[2]
    digest=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
    manifest=json.loads(args.manifest.read_bytes());before={p:digest(repo/p) for p in manifest['files']}
    if before!=manifest['files']:raise ValueError('reviewed source closure changed')
    binaries={name:dict(path=str(getattr(args,name).resolve()),sha256=digest(getattr(args,name))) for name in EXPECTED}
    if any(value['sha256']!=EXPECTED[name] for name,value in binaries.items()):raise ValueError('reviewed binary changed')
    root=args.root.resolve()
    if root.exists():raise ValueError('fresh evidence root required')
    os.environ.update(SPANN_REPRESENTATIVE_ROOT=str(root),SPANN_DYNAMIC_BINARY=binaries['builder']['path'],SPANN_WRITE_PLANNER_BINARY=binaries['builder']['path'],SPANN_NAMESPACE_RUNTIME=binaries['exporter']['path'],SPANN_STORAGE_AUDIT=binaries['audit']['path'])
    from .test_representative_compaction import (prepare_representative,block_inventory,
                                                run_native,RepresentativeCompaction)
    status=dict(status='failed',binaries=binaries,source_manifest=str(args.manifest.resolve()))
    def expired(*_):raise TimeoutError('representative work deadline exceeded')
    previous=signal.signal(signal.SIGALRM,expired);signal.alarm(300)
    try:
        parent=prepare_representative(root,binaries['builder']['path'])
        (root/'preflight.json').write_text(json.dumps(dict(binaries=binaries,source_hashes=before),indent=2)+'\n')
        blocks=block_inventory(parent,binaries['audit']['path'],root/'before-export-blocks')
        (root/'before-export-blocks.json').write_text(json.dumps(blocks,indent=2)+'\n')
        run_native(binaries['exporter']['path'],['--ignored','--exact','export_representative_namespace_append','--nocapture'],root/'genesis-export',env=dict(os.environ,SPANN_NAMESPACE_INPUT=str(root/'record-input.json'),SPANN_NAMESPACE_OUTPUT=str(root/'record-export')))
        suite=unittest.TestSuite([RepresentativeCompaction('test_actual_records_selected_leaves_topology_and_private_failure')])
        result=unittest.TextTestRunner(stream=sys.stderr,verbosity=2).run(suite)
        if not result.wasSuccessful() or result.testsRun!=1 or result.skipped:raise ValueError('representative integration method failed or skipped')
        status['status']='passed'
    except BaseException as error:
        status['error']=repr(error)
        raise
    finally:
        signal.alarm(0);signal.signal(signal.SIGALRM,previous)
        try:
            status['end_source_hashes']={p:digest(repo/p) for p in before}
            if status['end_source_hashes']!=before:status['status']='failed-source-changed'
        except BaseException as error:status.update(status='failed-source-reconciliation',reconciliation_error=repr(error))
        if root.exists():(root/'run-result.json').write_text(json.dumps(status,indent=2)+'\n')
    if status['status']!='passed':raise ValueError('representative reconciliation failed')


if __name__=='__main__':main()
