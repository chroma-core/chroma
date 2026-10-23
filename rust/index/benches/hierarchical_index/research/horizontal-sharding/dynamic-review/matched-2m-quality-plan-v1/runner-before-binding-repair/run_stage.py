"""Explicit stage entry points for the reviewed future Linux experiment.

A frozen stage spec is required; this module does not create machine windows.
No stage is automatically followed by a cloud action or another treatment.
"""
import argparse
import json
import platform
from pathlib import Path
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=['prepare', 'build', 'diagnose'])
    parser.add_argument('--spec', required=True)
    args = parser.parse_args()
    if platform.system() != 'Linux':
        raise RuntimeError('test-machine Linux required; no local corpus execution')
    spec = json.loads(Path(args.spec).read_text())
    if time.time() + 60 >= spec['deadline_unix']:
        raise RuntimeError('fixed window exhausted')
    if args.stage == 'prepare':
        from prepare_pair import prepare
        prepare(spec['vectors'], spec['queries'], spec['output'], spec['vectors_sha256'],
                spec['queries_sha256'], spec['deadline_unix'])
    elif args.stage == 'build':
        from build_rounds import build
        build(spec['projection'], spec['binary'], spec['binary_sha256'], spec['output'],
              spec['source_id'], spec['boundary_hash'], spec['threads'], spec['deadline_unix'],
              spec['min_free_bytes'], spec['max_process_rss_bytes'], spec['guard_timer'])
    else:
        from diagnostic_spec import create
        from project_rounds import digest
        from supervise import run
        if digest(Path(spec['binary'])) != spec['binary_sha256']:
            raise ValueError('diagnostic binary hash mismatch')
        create(spec['projection'], spec['build_output'], spec['queries'], spec['reference'],
               spec['diagnostic_spec'], spec['source_id'])
        run([spec['binary'], 'diagnose-ground-truth', '--spec', spec['diagnostic_spec']],
            spec['cwd'], spec['output'], spec['deadline_unix'], spec['min_free_bytes'],
            spec['max_process_rss_bytes'], spec['max_log_bytes'])
        report = json.loads((Path(spec['output'])/'stdout').read_text())
        if report.get('policy') != 'saved-gt-quality-v1':
            raise ValueError('unexpected diagnostic report policy')


if __name__ == '__main__':
    main()
