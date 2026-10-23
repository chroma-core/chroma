"""Launch one explicitly selected stage with pinned inputs and existing guards.

Does not provision machines, install guards, download input, or chain stages.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
from supervise import run
from artifact_bindings import matching_budget, verify_implementation


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--admission', required=True)
    args=parser.parse_args()
    if platform.system()!='Linux': raise RuntimeError('Linux test machine required')
    admission=json.loads(Path(args.admission).read_text())
    stage=admission['stage']
    if stage not in ('prepare','prepare-global','construct-global','diagnose-global','build','diagnose'): raise ValueError('unknown stage')
    root=Path(__file__).resolve().parent
    for name, expected in admission['runner_files'].items():
        path=(root/name).resolve()
        if not path.is_relative_to(root) or sha(path)!=expected: raise ValueError('runner binding mismatch')
    required={p.name for p in root.glob('*.py') if not p.name.startswith('test_')}
    if set(admission['runner_files'])!=required: raise ValueError('incomplete runner source binding')
    spec_path=Path(admission['stage_spec']).resolve()
    if sha(spec_path)!=admission['stage_spec_sha256']: raise ValueError('stage spec mismatch')
    spec=json.loads(spec_path.read_text())
    matching_budget(admission, spec)
    if spec['implementation_checkout'] != admission['implementation_checkout']:
        raise ValueError('implementation checkout differs from admission')
    verify_implementation(spec)
    if spec.get('implementation_artifacts') != admission['implementation_artifacts']:
        raise ValueError('implementation binding differs from admission')
    if spec['deadline_unix']!=admission['deadline_unix'] or time.time()+60>=spec['deadline_unix']:
        raise ValueError('deadline mismatch or expired')
    subprocess.run(['systemctl','is-active','--quiet',admission['guard_timer']],check=True)
    # Exact timer deadline/local watchdog verification is an external admission requirement.
    env=dict(os.environ,PYTHONPATH=admission['implementation_checkout'],
             OPENBLAS_NUM_THREADS=str(admission['threads']),OMP_NUM_THREADS=str(admission['threads']),
             MKL_NUM_THREADS=str(admission['threads']))
    if stage in ('prepare', 'prepare-global'):
        run([sys.executable,str(root/'run_stage.py'),stage,'--spec',str(spec_path)],
            admission['implementation_checkout'],admission['supervision_output'],spec['deadline_unix'],
            admission['min_free_bytes'],admission['max_process_rss_bytes'],admission['max_log_bytes'],env)
    else:
        # Replace launcher so nested supervisors own only their direct native child.
        os.environ.update(env)
        os.execv(sys.executable,[sys.executable,str(root/'run_stage.py'),stage,'--spec',str(spec_path)])


if __name__=='__main__':main()
