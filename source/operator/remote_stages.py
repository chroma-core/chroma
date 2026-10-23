"""Explicit remote stages. Invocation alone does not authorize a new machine window."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from process_lifecycle import run_owned
from build_validity import refresh, require_recompiled

PACKAGE_SHA = 'ffaf0c2cdf1e5e3243cc6ab57d8d7b95b3a5d42287781dd3a433951db044c372'
COMPILER_SHA = '028fd60b0e0add5505c661cd3ccde91393d615c8729bd95d94ab1e91409b36a1'

PYTHON = '/usr/bin/python3'
TOOLCHAIN = Path('/home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin')
TARGET = '/home/ubuntu/spann-owner32-build-v6/target'
CHOSEN_PROFILE_ENV = {'CARGO_PROFILE_RELEASE_DEBUG': '0'}


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            value.update(block)
    return value.hexdigest()


def save(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def remaining(deadline, required):
    if deadline - time.time() < required + 180:
        raise TimeoutError('Stage cannot fit its allowance and collection/shutdown reserve')


def environment():
    if not __debug__ or os.environ.get('PYTHONOPTIMIZE'):
        raise RuntimeError('Assertions must be enabled')
    forbidden = ['RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER']
    if any(os.environ.get(key) for key in forbidden):
        raise RuntimeError('Ambient Rust compilation overrides are forbidden')
    inherited_profile = {key: value for key, value in os.environ.items() if key.startswith('CARGO_PROFILE_')}
    if any(key != 'CARGO_PROFILE_RELEASE_DEBUG' for key in inherited_profile):
        raise RuntimeError('Unreviewed inherited Cargo profile override')
    env = dict(os.environ)
    env.update(CHOSEN_PROFILE_ENV)
    env.update(RUSTC=str(TOOLCHAIN / 'rustc'), CARGO_TARGET_DIR=TARGET)
    return env


def build_command():
    return [str(TOOLCHAIN / 'cargo'), 'build', '--release', '--locked', '--offline',
            '-j32', '-p', 'chroma-index', '--bench', 'hierarchical_spann_shard',
            '--message-format=json']


def validate_artifact(artifacts):
    if len(artifacts) != 1:
        raise ValueError('Expected exactly one optimized artifact')
    require_recompiled(artifacts[0])
    profile = artifacts[0]['profile']
    if profile.get('opt_level') != '3' or profile.get('debuginfo') != 0:
        raise ValueError('Artifact differs from opt-level3/debuginfo0 contract')
    return artifacts[0]


def science_command(control, plan):
    return [PYTHON, str(control / 'supervise.py'), '--plan', str(plan)]


def bind_input(spec, deadline):
    input_started = time.monotonic()
    full = hashlib.sha256()
    prefix = hashlib.sha256()
    seen = 0
    with Path(spec['vectors']['path']).open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            remaining(deadline, 300)
            full.update(block)
            prefix.update(block[:max(0, min(len(block), spec['rows'] * spec['dimension'] * 4 - seen))])
            seen += len(block)
    if seen != spec['vectors']['bytes'] or full.hexdigest() != spec['vectors']['sha256']:
        raise ValueError('Retained full input or prefix differs')
    return  {'source_bytes': seen,
         'source_sha256': full.hexdigest(), 'prefix_bytes': spec['rows'] * spec['dimension'] * 4,
         'prefix_sha256': prefix.hexdigest(), 'seconds': time.monotonic() - input_started}


def build(control, deadline):
    remaining(deadline, 300)
    env = environment()
    if digest(TOOLCHAIN / 'rustc') != COMPILER_SHA:
        raise ValueError('Compiler differs')
    spec = json.loads((control / 'SPEC-TEMPLATE.json').read_text())
    host = json.loads((control / 'host-admission.json').read_text())
    spec['vectors']['path'] = host['input_path']
    save(control / 'input-binding.json', bind_input(spec, deadline))
    folder = control / 'build'
    folder.mkdir(exist_ok=False)
    baseline_path = Path(__file__).with_name('cache-baseline-source-manifest.json')
    if digest(baseline_path) != 'edf371fcab03dcbb879e99fe622e7ec8ac323523605c68756dfbdbd7a94bacd2':
        raise ValueError('Baseline source manifest differs')
    refreshed = refresh(control / 'source', json.loads((control / 'source-manifest.json').read_text()), json.loads(baseline_path.read_text()))
    save(folder / 'source-refresh.json', refreshed)
    started = time.monotonic()
    with (folder / 'stdout.jsonl').open('x') as stdout, (folder / 'stderr').open('x') as stderr:
        result = run_owned(build_command(), folder / 'process.json', 300,
                           cwd=control / 'source', env=env, stdout=stdout, stderr=stderr)
    if result.returncode:
        raise RuntimeError('Offline build failed; retain logs and stop')
    artifacts = []
    for line in (folder / 'stdout.jsonl').read_text().splitlines():
        message = json.loads(line)
        if message.get('reason') == 'compiler-artifact' and message.get('target', {}).get('name') == 'hierarchical_spann_shard' and message.get('executable'):
            artifacts.append(message)
    validate_artifact(artifacts)
    binary = folder / 'native'
    shutil.copy2(artifacts[0]['executable'], binary)
    save(folder / 'result.json', {'binary': str(binary), 'binary_sha256': digest(binary),
         'compiler_sha256': COMPILER_SHA, 'opt_level': '3', 'profile': artifacts[0]['profile'],
         'source_manifest_sha256': digest(control / 'source-manifest.json'),
         'seconds': time.monotonic() - started, 'command': build_command(),
         'source_refresh': refreshed, 'bench_artifact_fresh': artifacts[0]['fresh'],
         'chosen_profile_environment': CHOSEN_PROFILE_ENV,
         'inherited_profile_environment': {key: value for key, value in os.environ.items() if key.startswith('CARGO_PROFILE_')},
         'build_environment': {key: env.get(key) for key in ['RUSTC', 'CARGO_TARGET_DIR', 'CARGO_PROFILE_RELEASE_DEBUG', 'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'PYTHONOPTIMIZE']}})


def prepare_output_root(plan, spec, validator):
    # Reuse the supervisor's path admission before any data-directory mutation.
    validator(plan, spec)
    root = Path(plan['data_output_root'])
    if root.exists() or root.is_symlink():
        raise ValueError('Data epoch root must be fresh, including empty roots')
    if not root.parent.is_dir() or root.parent.is_symlink():
        raise ValueError('Declared epoch parent must already exist and be real')
    root.mkdir()  # Deliberately nonrecursive: never create an unknown ancestor.
    if Path(spec['output']).exists():
        raise ValueError('Native index must remain absent')


def fill_and_run(control, deadline, output_root, shutdown_proof, host_admission):
    remaining(deadline, 300)
    environment()
    build_receipt = control / 'build/result.json'
    receipt = json.loads(build_receipt.read_text())
    spec = json.loads((control / 'SPEC-TEMPLATE.json').read_text())
    host = json.loads(host_admission.read_text())
    spec['vectors']['path'] = host['input_path']
    binding = json.loads((control / 'input-binding.json').read_text())
    if binding['source_sha256'] != spec['vectors']['sha256'] or binding['prefix_bytes'] != spec['rows'] * spec['dimension'] * 4:
        raise ValueError('Input/prefix binding differs from declared cohort')
    prefix_sha = binding['prefix_sha256']
    spec.update(prefix_sha256=prefix_sha, output=str(output_root / 'index'))
    spec_path = control / 'spec.json'
    save(spec_path, spec)
    plan = json.loads((control / 'PLAN-TEMPLATE.json').read_text())
    plan.update(binary=receipt['binary'], binary_sha256=receipt['binary_sha256'],
                build_receipt=str(build_receipt), build_receipt_sha256=digest(build_receipt),
                source_root=str(control / 'source'), spec=str(spec_path), spec_sha256=digest(spec_path),
                deadline_unix=deadline, shutdown_proof=str(shutdown_proof),
                shutdown_proof_sha256=digest(shutdown_proof), receipt_dir=str(control / 'measurement'),
                data_mount=host['data_mount'], source_prefix_sha256=prefix_sha,
                data_output_root=str(output_root), control_root=str(control), host_admission=str(host_admission),
                host_admission_sha256=digest(host_admission),
                selected_instance_id=host['instance_id'], region=host['region'])
    module_spec = importlib.util.spec_from_file_location('admitted_supervisor', control / 'supervise.py')
    supervisor = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(supervisor)
    prepare_output_root(plan, spec, supervisor.policy_and_paths)
    plan_path = control / 'plan.json'
    save(plan_path, plan)
    return run_owned(science_command(control, plan_path), control / 'supervisor-process.json',
                     deadline - time.time() - 180, graceful_seconds=10).returncode


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=['build', 'science'])
    parser.add_argument('--control', required=True, type=Path)
    parser.add_argument('--deadline', required=True, type=float)
    parser.add_argument('--output-root', type=Path)
    parser.add_argument('--shutdown-proof', type=Path)
    parser.add_argument('--host-admission', type=Path)
    args = parser.parse_args()
    if args.stage == 'build':
        build(args.control, args.deadline)
    else:
        if args.output_root is None or args.shutdown_proof is None or args.host_admission is None:
            parser.error('Science requires output root and shutdown proof')
        raise SystemExit(fill_and_run(args.control, args.deadline, args.output_root, args.shutdown_proof, args.host_admission))


if __name__ == '__main__':
    main()
