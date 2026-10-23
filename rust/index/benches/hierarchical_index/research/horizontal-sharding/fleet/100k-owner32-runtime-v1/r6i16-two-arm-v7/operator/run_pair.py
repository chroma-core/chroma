"""One baseline-first paired attempt; cloud shutdown remains independently enforced."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import time

BASE = Path('/home/ubuntu/spann-owner32-build-v6/epoch4')
BASE_BINARY_SHA = 'afb126d7e5933d86e33b29ee3f50e3c687cea0dcd1b50c5e1d74f035ab1e4ea6'
BASE_MANIFEST_SHA = 'edf371fcab03dcbb879e99fe622e7ec8ac323523605c68756dfbdbd7a94bacd2'
BATCH_MANIFEST_SHA = 'de9885cd18d092c061d4b744ee1b04b3cf34bcd4f24a3ffeebdd1c00b7102dd3'
BASE_RECEIPT_SHA = '8ac2b87280db6867b422e3b91f147ff65cc477c1953b8beeea983bae7911ba8a'

def digest(p):
    h = hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda: f.read(1048576), b''):
            h.update(b)
    return h.hexdigest()

def save(p, value):
    with Path(p).open('x') as f:
        json.dump(value, f, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())

def clean(p):
    p = Path(p)
    if not p.is_absolute() or '..' in p.parts:
        raise ValueError('Normalized absolute paths required')
    if any(x.is_symlink() for x in [p, *p.parents]):
        raise ValueError('Symlink path forbidden')
    return p

def check_source(root, manifest, expected, deadline):
    if digest(manifest) != expected:
        raise ValueError('Source manifest differs')
    m = json.loads(Path(manifest).read_text())
    for rel, desc in m['files'].items():
        if time.time() >= deadline - 300:
            raise TimeoutError('Source validation exhausted preservation reserve')
        p = root / rel
        if p.is_symlink() or p.stat().st_size != desc['bytes'] or digest(p) != desc['sha256']:
            raise ValueError('Source member differs: ' + rel)
    for rel, target in m['symlinks'].items():
        if os.readlink(root / rel) != target or not (root / rel).resolve().is_relative_to(root.resolve()):
            raise ValueError('Source symlink differs')

def paired_policy(plan):
    p = json.loads(json.dumps(plan))
    if p['limits']['fresh_window_seconds'] != 1800 or p['limits']['compute_usd'] != 2.4192:
        raise ValueError('Unexpected input operational policy')
    p['limits']['fresh_window_seconds'] = 2400
    p['limits']['compute_usd'] = 2.688
    return p

def module(control, name):
    sys.path.insert(0, str(control / 'operator'))
    s = importlib.util.spec_from_file_location(name, control / 'operator/remote_stages.py')
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    return m

def prepare(batched, baseline, proof, host, deadline):
    # Existing extracted batched control is immutable except fresh operational bindings.
    for p in [batched, baseline, proof, host]:
        clean(p)
    if baseline.exists() or baseline.is_relative_to(batched) or batched.is_relative_to(baseline):
        raise ValueError('Controls must be fresh and disjoint')
    if deadline - time.time() < 600 + 600 + 300:
        raise TimeoutError('Build, both science caps and preservation cannot fit')
    check_source(BASE / 'source', BASE / 'source-manifest.json', BASE_MANIFEST_SHA, deadline)
    check_source(batched / 'source', batched / 'source-manifest.json', BATCH_MANIFEST_SHA, deadline)
    receipt = json.loads((BASE / 'build/result.json').read_text())
    if digest(BASE / 'build/result.json') != BASE_RECEIPT_SHA or digest(BASE / 'build/native') != BASE_BINARY_SHA:
        raise ValueError('Retained baseline receipt or binary differs')
    if receipt['binary'] != str(BASE / 'build/native') or receipt['binary_sha256'] != BASE_BINARY_SHA or receipt['source_manifest_sha256'] != BASE_MANIFEST_SHA:
        raise ValueError('Retained baseline provenance differs')
    if receipt['profile']['opt_level'] != '3' or receipt['profile']['debuginfo'] != 0:
        raise ValueError('Baseline profile differs')
    plan = paired_policy(json.loads((batched / 'PLAN-TEMPLATE.json').read_text()))
    # Only these operational policy fields change. Both supervisors use identical bytes.
    baseline.mkdir()
    for name in ['supervise.py', 'SPEC-TEMPLATE.json']:
        shutil.copy2(batched / name, baseline / name)
    shutil.copytree(batched / 'operator', baseline / 'operator')
    shutil.copytree(BASE / 'source', baseline / 'source', symlinks=True)
    shutil.copy2(BASE / 'source-manifest.json', baseline / 'source-manifest.json')
    (baseline / 'build').mkdir()
    # Preserve the receipt bytes and its original binary path; never rewrite history.
    shutil.copy2(BASE / 'build/result.json', baseline / 'build/result.json')
    save(baseline / 'baseline-reuse-provenance.json', {
        'original_receipt': str(BASE / 'build/result.json'), 'original_receipt_sha256': BASE_RECEIPT_SHA,
        'receipt_copy_sha256': digest(baseline / 'build/result.json'),
        'binary': str(BASE / 'build/native'), 'binary_sha256': BASE_BINARY_SHA,
        'source_copy': str(baseline / 'source'), 'source_manifest_sha256': BASE_MANIFEST_SHA})
    for control, manifest in [(baseline, BASE_MANIFEST_SHA), (batched, BATCH_MANIFEST_SHA)]:
        template = dict(plan, source_manifest_sha256=manifest)
        if control == baseline:
            template['source_archive'] = {'sha256': 'bdee8c4ad3ff8ebe683ca02d16b9a57a9f38330eb6441de563802f03918fcfe1', 'bytes': 2364530}
        # Extracted templates are operational inputs, not historical receipts.
        (control / 'PLAN-TEMPLATE.json').write_text(json.dumps(template, indent=2) + '\n')
        shutil.copy2(host, control / 'host-admission.json') if host != control / 'host-admission.json' else None
        (control / 'tmp').mkdir(exist_ok=True)
    check_source(baseline / 'source', baseline / 'source-manifest.json', BASE_MANIFEST_SHA, deadline)
    return receipt

def verify_new_build(control):
    receipt = json.loads((control / 'build/result.json').read_text())
    if receipt.get('bench_artifact_fresh') is not False:
        raise ValueError('New benchmark compilation not proven before either arm')
    actual = digest(receipt['binary'])
    if actual != receipt['binary_sha256'] or actual == BASE_BINARY_SHA:
        raise ValueError('New binary mismatches receipt or equals known baseline before either arm')
    return {'bench_artifact_fresh': False, 'binary_sha256': actual,
            'baseline_binary_sha256': BASE_BINARY_SHA, 'decision': 'admitted-before-either-arm'}

def main():
    a = argparse.ArgumentParser()
    for flag in ['batched', 'baseline', 'science-parent', 'proof', 'host']:
        a.add_argument('--' + flag, type=Path, required=True)
    a.add_argument('--deadline', type=float, required=True)
    args = a.parse_args()
    outcome = {'order': ['baseline', 'batched'], 'deadline': args.deadline, 'arms': []}
    result_path = args.batched / 'paired-result.json'
    try:
        if not __debug__ or os.environ.get('PYTHONOPTIMIZE'):
            raise ValueError('Assertions must be enabled')
        prepare(args.batched, args.baseline, args.proof, args.host, args.deadline)
        stages = module(args.batched, 'batched_stages')
        os.environ['TMPDIR'] = str(args.batched / 'tmp')
        stages.build(args.batched, args.deadline)
        save(args.batched / 'pre-science-build-gate.json', verify_new_build(args.batched))
        parent = clean(args.science_parent)
        if parent.exists() or not parent.parent.is_dir():
            raise ValueError('Science parent must be fresh under an existing real parent')
        parent.mkdir()
        for label, control in [('baseline', args.baseline), ('batched', args.batched)]:
            if args.deadline - time.time() < (600 if label == 'baseline' else 300) + 300:
                raise TimeoutError('Remaining arm allowances and preservation cannot fit')
            stage = module(control, label + '_stages')
            if label == 'baseline':
                spec = json.loads((control / 'SPEC-TEMPLATE.json').read_text())
                spec['vectors']['path'] = json.loads(args.host.read_text())['input_path']
                stage.save(control / 'input-binding.json', stage.bind_input(spec, args.deadline))
            os.environ['TMPDIR'] = str(control / 'tmp')
            rc = stage.fill_and_run(control, args.deadline, parent / label, args.proof, args.host)
            outcome['arms'].append({'label': label, 'returncode': rc, 'control': str(control)})
            if rc:
                raise RuntimeError(label + ' arm failed; no retry')
        outcome['status'] = 'completed'
    except BaseException as e:
        outcome['status'] = 'failed'
        outcome['error'] = repr(e)
        raise
    finally:
        save(result_path, outcome)

if __name__ == '__main__':
    main()
