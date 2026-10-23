"""One no-build queue-cache diagnostic with immutable baseline reuse."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import time

ORIGINAL = Path('/home/ubuntu/spann-owner32-build-v6/epoch4')
RUNTIME = Path('/home/ubuntu/spann-owner32-build-v6/epoch7/baseline')
BINARY_SHA = 'afb126d7e5933d86e33b29ee3f50e3c687cea0dcd1b50c5e1d74f035ab1e4ea6'
MANIFEST_SHA = 'edf371fcab03dcbb879e99fe622e7ec8ac323523605c68756dfbdbd7a94bacd2'
RECEIPT_SHA = '8ac2b87280db6867b422e3b91f147ff65cc477c1953b8beeea983bae7911ba8a'
COMPILER_SHA = '028fd60b0e0add5505c661cd3ccde91393d615c8729bd95d94ab1e91409b36a1'
COMPILER = Path('/home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc')

def digest(p):
    h = hashlib.sha256()
    with Path(p).open('rb') as f:
        for block in iter(lambda: f.read(1048576), b''):
            h.update(block)
    return h.hexdigest()

def save(p, value):
    with Path(p).open('x') as f:
        json.dump(value, f, indent=2)
        f.write('\n');f.flush();os.fsync(f.fileno())

def clean(p):
    p = Path(p)
    if not p.is_absolute() or '..' in p.parts or any(x.is_symlink() for x in [p,*p.parents]):
        raise ValueError('Real normalized absolute path required')
    return p

def science_spec(original):
    spec = json.loads(json.dumps(original))
    if spec['full_records']['queue_cache_pages_per_home'] != 1:
        raise ValueError('Expected original one-page baseline')
    spec['full_records']['queue_cache_pages_per_home'] = 4
    return spec

def operational_plan(original):
    plan = json.loads(json.dumps(original))
    if plan['limits']['fresh_window_seconds'] != 2400 or plan['limits']['compute_usd'] != 2.688:
        raise ValueError('Expected accepted epoch7 runtime policy')
    plan['limits']['fresh_window_seconds'] = 900
    plan['limits']['compute_usd'] = 1.008
    return plan

RUNTIME_HASHES = {'supervise.py': '967f8ffc5ff8f0a37878537150cd2a0a9959909aa244434bfdcb9d446c2f39ef', 'operator/remote_stages.py': 'd9e04c8a123f3d92d7bea4b4c5da74fc90109c36639a29ab1efdc0b35aef29fa', 'operator/process_lifecycle.py': 'f9913422576a5d133eec68b41dbf5a66bab61958442c0b41a56dc49fc0d8bb27', 'operator/build_validity.py': '379f20b99f0a200c5cdbac30d736d14646e58735403fb4d6f3630f1221d5e20d', 'SPEC-TEMPLATE.json': '9b620681d3e070670450c1730219f772627c53ce7dca7564411393147531dcc7', 'PLAN-TEMPLATE.json': '0fa0ae8f45166d3ba9942804767d41f2bff8aa25cbd80890af00bdf463b3ce7d', 'input-binding.json': 'f12d66775aaa9205c62989d9d37f52bafccce8b5be941e5f8f8d4d07a0e36491'}

def verify_retained(deadline):
    for name, expected in RUNTIME_HASHES.items():
        if digest(RUNTIME/name) != expected:
            raise ValueError("Retained runtime differs: "+name)
    for path, expected in [(ORIGINAL/'build/native',BINARY_SHA),(ORIGINAL/'build/result.json',RECEIPT_SHA),(ORIGINAL/'source-manifest.json',MANIFEST_SHA),(RUNTIME/'source-manifest.json',MANIFEST_SHA),(COMPILER,COMPILER_SHA)]:
        if digest(path) != expected:
            raise ValueError('Retained baseline/compiler binding differs: '+str(path))
    receipt = json.loads((ORIGINAL/'build/result.json').read_text())
    if receipt['binary'] != str(ORIGINAL/'build/native') or receipt['binary_sha256'] != BINARY_SHA or receipt['source_manifest_sha256'] != MANIFEST_SHA or receipt['profile']['opt_level'] != '3' or receipt['profile']['debuginfo'] != 0:
        raise ValueError('Retained baseline provenance differs')
    manifest = json.loads((ORIGINAL/'source-manifest.json').read_text())
    for rel, row in manifest['files'].items():
        if deadline-time.time() < 600:
            raise TimeoutError('Preparation cannot preserve full science and collection/shutdown allowance')
        p = ORIGINAL/'source'/rel
        if p.is_symlink() or p.stat().st_size != row['bytes'] or digest(p) != row['sha256']:
            raise ValueError('Baseline source differs: '+rel)
    for rel, target in manifest['symlinks'].items():
        p = ORIGINAL/'source'/rel
        if os.readlink(p) != target or not p.resolve().is_relative_to((ORIGINAL/'source').resolve()):
            raise ValueError('Baseline source symlink differs')

def prepare(control, host, deadline):
    clean(control);clean(host)
    if control.exists() or control.is_relative_to(ORIGINAL) or ORIGINAL.is_relative_to(control) or control.is_relative_to(RUNTIME) or RUNTIME.is_relative_to(control):
        raise ValueError('Fresh disjoint control required')
    verify_retained(deadline)
    control.mkdir()
    for name in ['supervise.py','input-binding.json']:
        shutil.copy2(RUNTIME/name,control/name)
    shutil.copytree(RUNTIME/'operator',control/'operator')
    shutil.copytree(ORIGINAL/'source',control/'source',symlinks=True)
    shutil.copy2(ORIGINAL/'source-manifest.json',control/'source-manifest.json')
    save(control/'SPEC-TEMPLATE.json',science_spec(json.loads((RUNTIME/'SPEC-TEMPLATE.json').read_text())))
    save(control/'PLAN-TEMPLATE.json',operational_plan(json.loads((RUNTIME/'PLAN-TEMPLATE.json').read_text())))
    (control/'build').mkdir();shutil.copy2(ORIGINAL/'build/result.json',control/'build/result.json')
    shutil.copy2(host,control/'host-admission.json');(control/'tmp').mkdir()
    save(control/'reuse-provenance.json',{'original_receipt':str(ORIGINAL/'build/result.json'),'receipt_sha256':RECEIPT_SHA,'receipt_copy_sha256':digest(control/'build/result.json'),'original_binary':str(ORIGINAL/'build/native'),'binary_sha256':BINARY_SHA,'original_source':str(ORIGINAL/'source'),'copied_source':str(control/'source'),'source_manifest_sha256':MANIFEST_SHA,'runtime_origin':str(RUNTIME),'spec_delta':{'full_records.queue_cache_pages_per_home':[1,4]},'build_executed':False})

def main():
    parser = argparse.ArgumentParser()
    for name in ['control','host','proof','output-root']:
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--deadline',type=float,required=True)
    args = parser.parse_args()
    clean(args.proof);clean(args.output_root)
    if not __debug__ or os.environ.get('PYTHONOPTIMIZE'):
        raise ValueError('Assertions required')
    prepare(args.control,args.host,args.deadline)
    result = {'build_executed':False,'deadline':args.deadline}
    try:
        sys.path.insert(0,str(args.control/'operator'))
        spec = importlib.util.spec_from_file_location('reused_stages',args.control/'operator/remote_stages.py')
        stage = importlib.util.module_from_spec(spec);spec.loader.exec_module(stage)
        os.environ['TMPDIR'] = str(args.control/'tmp')
        rc = stage.fill_and_run(args.control,args.deadline,args.output_root,args.proof,args.host)
        result.update(returncode=rc,status='completed' if rc==0 else 'failed')
        return rc
    except BaseException as error:
        result.update(status='failed',error=repr(error));raise
    finally:
        save(args.control/'reuse-result.json',result)

if __name__ == '__main__':
    raise SystemExit(main())
