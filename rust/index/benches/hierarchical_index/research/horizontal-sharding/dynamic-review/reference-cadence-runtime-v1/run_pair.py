"""One no-build cadence pair with immutable epoch12 binary/source reuse."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import time

ORIGINAL = Path('/home/ubuntu/spann-owner32-build-v6/local-reassignment-epoch1/build-control')
RUNTIME = Path('/home/ubuntu/spann-owner32-build-v6/local-reassignment-epoch1/off')
BINARY_SHA = 'e506c9af99461d9f63f9e4ac382d2fbe026aeb49b77e19059934fb2c842a3af5'
MANIFEST_SHA = 'cd405935029a6569b277371a0eae82e67105ba8617e002b3e38490a646bda07a'
RECEIPT_SHA = '829cad79b8563b91b877b1740f82b4037e411874ecb180fb36778fd20cb9c497'
COMPILER_SHA = '028fd60b0e0add5505c661cd3ccde91393d615c8729bd95d94ab1e91409b36a1'
COMPILER = Path('/home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc')
READINESS = Path(__file__).with_name('READINESS.json')

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

def science_spec(original, cadence):
    spec = json.loads(json.dumps(original))
    if cadence not in [4096,100000]:
        raise ValueError('Cadence must be one of the two fixed arms')
    if spec['rows'] != 100000 or spec['dimension'] != 768 or spec['threads'] != 32 or spec['owner_slots'] != 1 or spec['balance_every'] != 4096 or spec['split_rows'] != 8192 or spec['split_working_bytes'] != 8589934592:
        raise ValueError('Retained baseline shape/settings differ')
    if spec['single_owner_local_reassignment'] is not False or spec['single_owner_local_scrub'] is not True or spec['immutable_handle_batching'] is not False or spec['owner_eligibility_prefix'] is not False or spec['full_records']['queue_cache_pages_per_home'] != 4 or spec['decoded_vector_cache'] != {'payload_bytes_per_owner':402653184,'aggregate_payload_bytes':402653184}:
        raise ValueError('Common scientific flags differ')
    spec.update(balance_every=cadence,split_rows=100000,split_working_bytes=68719476736)
    return spec

def readiness():
    value = json.loads(READINESS.read_text())
    if value.get('accepted') is not True or value.get('binary_sha256') != BINARY_SHA or value.get('source_manifest_sha256') != MANIFEST_SHA or not value.get('correctness_receipt_sha256'):
        raise ValueError('Spec-only reuse and correctness readiness are not admitted')
    return value

RUNTIME_HASHES = {'supervise.py': '967f8ffc5ff8f0a37878537150cd2a0a9959909aa244434bfdcb9d446c2f39ef', 'operator/remote_stages.py': 'd9e04c8a123f3d92d7bea4b4c5da74fc90109c36639a29ab1efdc0b35aef29fa', 'operator/process_lifecycle.py': 'f9913422576a5d133eec68b41dbf5a66bab61958442c0b41a56dc49fc0d8bb27', 'operator/build_validity.py': '379f20b99f0a200c5cdbac30d736d14646e58735403fb4d6f3630f1221d5e20d', 'SPEC-TEMPLATE.json': '3e630c12a7837e59597c59c9975a7cce07a80d7746545991d16f265d8d903e51', 'PLAN-TEMPLATE.json': 'ce1f8675e2b80855df6368b173d20a2b4c7e836a1fdef11492d2c4a2b520f254', 'input-binding.json': 'c68a539f2c308e265715a021cb15b965bb44311fd00e87979fefa92df83c9db0'}

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

def prepare(control, host, deadline, cadence):
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
    save(control/'SPEC-TEMPLATE.json',science_spec(json.loads((RUNTIME/'SPEC-TEMPLATE.json').read_text()),cadence))
    shutil.copy2(RUNTIME/'PLAN-TEMPLATE.json',control/'PLAN-TEMPLATE.json')
    (control/'build').mkdir();shutil.copy2(ORIGINAL/'build/result.json',control/'build/result.json')
    shutil.copy2(host,control/'host-admission.json');(control/'tmp').mkdir()
    save(control/'reuse-provenance.json',{'original_receipt':str(ORIGINAL/'build/result.json'),'receipt_sha256':RECEIPT_SHA,'receipt_copy_sha256':digest(control/'build/result.json'),'original_binary':str(ORIGINAL/'build/native'),'binary_sha256':BINARY_SHA,'original_source':str(ORIGINAL/'source'),'copied_source':str(control/'source'),'source_manifest_sha256':MANIFEST_SHA,'runtime_origin':str(RUNTIME),'spec_delta':{'balance_every':cadence,'common_split_rows':100000,'common_split_working_bytes':68719476736},'build_executed':False})

def main():
    parser = argparse.ArgumentParser()
    for name in ['short-control','long-control','host','proof','science-parent']:
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--deadline',type=float,required=True)
    args = parser.parse_args()
    controls=[args.short_control,args.long_control]
    for path in controls+[args.host,args.proof,args.science_parent]:clean(path)
    if any(x.is_relative_to(y)or y.is_relative_to(x)for x in controls for y in controls if x!=y):raise ValueError('Controls overlap')
    if args.short_control==args.long_control:raise ValueError('Distinct arm controls required')
    protected=[ORIGINAL,RUNTIME,Path('/home/ubuntu/spann-fineweb100k-retained-v1')]
    for fresh in controls+[args.science_parent]:
        for keep in protected:
            if fresh.is_relative_to(keep)or keep.is_relative_to(fresh):raise ValueError('Fresh path overlaps retained roots')
    for control in controls:
        if args.science_parent.is_relative_to(control)or control.is_relative_to(args.science_parent):raise ValueError('Science and controls overlap')
    if not __debug__ or os.environ.get('PYTHONOPTIMIZE'):raise ValueError('Assertions required')
    admitted=readiness()
    if args.deadline-time.time()<900:raise TimeoutError('Both science caps and preservation cannot fit')
    if args.science_parent.exists()or not args.science_parent.parent.is_dir():raise ValueError('Fresh science parent under existing real parent required')
    result={'build_executed':False,'deadline':args.deadline,'order':['cadence4096','cadence100000'],'arms':[],'readiness':admitted}
    prepare(args.short_control,args.host,args.deadline,4096)
    try:
        args.science_parent.mkdir()
        for label,control,cadence in [('cadence4096',args.short_control,4096),('cadence100000',args.long_control,100000)]:
            if args.deadline-time.time()<(600 if cadence==4096 else 300)+300:raise TimeoutError('Remaining arm caps and preservation cannot fit')
            if cadence==100000:prepare(control,args.host,args.deadline,cadence)
            sys.path.insert(0,str(control/'operator'))
            module_spec=importlib.util.spec_from_file_location('retained_stages_'+label,control/'operator/remote_stages.py')
            stage=importlib.util.module_from_spec(module_spec);module_spec.loader.exec_module(stage)
            os.environ['TMPDIR']=str(control/'tmp')
            rc=stage.fill_and_run(control,args.deadline,args.science_parent/label,args.proof,args.host)
            result['arms'].append({'label':label,'returncode':rc,'control':str(control)})
            if rc:raise RuntimeError(label+' failed; no retry')
        result['status']='completed'
        return 0
    except BaseException as error:result.update(status='failed',error=repr(error));raise
    finally:save(args.short_control/'reuse-pair-result.json',result)

if __name__ == '__main__':
    raise SystemExit(main())
