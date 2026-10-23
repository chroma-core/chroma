"""Connect admitted restore, deterministic binding, matched execution, collection.

This creates no admission, resource, watchdog or execution window. The externally
pinned admission must authorize the one generated bound_inputs descriptor.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time
from orchestration_contract import (ACCOUNT, BUDGET_KEYS, EPOCH, MAX_WIRE, checked,
    descriptor, load_json, relative, require, verified_manifest, write_new)

COHORT = dict(rows=2_000_000, dimension=768, metric='ip', source_id='fineweb-canonical2m-dynamic-v1')
ARTIFACTS = {'base_template', 'restore_script', 'restore_spec', 'binder', 'driver', 'collector',
             'collection', 'matched_inputs', 'queries', 'setup_artifacts', 'runner_artifacts',
             'implementation_artifacts', 'build_provenance', 'native_binary'}
EVIDENCE = {'resource_approval', 'start_attempt', 'watchdog_ready', 'mount_verification',
            'remote_guard', 'remote_guard_raw'}
STAGES = ('prepare-global', 'construct-global', 'diagnose-global')
REQUIRED_RESULTS = {'integration/matched-complete.json', 'integration/prepared/prepared.json',
    'integration/prepared/prepared-artifacts.json', 'integration/prepared/reference.jsonl',
    'integration/build/paired-build-artifacts.json', 'integration/build/construction-stage.json'}
for _arm in ('reference', 'authoritative'):
    REQUIRED_RESULTS.update({f'integration/build/{_arm}/manifest.json',
        f'integration/build/{_arm}/store/manifest.json', f'integration/build/{_arm}-spec.json',
        f'integration/diagnostic/{_arm}-spec.json', f'integration/diagnostic/{_arm}/result.json'})
    for _where in (f'integration/build/{_arm}-process', f'integration/diagnostic/{_arm}'):
        REQUIRED_RESULTS.update(f'{_where}/{tail}' for tail in ('result.json', 'stdout', 'stderr'))
# Only preparation uses launch_stage's outer supervision wrapper. Construction
# and diagnosis retain their per-arm process receipts above instead.
REQUIRED_RESULTS.update(f'integration/prepare-global-supervision/{tail}'
                        for tail in ('result.json', 'stdout', 'stderr'))



def verify_existing_implementation(base, runner_manifest):
    """Use the reviewed verifier before restore rather than duplicate its contract."""
    runner = Path(base['runner_directory']).resolve(strict=True)
    pinned = {str(checked(item)) for item in runner_manifest['files']}
    modules = {str(p.resolve()) for p in runner.glob('*.py') if not p.name.startswith('test_')}
    require(modules <= pinned and {'artifact_bindings.py','project_rounds.py'} <= {Path(p).name for p in modules},
            'incomplete runner source closure')
    previous = list(sys.path)
    sys.path.insert(0, str(runner))
    try:
        loader = importlib.util.spec_from_file_location('admitted_artifact_bindings', runner/'artifact_bindings.py')
        module = importlib.util.module_from_spec(loader)
        loader.loader.exec_module(module)
        import project_rounds
        require(Path(project_rounds.__file__).resolve() == runner/'project_rounds.py', 'wrong imported dependency')
        module.verify_implementation(base)
    finally:
        sys.path[:] = previous


def validate(spec, *, _cohort=COHORT):
    require(spec.get('format_version') == 1, 'unsupported orchestration format')
    admission_path = checked(spec['admission'])
    a = load_json(admission_path)
    require(a.get('format_version') == 1 and a.get('decision') == 'approved' and a.get('epoch') == EPOCH,
            'external approved admission is required')
    require(set(a['artifacts']) == ARTIFACTS and set(a['evidence']) == EVIDENCE, 'admission closure incomplete')
    paths = {key: checked(item) for key, item in a['artifacts'].items()}
    evidence = {key: checked(item) for key, item in a['evidence'].items()}
    base, collection = load_json(paths['base_template']), load_json(paths['collection'])
    root = Path(a['experiment_root']).resolve(strict=True)
    output = Path(spec['output'])
    require(output == root/'orchestration' and not output.exists(), 'fresh exact orchestration directory required')
    require(Path(base['integration_output']) == root/'integration', 'integration output differs')
    require(base.get('bound_inputs', 'absent') is None and a.get('generated_fields') == ['bound_inputs'],
            'only deterministic bound_inputs insertion may be authorized')
    require(a['expected']['cohort'] == _cohort, 'cohort differs from production contract')
    require(type(a['expected']['restore_objects']) is int and a['expected']['restore_objects'] > 0
            and (_cohort != COHORT or a['expected']['restore_objects'] == 21), 'restore proof count differs')
    require(all(base[k] == a['budgets'][k] for k in BUDGET_KEYS), 'base budget differs')
    require(all(type(base[k]) is int and base[k] > 0 for k in BUDGET_KEYS[1:-1]), 'invalid resource budget')
    stop = a['machine_stop_unix']
    require(time.time() < base['deadline_unix'] <= collection['deadline_unix'] - 120,
            'work/collection reserve exhausted')
    require(collection['deadline_unix'] <= stop - 60 and 0 < stop-time.time() <= 1200,
            'fixed stop/retrieval reserve differs')
    require(Path(collection['root']) == root, 'collection root differs')
    restore = load_json(paths['restore_spec'])
    require(restore['deadline_unix'] == base['deadline_unix']
            and restore['min_free_bytes'] == base['min_free_bytes']
            and Path(restore['attempt_dir']) == root/'restore', 'restore budget/output differs')
    inputs = load_json(paths['matched_inputs'])
    require(inputs['config'] == a['expected']['config']
            and a['expected']['config']['write_navigation'] == 'fp'
            and a['expected']['config']['fp_npa'] is True, 'explicit matched FP configuration differs')
    query = a['artifacts']['queries']
    require(query['bytes'] <= 2*1024*1024, 'query metadata byte cap')
    require(all(query[k] == inputs['queries'][k] for k in ('bytes', 'sha256')), 'frozen query binding differs')
    actual_ids = [json.loads(line)['query_id'] for line in paths['queries'].read_text().splitlines() if line.strip()]
    require(actual_ids == inputs['query_ids'] == a['expected']['query_ids']
            and len(actual_ids) == len(set(actual_ids)) == 16, 'ordered sixteen-query cohort differs')
    for key in ('setup_artifacts', 'runner_artifacts', 'implementation_artifacts', 'build_provenance'):
        require(base[key] == a['artifacts'][key], 'base implementation/provenance differs: ' + key)
    require(base['binary'] == str(paths['native_binary'])
            and base['binary_sha256'] == a['artifacts']['native_binary']['sha256'], 'native binary differs')
    verified_manifest(a['artifacts']['setup_artifacts'],
        [Path(__file__).resolve(), Path(__file__).with_name('orchestration_contract.py').resolve(),
         paths['binder'], paths['driver'], paths['collector'], paths['restore_script']])
    runner_manifest = verified_manifest(a['artifacts']['runner_artifacts'])
    verified_manifest(a['artifacts']['implementation_artifacts'])
    verify_existing_implementation(base, runner_manifest)
    approval = load_json(evidence['resource_approval'])
    require(approval.get('decision') == 'approved' and approval.get('epoch') == EPOCH,
            'actual external resource approval required')
    start, ready = load_json(evidence['start_attempt']), load_json(evidence['watchdog_ready'])
    mounted, guard = load_json(evidence['mount_verification']), load_json(evidence['remote_guard'])
    instance, volume = start['instance'], start['experiment_volume']
    require(start['identity']['Account'] == approval['account'] == ACCOUNT
            and instance['InstanceId'] == approval['instance_id']
            and instance['InstanceType'] == approval['instance_type'] == 'r6i.2xlarge', 'resource identity differs')
    require(volume['VolumeId'] == mounted['volume_id'] == approval['volume_id']
            and volume['Size'] == 64 and volume['VolumeType'] == 'gp3' and volume['Encrypted'] is True,
            'dedicated experiment volume differs')
    require(root.is_relative_to(Path(mounted['mount'])) and mounted['free_bytes'] >= approval['required_free_bytes'],
            'mount/capacity evidence differs')
    require(start['deadline_unix'] == ready['deadline'] == approval['machine_stop_unix'] == stop
            and ready['pid'] == start['watchdog_pid'], 'independent watchdog binding differs')
    require(guard['active'] is True and guard['timer'] == base['guard_timer']
            and guard['deadline_unix'] == stop and guard['instance_id'] == instance['InstanceId']
            and guard['raw'] == a['evidence']['remote_guard_raw'], 'remote absolute guard evidence differs')
    limits = a['retrieval_limits']
    require(type(limits['max_wire_bytes']) is int and 0 < limits['max_wire_bytes'] <= MAX_WIRE
            and type(limits['max_archive_bytes']) is int and 0 < limits['max_archive_bytes'] <= 68*1024**2
            and type(limits['max_sidecar_bytes']) is int and 0 < limits['max_sidecar_bytes'] <= 1024**2,
            'externally admitted retrieval bounds required')
    require(limits['max_sidecar_bytes'] >= 65536 and limits['max_wire_bytes'] >=
            limits['max_archive_bytes'] + 5*limits['max_sidecar_bytes'] + 32768,
            'retrieval wire cap cannot cover declared file caps and TAR framing')
    # Every path is explicit; collection never scans the experiment directory.
    required = {relative(root, admission_path)}
    for key in ('base_template','restore_spec','matched_inputs','queries','setup_artifacts',
                'runner_artifacts','implementation_artifacts','build_provenance','collection'):
        required.add(relative(root, paths[key]))
    required.update(relative(root, p) for p in evidence.values())
    required.update({'restore/receipt.json','restore/runtime-provenance.json','restore/failure.json',
                     'orchestration/restore.stdout','orchestration/restore.stderr',
                     'orchestration/binder.stdout','orchestration/binder.stderr',
                     'orchestration/work.stdout','orchestration/work.stderr',
                     'orchestration/binding-spec.json','orchestration/generated-base.json',
                     'orchestration/pre-collection.json','integration/native-common.json',
                     'integration/prepare-stage.json','integration/restore-bindings.json',
                     'integration/bound-inputs.json','integration/matched-complete.json'})
    closure_sources = {'build-source-artifacts.json': load_json(paths['build_provenance'])['source_manifest'],
                       'restore-plan.json': restore['plan'],
                       'restore-materializer.py': a['artifacts']['restore_script']}
    for item in closure_sources.values():
        require(item['bytes'] <= 2*1024*1024, 'closure metadata byte cap')
        checked(item)
    a['_closure_sources'] = closure_sources
    required.update('orchestration/closure/'+name for name in closure_sources)
    required.add('orchestration/closure/copies.json')
    required.update(REQUIRED_RESULTS)
    required.update(f'restore/object-{i:04d}.json' for i in range(a['expected']['restore_objects']))
    for stage in STAGES:
        required.update(f'integration/{stage}{tail}' for tail in
                        ('-execution.json','-admission.json','-exit.json','.stdout','.stderr'))
    require(len(collection['files']) <= 256 and required <= set(collection['files']),
            'collection omits required restore/resource/generated evidence')
    a['_required_success'] = sorted(required - {'restore/failure.json'})
    return a, paths, base, collection, root, output


def orchestrate(spec, *, _cohort=COHORT):
    # Validation failures have no admitted executable fallback and run no child.
    a, paths, base, collection, root, output = validate(spec, _cohort=_cohort)
    output.mkdir()
    result = dict(epoch=EPOCH, admission=spec['admission'], machine_stop_unix=a['machine_stop_unix'],
                  restore=None, binder=None, work=None, collection=None, status='incomplete')
    def child(stage, key, option, argument, deadline):
        script = checked(a['artifacts'][key])
        remaining = deadline-time.time()
        require(remaining > 0, 'fixed deadline exhausted before ' + stage)
        with (output/(stage+'.stdout')).open('xb') as out, (output/(stage+'.stderr')).open('xb') as err:
            return subprocess.run([sys.executable, str(script), option, str(argument)],
                stdout=out, stderr=err, timeout=remaining).returncode
    try:
        closure = output/'closure'
        closure.mkdir()
        copies = []
        for name, item in a['_closure_sources'].items():
            data = checked(item).read_bytes()
            require(len(data) == item['bytes'] and hashlib.sha256(data).hexdigest() == item['sha256'],
                    'closure source changed while copied')
            with (closure/name).open('xb') as stream:
                stream.write(data)
            copies.append(dict(source=item, copy=descriptor(closure/name)))
        write_new(closure/'copies.json', dict(files=copies, scope='Exact metadata/source-byte copies; no canonical/index/binary copy'))
        result['restore'] = child('restore','restore_script','--spec',paths['restore_spec'],base['deadline_unix'])
        require(result['restore'] == 0, 'restore failed; binding and matched driver skipped')
        receipt, runtime = root/'restore/receipt.json', root/'restore/runtime-provenance.json'
        binding = dict(base=a['artifacts']['base_template'], restore_receipt=descriptor(receipt),
            restore_runtime=descriptor(runtime), restore_spec=a['artifacts']['restore_spec'],
            matched_inputs=a['artifacts']['matched_inputs'], queries=a['artifacts']['queries'],
            output=str(root/'integration'))
        binding_path = output/'binding-spec.json'
        write_new(binding_path, binding)
        result['binder'] = child('binder','binder','--spec',binding_path,base['deadline_unix'])
        require(result['binder'] == 0, 'binding failed; matched driver skipped')
        bound_path = root/'integration/bound-inputs.json'
        required = [root/'integration'/n for n in ('native-common.json','prepare-stage.json','restore-bindings.json')]
        bound = descriptor(bound_path)
        verified = verified_manifest(bound, required)
        require(set(x['path'] for x in verified['files']) == set(map(str, required)), 'generated closure is not exactly three inputs')
        native = load_json(required[0])
        require(all(native[k] == v for k,v in a['expected']['cohort'].items())
                and native['config'] == a['expected']['config'], 'generated native cohort/config differs')
        for key in ('storage_chunk','balance_every','owner_slots','owner_vector_threshold','max_nodes',
                    'max_leaf_rows','max_owner_physical_rows'):
            require(native[key] == a['expected'][key], 'generated native setting differs: ' + key)
        proof = load_json(checked(binding['restore_runtime']))
        require(proof.get('status') == 'complete' and proof['rows'] == _cohort['rows']
                and proof['dimension'] == _cohort['dimension']
                and proof['expected_bytes'] == proof['canonical']['bytes'] == _cohort['rows']*_cohort['dimension']*4
                and Path(proof['canonical']['path']) == root/'restore/canonical.f32', 'restore cohort/extent differs')
        require(native.get('format_version') == 1, 'native format version differs')
        require(native['vectors'] == proof['canonical'], 'generated canonical binding differs')
        prepare = load_json(required[1])
        require(prepare['queries'] == a['artifacts']['queries'] and prepare['vectors'] == proof['canonical']
                and all(prepare[k] == base[k] for k in BUDGET_KEYS)
                and prepare['rows'] == _cohort['rows'] and prepare['dimension'] == _cohort['dimension']
                and prepare['output'] == str(root/'integration/prepared'), 'generated preparation binding differs')
        require(load_json(required[2]) == binding, 'generated binding spec differs')
        generated_base = dict(base, bound_inputs=bound)
        write_new(output/'generated-base.json', generated_base)
        # No approval document is created here; only the authorized generated field changes.
        checked(spec['admission'])
        for item in a['evidence'].values():
            checked(item)
        result['work'] = child('work','driver','--base',output/'generated-base.json',base['deadline_unix'])
        require(result['work'] == 0, 'matched driver failed')
        require((root/'integration/matched-complete.json').is_file(), 'driver omitted completed matched receipt')
        result['status'] = 'matched-complete'
    except Exception as exc:
        result.update(status='work-incomplete', error_type=type(exc).__name__, error=str(exc))
    finally:
        result['work_ended_unix'] = time.time()
        write_new(output/'pre-collection.json', result)
        try:
            checked(a['artifacts']['collection'])
            remaining = collection['deadline_unix']-time.time()
            require(remaining > 0, 'collection deadline exhausted; stop unchanged')
            with (output/'collection.stdout').open('xb') as out, (output/'collection.stderr').open('xb') as err:
                result['collection'] = subprocess.run([sys.executable, str(checked(a['artifacts']['collector'])),
                    '--spec',str(paths['collection']),'--output',str(output/'evidence.tar')],
                    stdout=out, stderr=err, timeout=remaining).returncode
            require(result['collection'] == 0, 'evidence collector failed')
            receipt = load_json(output/'evidence.tar.receipt.json')
            require(receipt['archive'] == descriptor(output/'evidence.tar'), 'collection archive binding differs')
            require(receipt['collector_sha256'] == a['artifacts']['collector']['sha256']
                    and receipt['spec_sha256'] == hashlib.sha256(json.dumps(collection,sort_keys=True).encode()).hexdigest()
                    and [r['path'] for r in receipt['files']] == collection['files']
                    and receipt['missing'] == [r['path'] for r in receipt['files'] if r.get('status') == 'missing'],
                    'collector source/spec/file-list binding differs')
            result['evidence_archive_valid'] = True
            result['collection_missing'] = receipt['missing']
            if result['status'] == 'matched-complete':
                require(not (set(receipt['missing']) & set(a['_required_success'])),
                        'successful work lacks required evidence')
        except Exception as exc:
            result['collection_error'] = repr(exc)
        result['ended_unix'] = time.time()
        result['success'] = result['status'] == 'matched-complete' and result['collection'] == 0 and 'collection_error' not in result
        files, omitted = [], []
        for name in ('pre-collection.json','collection.stdout','collection.stderr'):
            if (output/name).is_file():
                item = descriptor(output/name)
                if item['bytes'] <= a['retrieval_limits']['max_sidecar_bytes']:
                    files.append(dict(name=name, **item))
                else:
                    omitted.append(name)
        if result.get('evidence_archive_valid'):
            pair = [dict(name=name, **descriptor(output/name)) for name in ('evidence.tar','evidence.tar.receipt.json')]
            if pair[0]['bytes'] <= a['retrieval_limits']['max_archive_bytes'] and pair[1]['bytes'] <= a['retrieval_limits']['max_sidecar_bytes']:
                files.extend(pair)
            else:
                omitted.extend(['evidence.tar','evidence.tar.receipt.json'])
        if omitted:
            result['retrieval_omitted_over_cap'] = omitted
            result['success'] = False
        write_new(output/'result.json', result)
        item = descriptor(output/'result.json')
        require(item['bytes'] <= a['retrieval_limits']['max_sidecar_bytes'], 'terminal result exceeds admission')
        files.insert(0, dict(name='result.json', **item))
        # Includes ordinary USTAR file padding, end markers and one record pad.
        wire_upper = sum(512 + ((x['bytes']+511)//512)*512 for x in files) + 1024 + 10240
        require(wire_upper <= a['retrieval_limits']['max_wire_bytes'], 'retrieval wire reserve too small')
        manifest = dict(format_version=1,epoch=EPOCH,admission=spec['admission'],root=str(output),
                        machine_stop_unix=a['machine_stop_unix'],limits=a['retrieval_limits'],files=files)
        write_new(output/'retrieval-manifest.json', manifest)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spec', required=True)
    args = parser.parse_args()
    result = orchestrate(load_json(args.spec))
    print(json.dumps(dict(result=result, retrieval_manifest=descriptor(Path(load_json(args.spec)['output'])/'retrieval-manifest.json'))))
    raise SystemExit(not result['success'])
