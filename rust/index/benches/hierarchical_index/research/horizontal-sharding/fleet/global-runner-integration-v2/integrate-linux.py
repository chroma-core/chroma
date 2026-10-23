"""Connect three admitted stages on the small fixture; no cloud lifecycle actions."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import subprocess
import sys
import time


def verify_pinned(binding):
    def check(item):
        path = Path(item['path']).resolve()
        if path.stat().st_size != item['bytes'] or hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']:
            raise ValueError('pinned code changed: '+str(path))
        return path
    manifest = json.loads(check(binding).read_text())
    return {check(item):item['sha256'] for item in manifest['files']}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base', required=True)
    args = parser.parse_args()
    if platform.system() != 'Linux':
        raise RuntimeError('Linux integration only')
    base = json.loads(Path(args.base).read_text())
    runner = Path(base.pop('runner_directory'))
    root = Path(base.pop('integration_output'))
    setup_pins = verify_pinned(base['setup_artifacts'])
    runner_pins = verify_pinned(base['runner_artifacts'])
    for path in (Path(__file__).resolve(), Path(__file__).with_name('prepare-fixture.py').resolve()):
        if path not in setup_pins:
            raise ValueError('setup code missing from admitted manifest')
    actual_modules = {p.resolve() for p in runner.glob('*.py') if not p.name.startswith('test_')}
    if not actual_modules.issubset(runner_pins):
        raise ValueError('runner code missing from admitted manifest')
    sys.path.insert(0, str(runner))
    from artifact_bindings import descriptor, verify_implementation, verify_manifest, BUDGET_KEYS
    verify_implementation(base)
    fixture_module = importlib.util.spec_from_file_location('fixture', Path(__file__).with_name('prepare-fixture.py'))
    fixture = importlib.util.module_from_spec(fixture_module)
    fixture_module.loader.exec_module(fixture)
    fixture.prepare(root, base)
    runner_files = {p.name: runner_pins[p] for p in actual_modules}

    def stage(name, spec):
        if time.time()+60 >= base['deadline_unix']:
            raise RuntimeError('fixed window exhausted before stage')
        path = root/(name+'-execution.json')
        fixture.write(path, spec)
        admission = {key:base[key] for key in BUDGET_KEYS}
        admission.update(stage=name, runner_files=runner_files, stage_spec=str(path),
                         stage_spec_sha256=descriptor(path)['sha256'],
                         implementation_checkout=base['implementation_checkout'],
                         implementation_artifacts=base['implementation_artifacts'],
                         supervision_output=str(root/(name+'-supervision')))
        admission_path = root/(name+'-admission.json')
        fixture.write(admission_path, admission)
        with (root/(name+'.stdout')).open('x') as out, (root/(name+'.stderr')).open('x') as err:
            completed = subprocess.run([sys.executable, str(runner/'launch_stage.py'), '--admission', str(admission_path)],
                                       stdout=out, stderr=err)
        fixture.write(root/(name+'-exit.json'), dict(returncode=completed.returncode, ended_unix=time.time()))
        if completed.returncode:
            raise RuntimeError('stage failed: '+name)

    stage('prepare-global', json.loads((root/'prepare-stage.json').read_text()))
    prepared = descriptor(root/'prepared/prepared-artifacts.json')
    verify_manifest(prepared)
    common = json.loads((root/'native-common.json').read_text())
    shared = dict(base, native_common=common, prepared_artifacts=prepared,
                  prepared_cohort=descriptor(root/'prepared/prepared.json'))
    stage('construct-global', dict(shared, output=str(root/'build')))
    outputs = descriptor(root/'build/paired-build-artifacts.json')
    verify_manifest(outputs)
    stage('diagnose-global', dict(shared, output=str(root/'diagnostic'), build_output=str(root/'build'),
                                 paired_build_artifacts=outputs, queries=str(root/'queries.jsonl'),
                                 reference=str(root/'prepared/reference.jsonl')))
    assertions = {}
    for arm in ('reference', 'authoritative'):
        native = json.loads((root/'build'/arm/'manifest.json').read_text())
        report = json.loads((root/'diagnostic'/arm/'stdout').read_text())
        assert native['integrity_audit']['unique_live_rows'] == 128
        assert native['integrity_audit']['duplicate_current_entries'] == 0
        assert [c['rows'] for c in native['balance_cuts']] == [32,64,96,128]
        assert len(report['queries']) == 16
        assert {q['query_id'] for q in report['queries']} == {str(i) for i in range(16)}
        assert all(q['expected'] == q['covered'] == 100 for q in report['queries'])
        saved = root/'build'/arm/'store/manifest.json'
        assert report['owners'][0]['snapshot_sha256'] == descriptor(saved)['sha256']
        assert report['owners'][0]['five_roots'] == native['block_ids']
        assert report['owners'][0]['map_sha256'] == native['id_mapping']['sha256']
        assertions[arm] = {'live_rows':128, 'complete_queries':16, 'covered_per_query':100,
                           'balance_cuts':[32,64,96,128]}
        if arm == 'authoritative':
            moves = native['accounting']['event_counts'].get('move',0)
            owners = len(set(native['branch_owners'].values()))
            assert moves > 0 and owners > 1
            assertions[arm].update(moves=moves, owners=owners)
    verify_manifest(outputs)
    verify_pinned(base['setup_artifacts'])
    verify_pinned(base['runner_artifacts'])
    fixture.write(root/'integration-complete.json', dict(prepared=prepared, paired_build=outputs,
                  assertions=assertions, rows=128, dimension=768, queries=16, metric='ip', performance_claim=False))


if __name__ == '__main__':
    main()
