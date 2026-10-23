"""Run explicitly matched global builds with independently retained outputs."""
import json
import os
from pathlib import Path

from artifact_bindings import descriptor, verify_descriptor, verify_manifest, verify_cohort
from supervise import run


def validate_result(spec, spec_path, manifest, stdout, reader):
    if manifest != stdout or manifest['spec'] != spec:
        raise ValueError('native output/spec mismatch')
    if manifest['consumed_spec_sha256'] != descriptor(spec_path)['sha256']:
        raise ValueError('consumed native specification differs')
    for key in ('rows', 'dimension', 'metric', 'source_id', 'treatment'):
        if manifest[key] != spec[key]:
            raise ValueError('native cohort mismatch: ' + key)
    if manifest['input'] != spec['vectors']:
        raise ValueError('native input differs')
    expected_cuts = list(range(spec['balance_every'], spec['rows'], spec['balance_every'])) + [spec['rows']]
    if [cut['rows'] for cut in manifest['balance_cuts']] != expected_cuts:
        raise ValueError('native balance schedule differs')
    audit = manifest['integrity_audit']
    if audit['unique_live_rows'] != spec['rows'] or audit['duplicate_current_entries'] != 0:
        raise ValueError('incomplete unique saved membership')
    root_identity = {key: manifest['root'][key] for key in ('id', 'kind')}
    if reader['block_ids'] != manifest['block_ids'] or reader['root_ref'] != root_identity:
        raise ValueError('saved reader roots differ')
    if reader['construction'] != {'version': 2, 'protocol': 'explicit-fp-global-v1', 'config': spec['config']}:
        raise ValueError('saved reader construction differs')


def construct(spec, stage_spec_path):
    verify_cohort(spec)
    root = Path(spec['output'])
    root.mkdir(parents=True, exist_ok=False)
    common = spec['native_common']
    if 'treatment' in common or 'output_dir' in common:
        raise ValueError('treatment and output belong to paired runner')
    verify_manifest(spec['prepared_artifacts'], [common['vectors']['path']])
    verify_descriptor({'path': spec['binary'], 'bytes': spec['binary_bytes'], 'sha256': spec['binary_sha256']})
    env = dict(os.environ, RAYON_NUM_THREADS=str(spec['threads']), TOKIO_WORKER_THREADS=str(spec['threads']))
    stage_copy = root/'construction-stage.json'
    stage_copy.write_bytes(Path(stage_spec_path).read_bytes())
    if json.loads(stage_copy.read_text()) != spec:
        raise ValueError('construction stage spec changed')
    artifacts = [descriptor(stage_copy), spec['build_provenance'], spec['implementation_artifacts'],
                 spec['prepared_cohort']]
    provenance = json.loads(verify_descriptor(spec['build_provenance']).read_text())
    artifacts.extend(provenance[key] for key in ('source_manifest', 'compiler', 'binary'))
    for treatment in ('reference', 'authoritative'):
        native = dict(common, treatment=treatment, output_dir=str(root/treatment))
        native_path = root/(treatment+'-spec.json')
        native_path.write_text(json.dumps(native, indent=2)+'\n')
        receipt = root/(treatment+'-process')
        run([spec['binary'], 'construct-global', '--spec', str(native_path)], spec['cwd'], receipt,
            spec['deadline_unix'], spec['min_free_bytes'], spec['max_process_rss_bytes'],
            spec['max_log_bytes']//2, env)
        manifest_path = root/treatment/'manifest.json'
        reader_path = root/treatment/'store/manifest.json'
        validate_result(native, native_path, json.loads(manifest_path.read_text()),
                        json.loads((receipt/'stdout').read_text()), json.loads(reader_path.read_text()))
        reader = json.loads(reader_path.read_text())
        mapping_path = reader_path.parent / reader['id_mapping']['path']
        mapping = descriptor(mapping_path)
        if mapping['sha256'] != reader['id_mapping']['sha256'] or mapping['bytes'] != common['rows'] * 8:
            raise ValueError('saved identity map differs')
        artifacts.append(mapping)
        artifacts.extend([descriptor(p) for p in (native_path, manifest_path, reader_path,
                                                  receipt/'result.json', receipt/'stdout', receipt/'stderr')])
    verify_manifest(spec['prepared_artifacts'], [common['vectors']['path']])
    with (root/'paired-build-artifacts.json').open('x') as stream:
        json.dump({'format_version': 1, 'files': artifacts}, stream, indent=2)
        stream.write('\n')
