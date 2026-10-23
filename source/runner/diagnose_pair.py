"""Run common ground-truth coverage diagnostics against admitted global builds."""
import json
from pathlib import Path

from artifact_bindings import descriptor, verify_manifest, verify_cohort, verify_implementation
from construct_pair import validate_result
from supervise import run


def create_spec(snapshot, queries, reference, output, expected=100):
    saved = json.loads(Path(snapshot).read_text())
    selected = [json.loads(line) for line in Path(queries).read_text().splitlines() if line.strip()]
    if not selected or len({q['query_id'] for q in selected}) != len(selected):
        raise ValueError('invalid query identities')
    result = dict(format_version=1, source_id=saved['source_id'], dimension=saved['dimension'],
                  metric=saved['metric'], global_rows=saved['vector_count'], max_map_rows=saved['vector_count'],
                  ground_truth=descriptor(reference), query_vectors=descriptor(queries),
                  selected_queries=[dict(query_id=q['query_id'], expected=expected) for q in selected],
                  owners=[dict(owner_id=saved['shard_id'], generation=saved['generation'], snapshot=descriptor(snapshot))])
    with Path(output).open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    return result


def diagnose(spec):
    verify_cohort(spec)
    build = Path(spec['build_output'])
    required = [build/arm/name for arm in ('reference', 'authoritative')
                for name in ('manifest.json', 'store/manifest.json')]
    required.append(build/'construction-stage.json')
    verify_manifest(spec['paired_build_artifacts'], required)
    construction = json.loads((build/'construction-stage.json').read_text())
    verify_implementation(construction)
    verify_cohort(construction)
    for key in ('build_provenance', 'implementation_artifacts', 'binary_sha256', 'prepared_cohort'):
        if construction[key] != spec[key]:
            raise ValueError('diagnostic provenance differs from construction: ' + key)
    verify_manifest(spec['prepared_artifacts'], [spec['queries'], spec['reference']])
    output = Path(spec['output'])
    output.mkdir(parents=True, exist_ok=False)
    for arm in ('reference', 'authoritative'):
        native_path = build/(arm+'-spec.json')
        native = json.loads(native_path.read_text())
        if {k:v for k,v in native.items() if k not in ('treatment','output_dir')} != spec['native_common']:
            raise ValueError('build differs from admitted common settings')
        snapshot = build/arm/'store/manifest.json'
        validate_result(native, native_path, json.loads((build/arm/'manifest.json').read_text()),
                        json.loads((build/(arm+'-process/stdout')).read_text()), json.loads(snapshot.read_text()))
        diagnostic = output/(arm+'-spec.json')
        create_spec(snapshot, spec['queries'], spec['reference'], diagnostic)
        run([spec['binary'], 'diagnose-ground-truth', '--spec', str(diagnostic)], spec['cwd'], output/arm,
            spec['deadline_unix'], spec['min_free_bytes'], spec['max_process_rss_bytes'], spec['max_log_bytes']//2)
        report = json.loads((output/arm/'stdout').read_text())
        if report.get('policy') != 'saved-gt-quality-v1':
            raise ValueError('unexpected diagnostic policy')
    verify_manifest(spec['paired_build_artifacts'], required)
    verify_manifest(spec['prepared_artifacts'], [spec['queries'], spec['reference']])
