"""Bind prepared inputs to externally retained hashes before accepting a stage."""
import json
from pathlib import Path

from project_rounds import digest


BUDGET_KEYS = (
    'deadline_unix', 'threads', 'min_free_bytes',
    'max_process_rss_bytes', 'max_log_bytes', 'guard_timer',
)


def matching_budget(admission, spec):
    for key in BUDGET_KEYS:
        if admission[key] != spec[key]:
            raise ValueError('stage budget differs from admission: ' + key)
    for key in ('threads', 'min_free_bytes', 'max_process_rss_bytes', 'max_log_bytes'):
        if not isinstance(spec[key], int) or isinstance(spec[key], bool) or spec[key] <= 0:
            raise ValueError('positive integer resource limit required: ' + key)


def descriptor(path):
    path = Path(path).resolve(strict=True)
    return {'path': str(path), 'bytes': path.stat().st_size, 'sha256': digest(path)}


def verify_descriptor(item):
    path = Path(item['path'])
    if path.stat().st_size != item['bytes'] or digest(path) != item['sha256']:
        raise ValueError('bound artifact changed: ' + str(path))
    return path


def freeze_prepared(output, vectors, queries):
    """Create a receipt; its hash must be separately admitted by later stages."""
    root = Path(output).resolve()
    paths = {Path(vectors).resolve(), Path(queries).resolve()}
    paths.update(p.resolve() for p in root.rglob('*') if p.is_file())
    manifest = {'format_version': 1, 'files': [descriptor(p) for p in sorted(paths)]}
    target = root / 'prepared-artifacts.json'
    with target.open('x') as stream:
        json.dump(manifest, stream, indent=2)
        stream.write('\n')
    return descriptor(target)


def verify_manifest(binding, required_paths=()):
    path = verify_descriptor(binding)
    manifest = json.loads(path.read_text())
    if manifest['format_version'] != 1 or not manifest['files']:
        raise ValueError('unsupported or empty artifact manifest')
    seen = set()
    for item in manifest['files']:
        current = verify_descriptor(item).resolve()
        if current in seen:
            raise ValueError('duplicate artifact path')
        seen.add(current)
    if not {Path(p).resolve() for p in required_paths}.issubset(seen):
        raise ValueError('stage input missing from admitted artifact manifest')
    return manifest


def verify_implementation(spec):
    """Require imported project code and retained build provenance in the binding."""
    checkout = Path(spec['implementation_checkout']).resolve(strict=True)
    package = checkout / 'tools' / 'hierarchical_sharding'
    required = list(package.rglob('*.py'))
    if not required or not (package / 'spatial_prepare.py').is_file() or not (package / 'exact_blocked.py').is_file():
        raise ValueError('required preparation implementation missing')
    required.extend(p for p in (checkout/'tools/__init__.py', package/'__init__.py') if p.exists())
    provenance_path = verify_descriptor(spec['build_provenance'])
    provenance = json.loads(provenance_path.read_text())
    for key in ('source_manifest', 'compiler', 'binary'):
        required.append(verify_descriptor(provenance[key]))
    verify_manifest(provenance['source_manifest'])
    required.append(provenance_path)
    if Path(provenance['binary']['path']).resolve() != Path(spec['binary']).resolve():
        raise ValueError('stage binary differs from build provenance')
    if provenance['binary']['sha256'] != spec['binary_sha256']:
        raise ValueError('stage binary hash differs from build provenance')
    if not provenance.get('compiler_version') or not provenance.get('build_command'):
        raise ValueError('incomplete compiler provenance')
    verify_manifest(spec['implementation_artifacts'], required)


def freeze_build(output, projection_path):
    root = Path(output)
    projection = json.loads(Path(projection_path).read_text())
    last = {job['owner']: job for job in projection['jobs']}
    paths = [root/'build-results.json', root/'stage-result.json']
    for owner, job in last.items():
        snapshot = root/'indexes'/str(owner)/f"manifest-{job['generation']}.json"
        mapping = Path(json.loads(snapshot.read_text())['id_mapping']['path'])
        paths.extend([snapshot, mapping if mapping.is_absolute() else snapshot.parent/mapping])
    with (root/'build-artifacts.json').open('x') as stream:
        json.dump({'format_version': 1, 'files': [descriptor(p) for p in paths]}, stream, indent=2)
        stream.write('\n')


def verify_build(spec):
    root = Path(spec['build_output'])
    projection = json.loads(Path(spec['projection']).read_text())
    last = {job['owner']: (i, job) for i, job in enumerate(projection['jobs'])}
    paths = [root/'build-results.json', root/'stage-result.json']
    paths += [root/'indexes'/str(owner)/f"manifest-{job['generation']}.json"
              for owner, (_, job) in last.items()]
    verify_manifest(spec['build_artifacts'], paths)
    if json.loads((root/'stage-result.json').read_text())['status'] != 'completed':
        raise ValueError('build did not complete')
    results = json.loads((root/'build-results.json').read_text())
    if len(results) != len(projection['jobs']) or any(r['returncode'] != 0 or r['stop_reason'] for r in results):
        raise ValueError('incomplete build job receipts')
    for owner, (index, job) in last.items():
        snapshot = root/'indexes'/str(owner)/f"manifest-{job['generation']}.json"
        saved = json.loads(snapshot.read_text())
        result = results[index]
        if digest(snapshot) != result['saved_manifest_sha256']:
            raise ValueError('snapshot differs from completed build')
        if any(saved[key] != result[key] for key in ('block_ids', 'root_ref')):
            raise ValueError('saved root identity changed')
        if saved['construction'] != spec['construction'] or saved['routing_boundary_hash'] != spec['boundary_hash']:
            raise ValueError('saved build settings differ from admission')


def verify_cohort(spec):
    prepared_path = verify_descriptor(spec['prepared_cohort'])
    cohort = json.loads(prepared_path.read_text())
    required = [prepared_path]
    for key in ('vectors', 'queries', 'reference'):
        required.append(verify_descriptor(cohort[key]))
    verify_manifest(spec['prepared_artifacts'], required)
    native = spec['native_common']
    if any(native[key] != cohort[key] for key in ('rows', 'dimension', 'metric', 'vectors')):
        raise ValueError('native build differs from prepared cohort')
    for key in ('queries', 'reference'):
        if key in spec and descriptor(spec[key]) != cohort[key]:
            raise ValueError('diagnostic input differs from prepared cohort')
    return cohort
