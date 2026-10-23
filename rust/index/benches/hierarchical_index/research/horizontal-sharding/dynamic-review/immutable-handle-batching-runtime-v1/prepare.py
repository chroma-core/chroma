"""Overlay one frozen regular file while preserving every source archive link."""
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parent
REVIEW = ROOT.parent / 'immutable-handle-batching-v1'
BASE = ROOT.parent / 'full-record-100k-owner32-runtime-v1/durable-build-binding-v6/runtime-package.tar.gz'
EXPECTED_BASE = '2b41d2e427505b2cca50e6c3fdf6c5de76cc8aa8781bcf3a110a552c82022421'


def descriptor(data):
    return {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}


def main():
    assert descriptor(BASE.read_bytes())['sha256'] == EXPECTED_BASE
    with tarfile.open(BASE) as archive:
        members = archive.getmembers()
        content = {member.name: archive.extractfile(member).read() for member in members if member.isfile()}
    old_manifest = content['source-manifest.json']
    manifest = json.loads(old_manifest)
    delta = json.loads((REVIEW / 'source-manifest.json').read_text())
    change = (REVIEW / 'reference_owner_adapter.rs').read_bytes()
    assert descriptor(change) == {key: delta[key] for key in ['bytes', 'sha256']}
    manifest['files'][delta['path']] = descriptor(change)
    new_manifest = (json.dumps(manifest, indent=2) + '\n').encode()
    source_buffer = io.BytesIO()
    changed = []
    with tarfile.open(fileobj=io.BytesIO(content['source.tar.gz'])) as original, tarfile.open(fileobj=source_buffer, mode='w:gz') as target:
        for member in original.getmembers():
            body = original.extractfile(member).read() if member.isfile() else None
            if member.name == 'source/' + delta['path']:
                body = change
                member = copy.copy(member)
                member.size = len(body)
                changed.append(member.name)
            target.addfile(member, None if body is None else io.BytesIO(body))
    assert changed == ['source/' + delta['path']]
    new_source = source_buffer.getvalue()
    old_source_descriptor = descriptor(content['source.tar.gz'])
    new_source_descriptor = descriptor(new_source)
    replacements = {old_source_descriptor['sha256']: new_source_descriptor['sha256'],
                    descriptor(old_manifest)['sha256']: descriptor(new_manifest)['sha256']}

    def update(value):
        if isinstance(value, dict):
            if value.get('sha256') == old_source_descriptor['sha256']:
                return dict(value, **new_source_descriptor)
            return {key: update(item) for key, item in value.items()}
        if isinstance(value, list):
            return [update(item) for item in value]
        if isinstance(value, str):
            for old, new in replacements.items():
                value = value.replace(old, new)
        return value

    for name, body in list(content.items()):
        if name.endswith('.json') and name != 'source-manifest.json':
            content[name] = (json.dumps(update(json.loads(body)), indent=2) + '\n').encode()
    content['operator/extract_and_test.py'] = content['operator/extract_and_test.py'].decode().replace(
        descriptor(old_manifest)['sha256'], descriptor(new_manifest)['sha256']).replace(
        old_source_descriptor['sha256'], new_source_descriptor['sha256']).encode()
    content['source-manifest.json'] = new_manifest
    content['source.tar.gz'] = new_source
    (ROOT / 'source.tar.gz').write_bytes(new_source)
    (ROOT / 'source-manifest.json').write_bytes(new_manifest)
    package_path = ROOT / 'runtime-package.tar.gz'
    with tarfile.open(package_path, 'w:gz') as target:
        for member in members:
            if not member.isfile():
                target.addfile(member)
                continue
            body = content[member.name]
            item = copy.copy(member)
            item.size = len(body)
            target.addfile(item, io.BytesIO(body))

    # Exercise the exact packaged extractor, not a byte-only approximation.
    with tempfile.TemporaryDirectory(prefix='spann-handle-package-') as temporary:
        folder = Path(temporary)
        helper = folder / 'extract.py'
        helper.write_bytes(content['operator/extract_and_test.py'])
        module_spec = importlib.util.spec_from_file_location('saved_extractor', helper)
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        destination = folder / 'package'
        destination.mkdir()
        module.extract(package_path, destination)
        module.extract(destination / 'source.tar.gz', destination, allow_symlink=True)
        source = destination / 'source'
        for relative, expected in manifest['files'].items():
            path = source / relative
            assert path.is_file() and not path.is_symlink() and descriptor(path.read_bytes()) == expected
        for relative, target in manifest['symlinks'].items():
            path = source / relative
            assert path.is_symlink() and str(path.readlink()) == target and path.resolve().is_relative_to(source.resolve())
        actual = {str(path.relative_to(source)) for path in source.rglob('*') if path.is_file() and not path.is_symlink()}
        assert actual == set(manifest['files'])
    handoff = {'status': 'local package ready for root review; no cloud admission',
               'base_package': {'path': str(BASE), **descriptor(BASE.read_bytes())},
               'source_delta': delta, 'changed_source_members': changed,
               'source_archive': descriptor(new_source), 'source_manifest': descriptor(new_manifest),
               'package': descriptor(package_path.read_bytes()),
               'source_validation': {'regular_files': len(manifest['files']), 'symlinks': manifest['symlinks'],
                                     'exact_packaged_extract_function': 'passed', 'extra_missing_files': False},
               'spec': descriptor(content['SPEC-TEMPLATE.json']),
               'scientific_policy': 'Same100Kowner1/32workers/inputorder/settings/bytecache limits; sole source change batches immutable handle lookup. Independent stochastic shape/work limits elapsed comparisons.',
               'build': 'Same-j32release/opt3/debug0, persistent target, fresh durable source/control/TMPDIR.',
               'limits': json.loads(content['PLAN-TEMPLATE.json'])['limits'],
               'remaining_admission': 'Root packaging review, then fresh identity/rate/guards/capacity and mandatory Linux tests/compiler/build/input/host gates; no same-window retry.'}
    (ROOT / 'HANDOFF.json').write_text(json.dumps(handoff, indent=2) + '\n')
    print(json.dumps(handoff['package']))


if __name__ == '__main__':
    main()
