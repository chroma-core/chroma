"""Small checked-metadata helpers shared by the new orchestration and retrieval."""
import hashlib
import json
from pathlib import Path

EPOCH = 'matched-2m-dynamic-v1'
ACCOUNT = '079819354890'
BUDGET_KEYS = ('deadline_unix', 'threads', 'min_free_bytes', 'max_process_rss_bytes', 'max_log_bytes', 'guard_timer')
MAX_METADATA = 2 * 1024 * 1024
MAX_WIRE = 72 * 1024 * 1024
SIDECARS = {'result.json', 'pre-collection.json', 'collection.stdout', 'collection.stderr', 'evidence.tar.receipt.json'}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def load_json(path):
    with Path(path).open('rb') as stream:
        data = stream.read(MAX_METADATA + 1)
    require(len(data) <= MAX_METADATA, 'metadata byte cap')
    def unique(pairs):
        out = {}
        for key, value in pairs:
            require(key not in out, 'duplicate JSON field')
            out[key] = value
        return out
    return json.loads(data, object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def descriptor(path):
    path = Path(path).resolve(strict=True)
    require(path.is_file(), 'artifact is not a file')
    h, size = hashlib.sha256(), 0
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
            size += len(block)
    return dict(path=str(path), bytes=size, sha256=h.hexdigest())


def checked(item):
    require(type(item) is dict and set(item) == {'path', 'bytes', 'sha256'}, 'artifact descriptor shape')
    require(type(item['bytes']) is int and item['bytes'] >= 0, 'artifact byte count')
    path = Path(item['path'])
    require(path.is_absolute() and not path.is_symlink(), 'artifact must be an absolute regular path')
    require(descriptor(path) == item, 'changed admitted artifact: ' + str(path))
    return path


def write_new(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def relative(root, path):
    p = Path(path)
    require(p.is_absolute() and p.is_relative_to(root), 'evidence path outside admitted experiment')
    value = str(p.relative_to(root))
    require('..' not in Path(value).parts, 'unsafe evidence path')
    return value


def verified_manifest(binding, required=()):
    value = load_json(checked(binding))
    require(value.get('format_version') == 1 and type(value.get('files')) is list and value['files'], 'artifact manifest shape')
    paths = [str(checked(item)) for item in value['files']]
    require(len(paths) == len(set(paths)) and set(map(str, required)) <= set(paths), 'artifact closure differs')
    return value
