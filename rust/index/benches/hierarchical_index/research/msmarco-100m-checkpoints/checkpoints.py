"""Snapshot immutable blocks and make metadata usable for 1M resume runs."""
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid


def normalize(path, batch):
    meta_path = path / 'checkpoint.json'
    meta = json.loads(meta_path.read_text())
    total = meta['total_vectors']
    if total % batch:
        raise ValueError(f'{total} is not divisible by {batch}')
    original = path / 'build-checkpoint-metadata.json'
    if not original.exists():
        original.write_text(json.dumps(meta, indent=2) + '\n')
    meta['checkpoint_idx'] = total // batch - 1
    tmp = path / 'checkpoint.json.tmp'
    tmp.write_text(json.dumps(meta, indent=2) + '\n')
    os.replace(tmp, meta_path)
    return total


def snapshot(source, target):
    if target.exists():
        raise FileExistsError(target)
    target.mkdir(parents=True)
    for kind in ('block', 'sparse_index'):
        dest = target / 'data' / kind
        dest.mkdir(parents=True)
        for p in (source / 'data' / kind).iterdir():
            uuid.UUID(p.name)
            if not p.is_file():
                raise ValueError(p)
            os.link(p, dest / p.name)
    (target / 'checkpoint.json').write_bytes((source / 'checkpoint.json').read_bytes())
    total = normalize(target, 1_000_000)
    (target / 'SNAPSHOT_COMPLETE').write_text(f'{total}\n')


def manifest(path):
    result = {}
    for p in sorted((path / 'data').rglob('*')):
        if p.is_file():
            s = p.stat()
            result[str(p.relative_to(path))] = [s.st_size, s.st_ino, s.st_mtime_ns]
    result['checkpoint_sha256'] = hashlib.sha256((path / 'checkpoint.json').read_bytes()).hexdigest()
    return result


if __name__ == '__main__':
    command = sys.argv[1]
    if command == 'snapshot':
        snapshot(Path(sys.argv[2]), Path(sys.argv[3]))
    elif command == 'normalize':
        normalize(Path(sys.argv[2]), int(sys.argv[3]))
    elif command == 'manifest':
        print(json.dumps(manifest(Path(sys.argv[2])), sort_keys=True))
    else:
        raise ValueError(command)
