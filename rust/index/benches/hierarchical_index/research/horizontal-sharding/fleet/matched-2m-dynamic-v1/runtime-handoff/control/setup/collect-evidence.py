"""Create a bounded metadata-only archive on the experiment machine."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import tarfile
import time

MAX_FILE = 8 * 1024 * 1024
MAX_TOTAL = 64 * 1024 * 1024
# Paths are explicit admission inputs, never directory walks or inferred suffixes.
METADATA_EXCEPTIONS = {
    'integration/build/reference/store/manifest.json',
    'integration/build/authoritative/store/manifest.json',
    'integration/prepared/reference.jsonl',
}
DENIED_PARTS = {'store', 'vectors', 'objects', 'canonical', 'target', '.git'}
ALLOWED_SUFFIXES = {'.json', '.jsonl', '.py', '.md', '.stdout', '.stderr', '.log'}


def collect(spec, output):
    root = Path(spec['root']).resolve(strict=True)
    deadline = spec['deadline_unix']
    names = spec['files']
    if len(names) > 256 or len(set(names)) != len(names):
        raise ValueError('invalid evidence file count or duplicate')
    output = Path(output)
    records = []
    total = 0
    with output.open('xb') as raw, tarfile.open(fileobj=raw, mode='w') as archive:
        for name in names:
            if time.time() >= deadline:
                raise TimeoutError('collection deadline reached')
            relative = Path(name)
            if relative.is_absolute() or '..' in relative.parts or (any(p in DENIED_PARTS for p in relative.parts) and str(relative) not in METADATA_EXCEPTIONS):
                raise ValueError('forbidden evidence path')
            # Only the frozen small query file may contain vector rows.
            if (relative.suffix not in ALLOWED_SUFFIXES and relative.name not in {'stdout', 'stderr'}) or (relative.suffix == '.jsonl' and relative.name != 'matched-queries16.jsonl' and str(relative) not in METADATA_EXCEPTIONS):
                raise ValueError('unapproved evidence type')
            path = root/relative
            if not path.exists():
                records.append({'path': name, 'status': 'missing'})
                continue
            if path.is_symlink() or not path.resolve().is_relative_to(root) or not path.is_file():
                raise ValueError('evidence must be a regular contained file')
            size = path.stat().st_size
            if size > MAX_FILE or total + size > MAX_TOTAL:
                raise ValueError('evidence byte budget exceeded')
            # Read once with a hard bound so growing logs cannot exceed admission.
            with path.open('rb') as stream:
                data = stream.read(MAX_FILE + 1)
            if len(data) > MAX_FILE or total + len(data) > MAX_TOTAL:
                raise ValueError('evidence grew beyond byte budget')
            entry = tarfile.TarInfo(str(relative))
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
            total += len(data)
            records.append({'path': name, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
        receipt = json.dumps({'files': records, 'payload_bytes': total, 'payload_limit_bytes': MAX_TOTAL, 'ended_unix': time.time()}, indent=2).encode()
        entry = tarfile.TarInfo('collection-receipt.json')
        entry.size = len(receipt)
        archive.addfile(entry, io.BytesIO(receipt))
    receipt_path = output.with_suffix(output.suffix + '.receipt.json')
    with receipt_path.open('x') as stream:
        json.dump({'archive': {'path': str(output), 'bytes': output.stat().st_size,
                   'sha256': hashlib.sha256(output.read_bytes()).hexdigest()},
                   'collector_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   'spec_sha256': hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest(),
                   'spec_hash_encoding': 'JSON sorted keys, default separators, UTF-8',
                   'missing': [r['path'] for r in records if r.get('status') == 'missing'],
                   'files': records}, stream, indent=2)
    return records


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spec', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    collect(json.loads(Path(args.spec).read_text()), args.output)
