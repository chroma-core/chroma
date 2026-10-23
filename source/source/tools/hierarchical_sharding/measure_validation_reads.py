"""Diagnostic-only accounting for one existing deployment validation call."""
import argparse
import builtins
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import stat
import sys
import tempfile
import time
from unittest.mock import patch

MAX_FILES = 256
MAX_EVENTS = 8192
MAX_BYTES = 64 * 1024 * 1024
MAX_REPORT = 1024 * 1024


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory(root):
    rows = {}
    total = 0
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise ValueError('fixture contains a symbolic link')
        if path.is_dir():
            continue
        if not path.is_file():
            raise ValueError('fixture contains a nonregular object')
        size = path.stat().st_size
        total += size
        if len(rows) >= MAX_FILES or total > MAX_BYTES:
            raise ValueError('fixture inventory exceeds diagnostic budget')
        rows[str(path.relative_to(root))] = dict(bytes=size, sha256=digest(path))
    return rows


class Reads:
    """Count returned binary bytes and separately re-encoded text bytes.

    This wraps builtins.open and io.open only. mmap, os.open/os.read, import
    machinery, native/provider I/O and operating-system buffering are excluded.
    """
    def __init__(self):
        self.rows = {}
        self.events = 0
        self.returned_bytes = 0
        self.excluded_temporaries = []
        self.started = time.monotonic()

    def event(self, path, kind, value=0):
        self.events += 1
        if self.events > MAX_EVENTS or time.monotonic() - self.started > 5:
            raise ValueError('diagnostic work/event budget exceeded')
        frame = sys._getframe(2)
        phase = '<other>'
        while frame:
            module = frame.f_globals.get('__name__', '')
            if module.startswith('tools.hierarchical_sharding.') and module != __name__:
                phase = module.rsplit('.', 1)[-1] + '.' + frame.f_code.co_name
                break
            frame = frame.f_back
        key = (path, phase)
        if key not in self.rows:
            if len(self.rows) >= 512 or len({p for p, _ in self.rows} | {path}) > MAX_FILES:
                raise ValueError('diagnostic path/phase budget exceeded')
            self.rows[key] = dict(opens=0, binary_returned_bytes=0,
                                  text_utf8_returned_bytes=0, seeks=0, reads=0)
        row = self.rows[key]
        if kind == 'open':
            row['opens'] += 1
        elif kind == 'seek':
            row['seeks'] += 1
        else:
            row['reads'] += 1
            row[kind] += value
            self.returned_bytes += value
            if self.returned_bytes > MAX_BYTES:
                raise ValueError('diagnostic returned-byte budget exceeded')

    def opened(self, original, file, *args, **kwargs):
        if not isinstance(file, (str, bytes, os.PathLike)):
            raise ValueError('descriptor-based open is outside diagnostic surface')
        name = os.path.abspath(os.fsdecode(file))
        stream = original(file, *args, **kwargs)
        frame = sys._getframe(1)
        while frame and frame.f_code is not tempfile.TemporaryFile.__code__:
            frame = frame.f_back
        if frame is not None:
            # The validator's existing mmap bitmap uses an unlinked temporary
            # file. The stdlib needs its original buffer/raw attributes while
            # constructing that handle. No named fixture file qualifies.
            mode = args[0] if args else kwargs.get('mode', 'r')
            info = os.fstat(stream.fileno())
            if (name != os.path.abspath(tempfile.gettempdir()) or mode != 'w+b'
                    or info.st_nlink != 0 or not stat.S_ISREG(info.st_mode)
                    or len(self.excluded_temporaries) >= 1):
                stream.close()
                raise ValueError('unexpected temporary backing handle')
            self.excluded_temporaries.append(dict(mode=mode,
                reason='stdlib TemporaryFile unlinked bitmap backing; mmap/temp I/O excluded'))
            return stream
        try:
            self.event(name, 'open')
        except BaseException:
            stream.close()
            raise
        owner = self

        class Stream:
            def __enter__(self):
                stream.__enter__()
                return self

            def __exit__(self, *values):
                return stream.__exit__(*values)

            def __getattr__(self, key):
                if key in ('read1', 'readinto1', 'peek', 'readall', 'readlines', 'buffer', 'raw', 'detach'):
                    raise ValueError('unsupported diagnostic read surface: ' + key)
                return getattr(stream, key)

            def returned(self, value):
                if isinstance(value, str):
                    owner.event(name, 'text_utf8_returned_bytes', len(value.encode('utf-8')))
                else:
                    owner.event(name, 'binary_returned_bytes', len(value))
                return value

            def read(self, *values):
                return self.returned(stream.read(*values))

            def readline(self, *values):
                return self.returned(stream.readline(*values))

            def readinto(self, value):
                count = stream.readinto(value)
                owner.event(name, 'binary_returned_bytes', count or 0)
                return count

            def seek(self, *values):
                result = stream.seek(*values)
                owner.event(name, 'seek')
                return result

            def __iter__(self):
                return self

            def __next__(self):
                value = self.readline()
                if not value:
                    raise StopIteration
                return value

        return Stream()

    def run(self, record):
        # Import before the measured region; lazy imports remain explicitly
        # outside any claim of complete interpreter/native I/O accounting.
        from .dynamic_deployment import validate_record
        from . import dynamic_content, ownership_pages, ownership_trie  # noqa: F401
        ordinary, pathlib_open = builtins.open, io.open
        self.started = time.monotonic()
        with patch('builtins.open', lambda *a, **k: self.opened(ordinary, *a, **k)), \
                patch('io.open', lambda *a, **k: self.opened(pathlib_open, *a, **k)):
            result = validate_record(record)
        return dict(validation=result, seconds=time.monotonic() - self.started,
                    events=self.events, excluded_temporaries=self.excluded_temporaries,
                    rows=[dict(path=p, phase=f, **v)
                                             for (p, f), v in sorted(self.rows.items())])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--child', action='store_true')
    args = parser.parse_args()
    root = args.fixture.resolve()
    if args.child:
        pointer = json.loads((root / 'current.json').read_bytes())
        record = json.loads((root / pointer['manifest']).read_bytes())
        report = Reads().run(record)
        expected = dict(tree_sha256=record['tree_sha256'], vector_count=16,
                        subtrees=2,
                        validation_scope='local model/maps/snapshot bytes and global ownership; lower block bytes only if provided',
                        lower_block_contents_verified=False, lower_reader_open_required=True)
        if report['validation'] != expected:
            raise ValueError('retained fixture validation report differs from expected contract')
        report['expected_validation_report'] = expected
        payload = json.dumps(report, sort_keys=True).encode()
        if len(payload) > MAX_REPORT:
            raise ValueError('diagnostic report exceeds budget')
        sys.stdout.buffer.write(payload + b'\n')
        return
    if args.output is None:
        raise ValueError('fresh output directory required')
    output = args.output.resolve()
    if output == root or root in output.parents or output in root.parents:
        raise ValueError('output overlaps read-only fixture')
    before = inventory(root)
    output.mkdir(parents=True, exist_ok=False)
    (output / 'before.json').write_text(json.dumps(before, indent=2) + '\n')
    result = dict(status='failed', command=[sys.executable, '-m', __spec__.name,
                  '--fixture', str(root), '--child'])
    try:
        with (output / 'child.stdout').open('xb') as stdout, (output / 'child.stderr').open('xb') as stderr:
            child = subprocess.run(result['command'], stdout=stdout, stderr=stderr, timeout=10)
        result['returncode'] = child.returncode
        if child.returncode or (output / 'child.stdout').stat().st_size > MAX_REPORT:
            raise ValueError('child failed or output exceeded observed limit')
        report = json.loads((output / 'child.stdout').read_bytes())
        if report['seconds'] > 5:
            raise ValueError('observed work deadline exceeded')
        result['status'] = 'passed'
    except BaseException as error:
        result['error'] = repr(error)
        raise
    finally:
        try:
            after = inventory(root)
            (output / 'after.json').write_text(json.dumps(after, indent=2) + '\n')
            result['inventory_unchanged'] = after == before
            if after != before:
                result['status'] = 'failed'
        except BaseException as error:
            result.update(status='failed', post_inventory_error=repr(error))
        result['scope'] = 'one validation; wrapped Python returned bytes only; no cache or durability change'
        (output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    if result['status'] != 'passed':
        raise ValueError('diagnostic did not pass')


if __name__ == '__main__':
    main()
