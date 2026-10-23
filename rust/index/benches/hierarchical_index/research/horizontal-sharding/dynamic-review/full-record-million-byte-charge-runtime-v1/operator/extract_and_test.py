"""Verify and extract the immutable package into a fresh control directory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import time


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            h.update(block)
    return h.hexdigest()


def extract(archive, destination, allow_symlink=False):
    with tarfile.open(archive) as tar:
        members = tar.getmembers()
        names = set()
        for member in members:
            relative = Path(member.name)
            if relative.is_absolute() or '..' in relative.parts or member.name in names:
                raise ValueError('Unsafe or duplicate member')
            names.add(member.name)
            if not (member.isfile() or member.isdir() or (allow_symlink and member.issym())):
                raise ValueError('Unsupported archive member')
            if member.issym() and not (destination / relative.parent / member.linkname).resolve().is_relative_to(destination.resolve()):
                raise ValueError('Escaping symlink')
        tar.extractall(destination, members=members, filter='data')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--package', required=True, type=Path)
    parser.add_argument('--package-sha256', required=True)
    parser.add_argument('--package-bytes', required=True, type=int)
    parser.add_argument('--control', required=True, type=Path)
    parser.add_argument('--deadline', required=True, type=float)
    args = parser.parse_args()
    if not __debug__ or os.environ.get('PYTHONOPTIMIZE'):
        raise RuntimeError('Assertions must be enabled')
    if time.time() + 300 + 300 >= args.deadline:
        raise TimeoutError('Setup and preservation cannot fit')
    if args.package.stat().st_size != args.package_bytes or digest(args.package) != args.package_sha256:
        raise ValueError('Package mismatch')
    args.control.mkdir(parents=True, exist_ok=False)
    extract(args.package, args.control)
    manifest_path = args.control / 'source-manifest.json'
    if digest(manifest_path) != 'd1456895c79c8c5dc74c1f834bbb5b60ba0584cb8945332e9fe4482896318dc5':
        raise ValueError('Source manifest mismatch')
    archive = args.control / 'source.tar.gz'
    if digest(archive) != 'b63907d5702a5949367733418ade13a98d2d300a2b52fc75902bb66ffa829eb7':
        raise ValueError('Source archive mismatch')
    source = args.control / 'source'
    extract(archive, args.control, allow_symlink=True)
    manifest = json.loads(manifest_path.read_text())
    if len(manifest['files']) != 957:
        raise ValueError('Source count mismatch')
    for name, descriptor in manifest['files'].items():
        path = source / name
        if path.is_symlink() or path.stat().st_size != descriptor['bytes'] or digest(path) != descriptor['sha256']:
            raise ValueError('Source differs: ' + name)
    for name, target in manifest['symlinks'].items():
        if os.readlink(source / name) != target:
            raise ValueError('Source link differs')
    subprocess.run(['/mnt/spann/pilot-venv/bin/python', '-m', 'unittest', '-v', 'test_supervisor', 'test_monitor'],
                   cwd=args.control, check=True, timeout=120)
    subprocess.run(['/mnt/spann/pilot-venv/bin/python', '-m', 'unittest', '-v', 'test_process_lifecycle'],
                   cwd=args.control / 'operator', check=True, timeout=120)


if __name__ == '__main__':
    main()

