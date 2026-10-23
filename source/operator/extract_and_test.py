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
    if digest(manifest_path) != '62877a1d43c493e03a718372b568466b71ab7bb1359c29dfc71121d3ada3e50e':
        raise ValueError('Source manifest mismatch')
    archive = args.control / 'source.tar.gz'
    if digest(archive) != '1ec24c7d7aeec6ac7d745e94d82600164f0967c0b8324cccd940480dde897202':
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
    subprocess.run(['/usr/bin/python3', '-m', 'unittest', '-v', 'test_supervisor', 'test_monitor', 'test_host_policy'],
                   cwd=args.control, check=True, timeout=120)
    subprocess.run(['/usr/bin/python3', '-m', 'unittest', '-v', 'test_process_lifecycle', 'test_build_profile', 'test_build_validity', 'test_pair_build_gate'],
                   cwd=args.control / 'operator', check=True, timeout=120)


if __name__ == '__main__':
    main()

