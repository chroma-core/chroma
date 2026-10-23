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
    parser.add_argument('--control', required=True, type=Path)
    parser.add_argument('--deadline', required=True, type=float)
    args = parser.parse_args()
    if not __debug__ or os.environ.get('PYTHONOPTIMIZE'):
        raise RuntimeError('Assertions must be enabled')
    if time.time() + 300 + 300 >= args.deadline:
        raise TimeoutError('Setup and preservation cannot fit')
    if args.package.stat().st_size != 2403879 or digest(args.package) != 'ffaf0c2cdf1e5e3243cc6ab57d8d7b95b3a5d42287781dd3a433951db044c372':
        raise ValueError('Package mismatch')
    args.control.mkdir(parents=True, exist_ok=False)
    extract(args.package, args.control)
    manifest_path = args.control / 'source-manifest.json'
    if digest(manifest_path) != 'ad598187e5c7ecc6654a2073db6bf84ff89be71fbfa3e9a931d819679534cf5d':
        raise ValueError('Source manifest mismatch')
    archive = args.control / 'source.tar.gz'
    if digest(archive) != '8b704952a5df5c9ffcbd5dea5283d7c03af94ebd55baf2973aff60815f91f04a':
        raise ValueError('Source archive mismatch')
    source = args.control / 'source'
    source.mkdir()
    extract(archive, source, allow_symlink=True)
    manifest = json.loads(manifest_path.read_text())
    if len(manifest['files']) != 955:
        raise ValueError('Source count mismatch')
    for name, descriptor in manifest['files'].items():
        path = source / name
        if path.is_symlink() or path.stat().st_size != descriptor['bytes'] or digest(path) != descriptor['sha256']:
            raise ValueError('Source differs: ' + name)
    for name, target in manifest['symlinks'].items():
        if os.readlink(source / name) != target:
            raise ValueError('Source link differs')
    subprocess.run(['/mnt/spann/pilot-venv/bin/python', '-m', 'unittest', '-v', 'test_supervisor'],
                   cwd=args.control, check=True, timeout=120)


if __name__ == '__main__':
    main()
