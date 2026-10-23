"""Read-only machine inventory before admitting the build; never installs or starts work."""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import time

if platform.system() != 'Linux':
    raise RuntimeError('Run on the existing Linux test driver only')
root = Path('/mnt/spann/global-runner-integration-v1/source')
compiler_root = Path('/home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin')

def command(args):
    p = subprocess.run(args, capture_output=True, text=True, timeout=15)
    return dict(arguments=args, returncode=p.returncode, stdout=p.stdout, stderr=p.stderr)

def artifact(path):
    if not path.is_file():
        return dict(path=str(path), exists=False)
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1048576), b''):
            digest.update(chunk)
    return dict(path=str(path), resolved=str(path.resolve()), bytes=path.stat().st_size, sha256=digest.hexdigest())

configs = set()
for parent in (root, *root.parents):
    configs.update(parent/'.cargo'/name for name in ('config', 'config.toml'))
cargo_home = Path(os.environ.get('CARGO_HOME', str(Path.home()/'.cargo')))
configs.update(cargo_home/name for name in ('config', 'config.toml'))
result = dict(observed_unix=time.time(), architecture=platform.machine(),
              disk=shutil.disk_usage('/mnt/spann')._asdict(),
              memory=Path('/proc/meminfo').read_text(),
              compiler=artifact(compiler_root/'rustc'), cargo=artifact(compiler_root/'cargo'),
              cargo_configs=[artifact(p) for p in sorted(configs)],
              relevant_environment_names=sorted(k for k in os.environ if k.startswith(('RUST', 'CARGO', 'PYTHON'))),
              processes=command(['ps', '-eo', 'pid,ppid,etimes,comm,args']),
              timers=command(['systemctl', 'list-timers', '--all', '--no-pager']),
              python=command(['python3', '-c', 'import sys,numpy; print(sys.version); print(numpy.__version__); print(numpy.__file__)']))
for name in ('rustc', 'cargo'):
    p = compiler_root/name
    if p.is_file():
        result[name+'_version'] = command([str(p), '--version', '--verbose'])
# Configuration contents are intentionally not printed: inspect selected files separately
# after identifying them, because registry configuration can contain credentials.
print(json.dumps(result, indent=2))
