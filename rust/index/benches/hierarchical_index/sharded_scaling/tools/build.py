"""Build one reader binary on the coordinator from a committed source revision.

The script exports the commit from the local checkout, syncs it into one
persistent source directory on the coordinator, and builds the reader with the
retained compiler cache. Syncing compares checksums and gives changed files a
fresh modification time, so Cargo rebuilds exactly what changed. Each binary
is copied to `reader-dev/bin/<name>-<commit12>` and never overwritten.

Usage: python3 build.py --commit SHA --name NAME [--bench two_machine_spann]
"""
import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
CHECKOUT = Path('<chroma-checkout>')
REMOTE = '/home/ubuntu/spann-two-machine-v1/reader-dev'
TARGET = '<cargo-target-dir>'
RUSTFLAGS = '-C target-cpu=native -C force-frame-pointers=yes -C debuginfo=1'


def rsh(*args, timeout=3600):
    r = subprocess.run([str(HERE / 'rsh'), 'c', *args], capture_output=True, text=True, timeout=timeout)
    if r.returncode:
        raise RuntimeError(r.stderr[-4000:] + r.stdout[-4000:])
    return r.stdout


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--commit', required=True)
    p.add_argument('--name', required=True)
    p.add_argument('--bench', default='two_machine_reader', choices=['two_machine_reader', 'two_machine_spann', 'two_machine_correctness'])
    a = p.parse_args()
    commit = subprocess.check_output(['git', '-C', str(CHECKOUT), 'rev-parse', a.commit], text=True).strip()
    dest = f'{REMOTE}/bin/{a.name}-{commit[:12]}'
    rsh(f'mkdir -p {REMOTE}/src {REMOTE}/bin {REMOTE}/builds && test ! -e {dest}')
    ip = json.loads((HERE.parent / 'reader-fixtures35/hosts.json').read_text())['<coordinator-instance>']['PublicIpAddress']
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(f'git -C {CHECKOUT} archive {commit} | tar -x -C {tmp}', shell=True, check=True)
        ssh = ('ssh -i <ssh-key.pem> -o BatchMode=yes -o StrictHostKeyChecking=yes '
               '-o UserKnownHostsFile=<known-hosts-file>')
        subprocess.run(['rsync', '-rl', '--checksum', '--delete', '-e', ssh, f'{tmp}/', f'ubuntu@{ip}:{REMOTE}/src/'],
                       check=True, timeout=1800)
    log = f'{REMOTE}/builds/{a.name}-{commit[:12]}'
    env = (f'PATH=/home/ubuntu/.cargo/bin:$PATH RUSTUP_TOOLCHAIN=1.92.0 CARGO_TARGET_DIR={TARGET} '
           f'RUSTFLAGS="{RUSTFLAGS}" CARGO_BUILD_JOBS=32')
    began = time.time()
    rsh(f'touch {REMOTE}/heartbeat; cd {REMOTE}/src && {env} cargo rustc -p chroma-index --release --locked --features clustering-profile '
        f'--bench {a.bench} --message-format=json -- --cfg distributed_owner > {log}.json 2> {log}.stderr; '
        f'code=$?; touch {REMOTE}/heartbeat; tail -5 {log}.stderr; exit $code')
    seconds = time.time() - began
    exe = rsh(f"python3 -c 'import json,sys; print([m[\"executable\"] for m in map(json.loads, open(sys.argv[1])) "
              f"if m.get(\"reason\")==\"compiler-artifact\" and m.get(\"executable\") and "
              f"m[\"target\"][\"name\"]==\"{a.bench}\"][-1])' {log}.json").strip()
    sha = rsh(f'cp {exe} {dest} && sha256sum {dest}').split()[0]
    receipt = dict(name=a.name, bench=a.bench, commit=commit, binary=dest, sha256=sha, build_seconds=seconds, rustflags=RUSTFLAGS)
    rsh(f"cat > {dest}.json <<'EOF'\n{json.dumps(receipt, indent=2)}\nEOF")
    (HERE / 'builds').mkdir(exist_ok=True)
    (HERE / 'builds' / f'{a.name}-{commit[:12]}.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt))


if __name__ == '__main__':
    sys.exit(main())
