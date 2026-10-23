#!/usr/bin/env python3
"""Build an admitted source closure under the task machine's exclusive lock."""
import argparse
import fcntl
import hashlib
import json
import os
import signal
from pathlib import Path
import shutil
import subprocess
import time

def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(8*1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()

def write(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')

def interrupted(signum, _frame):
    raise InterruptedError(f'Build supervisor received signal {signum}')

def main():
    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupted)
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--deadline', required=True, type=int)
    args = parser.parse_args()
    spec = json.loads(args.manifest.read_text())
    if spec.get('combined_review_passed') is not True or args.deadline-time.time() < 120:
        raise RuntimeError('Reviewed build admission and sufficient fixed window are required')
    kind = spec.get('build_kind')
    if kind != 'distributed':
        raise RuntimeError('Epoch13 builds only the reviewed distributed source; retain the epoch12 reference')
    if spec.get('source') != '/home/ubuntu/spann-two-machine-v1/epoch27/source' or not spec.get('source_files'):
        raise RuntimeError('Fresh immutable epoch27 source closure is required')
    if spec.get('cargo_target_dir') != '/home/ubuntu/spann-single-shard-3m-attribution-v1/epoch2/target':
        raise RuntimeError('Reuse the reviewed epoch12 distributed build cache')
    lock = Path('/home/ubuntu/spann-benchmark.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    args.output.mkdir(parents=True, exist_ok=False)
    source = Path(spec['source']).resolve()
    expected = dict(spec['source_files'])
    actual = set()
    for path in source.rglob('*'):
        if path.is_symlink():
            raise RuntimeError('Extracted source must contain regular files only')
        if path.is_file():
            name = str(path.relative_to(source))
            actual.add(name)
            if expected.get(name) != sha(path):
                raise RuntimeError(f'Source hash mismatch: {name}')
    if actual != set(expected):
        raise RuntimeError('Source closure differs from admission')
    # Archives normalize mtimes. Cargo's shared target cache can otherwise treat
    # changed source in a fresh directory as older than its cached executable.
    # Refresh only timestamps, after verifying the complete content closure.
    for name in expected:
        os.utime(source/name, None)
    target = Path(spec['cargo_target_dir']).resolve()
    target.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(target).free < spec['minimum_free_bytes']:
        raise RuntimeError('Insufficient disk headroom; preserved artifacts remain untouched')
    env = dict(os.environ, PATH='/home/ubuntu/.cargo/bin:'+os.environ.get('PATH',''), RUSTUP_TOOLCHAIN='1.92.0', CARGO_TARGET_DIR=str(target), RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes -C debuginfo=1')
    for key in ['RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'CARGO_ENCODED_RUSTFLAGS']:
        env.pop(key, None)
    compiler = subprocess.check_output(['rustc','--version'], env=env, text=True).strip()
    if not compiler.startswith('rustc 1.92.0 '):
        raise RuntimeError('Compiler does not match the reference')
    hardware = subprocess.check_output(['lscpu','-p=CPU,CORE,SOCKET'], text=True)
    cores = {tuple(line.split(',')[1:]) for line in hardware.splitlines() if line and not line.startswith('#')}
    if len(cores) != 32:
        raise RuntimeError('Expected 32 physical cores')
    write(args.output/'environment.json', {'compiler':compiler, 'physical_cores':len(cores), 'hardware':hardware, 'deadline':args.deadline, 'manifest_sha256':sha(args.manifest), 'env':{k:env[k] for k in ['RUSTUP_TOOLCHAIN','CARGO_TARGET_DIR','RUSTFLAGS']}})
    benches = ['two_machine_reader', 'two_machine_spann'] if kind == 'distributed' else ['single_shard_attribution']
    for name in benches:
        seconds = min(900, args.deadline-time.time()-60)
        if seconds < 60:
            raise RuntimeError('Insufficient remaining fixed window for build')
        command = ['cargo','rustc','-p','chroma-index','--release','--locked','--bench',name,'--message-format=json']
        if kind == 'distributed':
            command += ['--','--cfg','distributed_owner']
        write(args.output/f'{name}.command.json', {'argv':command, 'cwd':str(source), 'timeout_seconds':seconds})
        started = time.time()
        failure = None
        with (args.output/f'{name}.stdout').open('w') as stdout, (args.output/f'{name}.stderr').open('w') as stderr:
            process = subprocess.Popen(command, cwd=source, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
            try:
                code = process.wait(timeout=seconds)
            except BaseException as error:
                failure = type(error).__name__ + ': ' + str(error)
                code = 124 if isinstance(error, subprocess.TimeoutExpired) else 130
            finally:
                for signum in (signal.SIGTERM, signal.SIGHUP):
                    signal.signal(signum, signal.SIG_IGN)
                # Cargo descendants share this dedicated group. Do not release
                # the writer lock while a compiler can still modify the target.
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                    time.sleep(1)
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                for _ in range(100):
                    remaining = []
                    for stat in Path('/proc').glob('[0-9]*/stat'):
                        try:
                            fields = stat.read_text().rsplit(')', 1)[1].split()
                            if int(fields[2]) == process.pid and fields[0] not in ('Z', 'X'):
                                remaining.append(stat.parent.name)
                        except (FileNotFoundError, ProcessLookupError):
                            continue
                    if not remaining:
                        break
                    time.sleep(.1)
                if remaining:
                    write(args.output/f'{name}.orphan-failure.json', {'remaining_pids':remaining, 'process_group':process.pid})
                    # Keep the exclusive lock until the independent host guard
                    # stops the machine; another writer must not reuse the target.
                    while time.time() < args.deadline:
                        time.sleep(1)
                    raise RuntimeError('Compiler descendants remain; host shutdown required')
        for signum in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, interrupted)
        write(args.output/f'{name}.process.json', {'exit_code':code, 'wall_seconds':time.time()-started, 'failure':failure, 'process_group':process.pid})
        if code:
            raise RuntimeError(f'Build failed: {name}; {failure or "compiler returned failure"}')
        artifacts = []
        finished = False
        for line in (args.output/f'{name}.stdout').read_text().splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get('reason') == 'build-finished':
                finished = event.get('success') is True
            info = event.get('target', {})
            if event.get('reason') == 'compiler-artifact' and event.get('executable') and info.get('name') == name and 'bench' in info.get('kind', []):
                if Path(info['src_path']).resolve() != source/f'rust/index/benches/{name}.rs' or 'chroma-index' not in event.get('package_id',''):
                    raise RuntimeError('Compiler artifact identifies a different source')
                artifacts.append(event)
        if not finished or len(artifacts) != 1:
            raise RuntimeError('Expected one exact successful compiler artifact')
        if artifacts[0].get('fresh') is not False:
            raise RuntimeError('Fresh source must compile; cached executable is not admitted')
        binary = Path(artifacts[0]['executable']).resolve()
        if target/'release'/'deps' not in binary.parents or not os.access(binary, os.X_OK):
            raise RuntimeError('Compiler artifact is outside admitted target')
        saved = args.output/name
        shutil.copy2(binary, saved)
        write(args.output/f'{name}.binary.json', {'path':str(saved), 'sha256':sha(saved), 'compiler_artifact':artifacts[0]})

if __name__ == '__main__':
    main()
