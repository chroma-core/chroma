"""Run reader bench processes and collect their results.

One invocation runs one or two reader binaries in a fixed order (default A, B,
B, A), each as a fresh process against the same fixture, queries and config.
Each process writes a results directory on the coordinator; this script copies
it back under `runs/<label>/<order>-<side>/` and records the binaries' hashes,
the source commits and both machines' state in `runs/<label>/run.json`. With
only binary A, an A/A run splits its processes into two halves for comparison.

`--reader single` runs today's single-process reader on the coordinator.
`--reader owners` runs the replicated-tree reader. With `--two-machines`, the
coordinator scans owner 1 and the peer serves owner 2: before every process
the script starts the peer's scan server with the same binary, so each process
meets an empty peer cache, and stops it afterwards. Without `--two-machines`
the coordinator scans every owner itself.

The machines must be running and `reader-fixtures35/hosts.json` must hold
their current public addresses. Every process refreshes the idle-stop
heartbeat on both machines.

Usage:
  python3 run-bench.py --label NAME --fixture 10m --config configs/x.json \
      --a /remote/path/reader --commit-a SHA [--b /remote/path/reader --commit-b SHA] \
      [--reader owners --two-machines]
"""
import argparse
import hashlib
import json
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent / 'reader-fixtures35'
MACHINES = {'c': '<coordinator-instance>', 'p': '<peer-instance>'}
REMOTE = '/home/ubuntu/spann-two-machine-v1/reader-dev'
HEARTBEAT = f'{REMOTE}/heartbeat'
TOKEN = '/home/ubuntu/spann-two-machine-v1/epoch1/run-auth.token'
SSH_OPTIONS = ['-i', '<ssh-key.pem>', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
               '-o', 'UserKnownHostsFile=<known-hosts-file>']
SIZES = {
    '1m': dict(rows=1_000_000, fixture='1000000', vector_path=None, peer_vector_path=None, cache=12 * 2**30),
    '3m': dict(rows=3_000_000, fixture='3000000', vector_path=None, peer_vector_path=None, cache=24 * 2**30),
    '10m': dict(rows=10_000_000, fixture='10000000', vector_path='/run/spann-reader-dev/fineweb10m.f32',
                peer_vector_path='/run/spann-reader35-input/fineweb10m.f32', cache=48 * 2**30),
}
# Sizes of the FineWeb 100M ladder built on the builder machine. Their
# ground truth and queries live on that machine; pass them with
# --remote-input, and the size's generation with --generation/--storage.
for _m in (1, 2, 3, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100):
    SIZES[f'ladder-{_m}m'] = dict(rows=_m * 1_000_000, fixture=f'ladder-{_m}m', vector_path='/data/fineweb100m.f32',
                                  peer_vector_path=None, cache=48 * 2**30)
RECEIPTS = {'c': '/run/spann-reader-dev', 'p': '/run/spann-reader35-input'}
PEER_BIN = '/run/spann-reader35-input/bin'
PEER_SCAN = ('9200', '9201')
# One hardware thread on each of 28 of the 32 physical cores, leaving four
# cores for network and storage threads. Valid only when CPU N and N+32 are
# hyperthread siblings, which the script checks before pinning.
DEFAULT_PIN = '0-27'
PROCESS_TIMEOUT = 2 * 3600
ENV = 'SPANN_STORAGE_RUNTIME_WORKERS=16'


class RemoteError(RuntimeError):
    pass


# Set by --pair: an extra machine pair described in pairs/<name>.json.
PAIR = None


def machine(m):
    if PAIR:
        return json.loads((HERE / 'pairs' / f'{PAIR}.json').read_text())[m]
    return json.loads((FIXTURES / 'hosts.json').read_text())[MACHINES[m]]


def host(m):
    return 'ubuntu@' + machine(m)['PublicIpAddress']


def private_ip(m):
    return machine(m)['PrivateIpAddress']


def ssh(cmd, m='c', timeout=600):
    r = subprocess.run(['ssh', *SSH_OPTIONS, host(m), cmd], capture_output=True, text=True, timeout=timeout)
    if r.returncode == 255:
        raise RemoteError(f'ssh connection to {m} failed: {r.stderr[-2000:]}')
    if r.returncode:
        raise RemoteError(f'{m}: command exited {r.returncode}: {cmd}\n{r.stderr[-3000:]}\n{r.stdout[-3000:]}')
    return r.stdout


# Copies of the large query inputs already on the machines. `put` copies one
# remotely instead of uploading when its SHA-256 matches the local file.
REMOTE_COPIES = {
    'reader35-queries1000.jsonl': '/home/ubuntu/spann-two-machine-v1/development30/reader35-queries1000.jsonl',
    **{f'{n}.exact.jsonl': f'/home/ubuntu/spann-two-machine-v1/development30/reader35-groundtruth/{n}.exact.jsonl'
       for n in ('1000000', '3000000', '10000000')},
}


def put(local, remote, m='c'):
    copy = REMOTE_COPIES.get(Path(local).name)
    if copy:
        digest = hashlib.sha256(Path(local).read_bytes()).hexdigest()
        out = ssh(f'sha256sum {shlex.quote(copy)} 2>/dev/null || true', m).split()
        if out and out[0] == digest:
            ssh(f'cp {shlex.quote(copy)} {shlex.quote(remote)}', m)
            return
    subprocess.run(['scp', '-q', *SSH_OPTIONS, str(local), f'{host(m)}:{remote}'], check=True, timeout=600)


def fetch(remote, local, m='c'):
    """Copy a remote file or directory to `local` as a gzip-compressed tar
    stream; per-query records compress several times over a slow link."""
    local.parent.mkdir(parents=True, exist_ok=True)
    parent, name = remote.rstrip('/').rsplit('/', 1)
    with tempfile.TemporaryDirectory(dir=local.parent) as tmp:
        sender = subprocess.Popen(['ssh', *SSH_OPTIONS, host(m), f'tar -C {shlex.quote(parent)} -czf - {shlex.quote(name)}'],
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        r = subprocess.run(['tar', '-C', tmp, '-xzf', '-'], stdin=sender.stdout, capture_output=True, timeout=3600)
        sender.stdout.close()
        if sender.wait(timeout=120) or r.returncode:
            return False
        Path(tmp, name).rename(local)
    return True


def machine_state(m):
    probe = ('lscpu; echo ---; (numactl -H || true); echo ---; cat /sys/kernel/mm/transparent_hugepage/enabled; '
             'cat /sys/devices/system/clocksource/clocksource0/current_clocksource; uname -r; free -b; df -B1 / /run')
    return ssh(probe, m)


def check_pinning(m):
    siblings = ssh('cat /sys/devices/system/cpu/cpu0/topology/thread_siblings_list', m).strip()
    if siblings != '0,32':
        sys.exit(f'{m}: CPU 0 siblings are {siblings!r}, not "0,32"; choose --taskset explicitly or pass --taskset ""')


def stage_from_build_machine(binary):
    """Copy a binary built on the original coordinator to this pair's query
    machine at the same path, checking its hash. A no-op on the original pair."""
    if not PAIR:
        return
    build = 'ubuntu@' + json.loads((FIXTURES / 'hosts.json').read_text())[MACHINES['c']]['PublicIpAddress']
    sha = subprocess.run(['ssh', *SSH_OPTIONS, build, f'sha256sum {binary}'], capture_output=True, text=True,
                         check=True, timeout=120).stdout.split()[0]
    have = ssh(f'test -f {binary} && sha256sum {binary} || true').split()
    if have and have[0] == sha:
        return
    ssh(f'mkdir -p {Path(binary).parent}')
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / Path(binary).name
        subprocess.run(['scp', '-q', *SSH_OPTIONS, f'{build}:{binary}', str(local)], check=True, timeout=900)
        put(local, binary)
    if ssh(f'chmod +x {binary} && sha256sum {binary}').split()[0] != sha:
        raise RemoteError(f'copy of {binary} on {PAIR} differs from the build machine')


def stage_on_peer(binary, sha):
    """Copy a coordinator binary to the peer through this machine, checking its hash."""
    dest = f'{PEER_BIN}/{Path(binary).name}'
    have = ssh(f'test -f {dest} && sha256sum {dest} || true', 'p').split()
    if have and have[0] == sha:
        return dest
    ssh(f'mkdir -p {PEER_BIN}', 'p')
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / Path(binary).name
        subprocess.run(['scp', '-q', *SSH_OPTIONS, f'{host("c")}:{binary}', str(local)], check=True, timeout=900)
        put(local, dest, 'p')
    got = ssh(f'chmod +x {dest} && sha256sum {dest}', 'p').split()[0]
    if got != sha:
        raise RemoteError(f'peer copy of {binary} has hash {got}, expected {sha}')
    return dest


def fixture_args(run, size):
    return ['--store', f'{run}/unused-store', '--storage-config', f'{run}/inputs/storage.json',
            '--descriptor', f'{run}/inputs/generation.json', '--expected-rows', str(size['rows'])]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--label', required=True)
    p.add_argument('--fixture', choices=SIZES, required=True)
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--a', required=True)
    p.add_argument('--commit-a', required=True)
    p.add_argument('--b')
    p.add_argument('--commit-b')
    p.add_argument('--order', default=None, help='default ABBA, or AAAA for an A/A run')
    p.add_argument('--reader', choices=['single', 'owners'], default='single')
    p.add_argument('--two-machines', action='store_true')
    p.add_argument('--query-limit', type=int)
    p.add_argument('--passes', default='cold,warm')
    p.add_argument('--cold-threads', type=int, default=32)
    p.add_argument('--vector-path')
    p.add_argument('--full-vectors', choices=['corpus', 'store'], default='corpus',
                   help='rerank from a local corpus copy or from the vector stores saved with the generation')
    p.add_argument('--hold-cpu-latency', type=int, metavar='US',
                   help='hold /dev/cpu_dma_latency at this many microseconds on every machine during the run, '
                        'which keeps idle cores out of deep sleep states (released when the run ends)')
    p.add_argument('--pair', help='run on the extra pair in pairs/<name>.json instead of the original machines')
    p.add_argument('--no-corpus', action='store_true',
                   help='use no local corpus copy (needs --full-vectors store; recall@100 ties stay unresolved)')
    p.add_argument('--remote-input', action='append', default=[], metavar='NAME=PATH',
                   help='copy this input from a path already on the query machine instead of uploading it')
    p.add_argument('--bench-arg', action='append', default=[],
                   help='extra argument passed to every bench process, e.g. --bench-arg=--record-leaves')
    p.add_argument('--generation', help='fixture generation descriptor to use instead of the default')
    p.add_argument('--storage', help='fixture storage config to use instead of the default')
    p.add_argument('--taskset', default=DEFAULT_PIN, help='CPU list for reader processes; empty to skip pinning')
    a = p.parse_args()
    global PAIR
    PAIR = a.pair
    if a.no_corpus and a.full_vectors != 'store':
        sys.exit('--no-corpus needs --full-vectors store')
    order = a.order or ('ABBA' if a.b else 'AAAA')
    if bool(a.b) != bool(a.commit_b) or set(order) - ({'A', 'B'} if a.b else {'A'}):
        sys.exit('--b and --commit-b go together; without them the order may only contain A')
    if a.two_machines and a.reader != 'owners':
        sys.exit('--two-machines needs --reader owners')
    size = SIZES[a.fixture]
    machines = ['c', 'p'] if a.two_machines else ['c']
    local_run = HERE / 'runs' / a.label
    if local_run.exists():
        sys.exit(f'{local_run} exists; preserve prior runs')
    vector_path = None if a.no_corpus else (a.vector_path or size['vector_path'])
    if vector_path:
        ssh(f'test -f {shlex.quote(vector_path)}')
    if a.two_machines and size['peer_vector_path'] and not a.no_corpus:
        ssh(f'test -f {shlex.quote(size["peer_vector_path"])}', 'p')
    if a.taskset == DEFAULT_PIN:
        for m in machines:
            check_pinning(m)
    remote_run = f'{REMOTE}/runs/{a.label}'
    inputs = {
        'queries.jsonl': FIXTURES / 'query-data/reader35-queries1000.jsonl',
        'groundtruth.jsonl': FIXTURES / f'query-data/groundtruth/{size["fixture"]}.exact.jsonl',
        'storage.json': Path(a.storage) if a.storage else FIXTURES / f'fixtures/{size["fixture"]}/storage.json',
        'generation.json': Path(a.generation) if a.generation else FIXTURES / f'fixtures/{size["fixture"]}/generation.json',
        'config.json': a.config,
    }
    remote_inputs = dict(x.split('=', 1) for x in a.remote_input)
    for m in machines:
        ssh(f'test ! -e {remote_run} && mkdir -p {remote_run}/inputs', m)
        for name, path in inputs.items():
            if name in remote_inputs:
                ssh(f'cp {shlex.quote(remote_inputs[name])} {remote_run}/inputs/{name}', m)
            elif m == 'c' or name in ('storage.json', 'generation.json'):
                put(path, f'{remote_run}/inputs/{name}', m)
    binaries = {'A': a.a, 'B': a.b} if a.b else {'A': a.a}
    for path in binaries.values():
        stage_from_build_machine(path)
    hashes = {side: ssh(f'sha256sum {shlex.quote(path)}').split()[0] for side, path in binaries.items()}
    peer_binaries = {side: stage_on_peer(path, hashes[side]) for side, path in binaries.items()} if a.two_machines else {}
    receipt = dict(label=a.label, fixture=a.fixture, remote_inputs=remote_inputs, order=order, reader=a.reader, two_machines=a.two_machines,
                   binaries=binaries, peer_binaries=peer_binaries, sha256=hashes,
                   commits={'A': a.commit_a, 'B': a.commit_b}, args=vars(a) | {'config': str(a.config)},
                   started_unix=time.time(), machine_state={m: machine_state(m) for m in machines}, processes=[])
    local_run.mkdir(parents=True)
    write_receipt = lambda: (local_run / 'run.json').write_text(json.dumps(receipt, indent=2) + '\n')
    write_receipt()
    holders = {}
    if a.hold_cpu_latency is not None:
        # The kernel honors the request only while the file stays open, so a
        # holder process keeps it open and the setting reverts when it exits.
        hold = ("import os,struct,time; f=os.open('/dev/cpu_dma_latency', os.O_WRONLY); "
                f"os.write(f, struct.pack('i', {int(a.hold_cpu_latency)})); time.sleep(6*3600)")
        for m in machines:
            holders[m] = ssh(f'sudo setsid nohup python3 -c {shlex.quote(hold)} > /dev/null 2>&1 < /dev/null & echo $!', m).strip()
        receipt['cpu_latency_hold'] = {'microseconds': a.hold_cpu_latency, 'pids': holders}
        write_receipt()
    pin = ['taskset', '-c', a.taskset] if a.taskset else []
    for i, side in enumerate(order):
        out = f'{remote_run}/{i}-{side}'
        row = dict(index=i, side=side)
        receipt['processes'].append(row)
        server_pid = None
        if a.two_machines:
            serve_out = f'{remote_run}/{i}-{side}-serve'
            serve = [*pin, peer_binaries[side], '--mode', 'serve', *fixture_args(remote_run, size),
                     '--local-owners', '2', '--scan-data-bind', f'{private_ip("p")}:{PEER_SCAN[0]}',
                     '--scan-control-bind', f'{private_ip("p")}:{PEER_SCAN[1]}', '--token-file', TOKEN,
                     '--receipt-dir', RECEIPTS['p'], '--full-vectors', a.full_vectors, '--output', serve_out]
            if size['peer_vector_path']:
                serve += ['--vector-path', size['peer_vector_path']]
            row['serve_argv'] = serve
            ssh(f'touch {HEARTBEAT}; {ENV} setsid nohup {shlex.join(serve)} > {serve_out}.log 2>&1 < /dev/null & '
                'echo started', 'p')
            for _ in range(600):
                ready = ssh(f'cat {serve_out}/ready.json 2>/dev/null || true', 'p')
                if ready.strip():
                    server_pid = json.loads(ready)['pid']
                    row['serve_ready'] = json.loads(ready)
                    break
                if 'Error' in ssh(f'cat {serve_out}.log 2>/dev/null || true', 'p'):
                    break
                time.sleep(2)
            if server_pid is None:
                row['status'] = 'peer server did not become ready'
                fetch(f'{serve_out}.log', local_run / f'{i}-{side}-serve.log', 'p')
                write_receipt()
                sys.exit(f'peer server for process {i} did not become ready')
        argv = [*pin, binaries[side], '--mode', 'bench', *fixture_args(remote_run, size),
                '--vector-cache-bytes', str(size['cache']),
                '--queries', f'{remote_run}/inputs/queries.jsonl',
                '--groundtruth', f'{remote_run}/inputs/groundtruth.jsonl',
                '--config', f'{remote_run}/inputs/config.json',
                '--passes', a.passes, '--cold-threads', str(a.cold_threads), '--output', out]
        if vector_path:
            argv += ['--vector-path', vector_path]
        if a.no_corpus:
            argv += ['--no-vectors']
        if a.query_limit:
            argv += ['--query-limit', str(a.query_limit)]
        argv += [x.replace('{out}', out) for x in a.bench_arg]
        if a.reader == 'owners':
            argv += ['--reader', 'owners', '--receipt-dir', RECEIPTS['c'], '--full-vectors', a.full_vectors]
            # Built once per fixture by the first run, then reused; it maps each
            # true neighbor to its leaf for the per-level and optimal recall columns.
            argv += ['--gt-leaf-map', f'{RECEIPTS["c"]}/gt-leaf-map-{a.fixture}.json']
            if a.two_machines:
                argv += ['--local-owners', '1', '--token-file', TOKEN, '--remote',
                         f'2@{private_ip("p")}:{PEER_SCAN[0]}/{private_ip("p")}:{PEER_SCAN[1]}']
        row['argv'] = argv
        cmd = (f'touch {HEARTBEAT}; {ENV} {shlex.join(argv)} > {out}.stdout 2> {out}.stderr; '
               f'code=$?; touch {HEARTBEAT}; exit $code')
        began = time.time()
        try:
            ssh(cmd, timeout=PROCESS_TIMEOUT)
            status = 'ok'
        except subprocess.TimeoutExpired:
            status = 'timed out locally; the remote process may still be running'
        except RemoteError as e:
            status = f'failed: {e}'
        row.update(seconds=time.time() - began, status=status)
        for suffix in ('.stdout', '.stderr'):
            fetch(out + suffix, local_run / f'{i}-{side}{suffix}')
        if status == 'ok' and not fetch(out, local_run / f'{i}-{side}'):
            row['status'] = status = 'results copy failed'
        if server_pid is not None:
            ssh(f'kill {server_pid} || true; touch {HEARTBEAT}', 'p')
            fetch(f'{remote_run}/{i}-{side}-serve', local_run / f'{i}-{side}-serve', 'p')
            fetch(f'{remote_run}/{i}-{side}-serve.log', local_run / f'{i}-{side}-serve.log', 'p')
        write_receipt()
        print(i, side, status, flush=True)
        if status != 'ok':
            sys.exit(1)
    for m, pid in holders.items():
        ssh(f'sudo kill {pid} || true', m)
    dirs = {s: [local_run / f'{i}-{x}' for i, x in enumerate(order) if x == s] for s in set(order)}
    if a.b:
        left, right = dirs['A'], dirs['B']
    else:
        left, right = dirs['A'][0::2], dirs['A'][1::2]
    join = lambda paths: shlex.quote(','.join(str(p) for p in paths))
    if right:
        print('\nCompare with:\n  python3', shlex.quote(str(HERE / 'compare.py')), join(left), join(right))


if __name__ == '__main__':
    main()
