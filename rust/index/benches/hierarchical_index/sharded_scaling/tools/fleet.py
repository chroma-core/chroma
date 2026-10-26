"""Machine-count experiment: serve one 100M index from 1, 2, 4 and 8 machines.

The index is the 100M FineWeb ladder size, saved with its full-vector stores
in S3. Each reader process splits it at open time into M parts by subtree (`--parts M`). Machine m1 navigates
every query, scans part 1 itself and sends one scan request to each other
part's machine; machine mj serves part j. Every machine is an r6i.4xlarge, so
total resources grow with M (the deployment comparison, not a fixed-cost one).
Full vectors for rerank come from the vector store, so no machine needs a
local corpus copy. Recall@100 ties beyond the listed references stay
unresolved for the same reason.

Each configuration runs a parallel cold pass to fill caches, a warm pass one
query at a time (latency), and a second parallel pass on warm caches
(throughput at a fixed concurrency).

Usage:
  python3 fleet.py launch                 # 8 x r6i.4xlarge, pairs/fleet.json
  python3 fleet.py stage                  # after the 100M save with vector stores
  python3 fleet.py run --machines 1 2 4 8
  python3 fleet.py stop
"""
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import importlib.util
spec = importlib.util.spec_from_file_location('pair', HERE / 'pair.py')
pair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pair)

NAME = 'fleet'
ROLES = [f'm{i}' for i in range(1, 9)]
BUCKET = '<artifact-bucket>'
STAGING = 'sha256/ladder100m-staging'
BINARY = 'fleet-reader-8366bca9ace4'
SAVE = '/data/ladder2/store-100000000'
ROOT = '/home/ubuntu/fleet'
TOKEN = '/home/ubuntu/spann-two-machine-v1/epoch1/run-auth.token'
ROWS = 100_000_000


# Each second, on every machine of a run: the time, the aggregate `cpu` line of
# /proc/stat (user nice system idle iowait irq softirq steal ... in clock
# ticks) and the number of runnable threads, so each machine's CPU use and run
# queue can be read for any pass's time window.
SAMPLER = '''#!/bin/bash
while true; do
  echo "$(date +%s.%N) $(head -1 /proc/stat) $(grep ^procs_running /proc/stat)"
  sleep 1
done
'''


def machines():
    return json.loads((HERE / 'pairs' / f'{NAME}.json').read_text())


def ssh(role, cmd, timeout=1800, stdin=None):
    return pair.ssh(machines()[role]['PublicIpAddress'], cmd, timeout=timeout, stdin=stdin)


def bsh(cmd):
    return subprocess.run([str(HERE / 'bsh'), cmd], capture_output=True, text=True, check=True).stdout


def s3_get(role, key, dest):
    ssh(role, f"python3 -c \"import boto3;boto3.Session(region_name='us-east-1').client('s3').download_file("
              f"'{BUCKET}','{STAGING}/{key}','{dest}',ExtraArgs={{'ExpectedBucketOwner':'<aws-account-id>'}})\"")


def stage():
    # The 100M store save wrote its generation (event `generation_saved`), but
    # its serial fresh-reader validation was stopped, so it has no result.json.
    # The descriptor comes from the writer's own file; validation runs
    # separately with the parallel verifier, and fails void these runs.
    if not bsh(f'grep -c generation_saved {SAVE}/owner1/events.jsonl || true').strip().startswith('1'):
        sys.exit('100M generation not saved')
    generation = json.dumps(json.loads(bsh(f'cat {SAVE}/owner1/first-generation-descriptor.json')), indent=1)
    storage = bsh(f'cat {SAVE}/storage.json')
    # Ground truth and queries go to S3 from the builder; only m1 needs them.
    bsh("~/venv/bin/python -c \"import boto3;s3=boto3.Session(region_name='us-east-1').client('s3');"
        f"[s3.upload_file(src,'{BUCKET}','{STAGING}/'+key,ExtraArgs={{'ExpectedBucketOwner':'<aws-account-id>'}}) for src,key in "
        "[('/data/gt/merged/dev-100000000.exact.jsonl','gt-dev-100000000.exact.jsonl'),('/data/queries/dev1000.jsonl','queries-dev1000.jsonl')]]\"")
    for role in ROLES:
        ssh(role, f'mkdir -p {ROOT}/inputs')
        s3_get(role, f'bin/{BINARY}', f'{ROOT}/{BINARY}')
        ssh(role, f'chmod +x {ROOT}/{BINARY}; cat > {ROOT}/inputs/generation.json', stdin=generation)
        ssh(role, f'cat > {ROOT}/inputs/storage.json', stdin=storage)
    s3_get('m1', 'gt-dev-100000000.exact.jsonl', f'{ROOT}/inputs/groundtruth.jsonl')
    s3_get('m1', 'queries-dev1000.jsonl', f'{ROOT}/inputs/queries.jsonl')
    ssh('m1', f'cat > {ROOT}/inputs/config.json', stdin=(HERE / 'configs' / 'fleet-100m.json').read_text())
    print('staged', [ssh(r, f'sha256sum {ROOT}/{BINARY}').split()[0][:12] for r in ROLES])


# The vector-store block cache counts blocks, not bytes: its capacity is the
# byte budget over the store's 512 KiB maximum block size. Stored blocks
# average about 220-320 KB, so this budget (280,000 blocks) holds at most about
# 90 GB, which fits in an r6i.4xlarge's 123 GiB beside about 12 GB of postings.
# A warm recall@100 pass on one machine touches about 233,000 blocks, so every
# configuration's warm pass is served from cache at every machine count.
VECTOR_CACHE_BYTES = 280_000 * 512 * 1024
# Rerank keeps many S3 connections open at once. At SSH's default soft limit
# of 1,024 open files, a reader's S3 reads fail ("dispatch failure") and a
# server's listener stops accepting connections. Each process raises its own
# soft limit to the hard limit.
OPEN_FILES = 1_048_576


def common():
    return (f'--store {ROOT}/unused-store --storage-config {ROOT}/inputs/storage.json '
            f'--descriptor {ROOT}/inputs/generation.json --expected-rows {ROWS} --full-vectors store '
            f'--no-vectors --vector-store-cache-bytes {VECTOR_CACHE_BYTES} --posting-cache-bytes 68719476736')


def run(counts):
    ips = {r: m['PrivateIpAddress'] for r, m in machines().items()}
    out_root = HERE / 'runs'
    for m in counts:
        label = f'fleet-100m-{m}machines'
        if (out_root / label).exists():
            print('exists', label)
            continue
        for role in ROLES:
            ssh(role, f'touch {pair.HEARTBEAT}')
        servers = {}
        for j in range(2, m + 1):
            role = f'm{j}'
            ssh(role, f'pkill -f "[{BINARY[0]}]{BINARY[1:]} --mode serve" || true; rm -rf {ROOT}/serve-{m}')
            ssh(role, f'cd {ROOT}; ulimit -n {OPEN_FILES}; setsid nohup ./{BINARY} --mode serve {common()} --parts {m} --local-owners {j} '
                      f'--token-file {TOKEN} --scan-data-bind {ips[role]}:9200 --scan-control-bind {ips[role]}:9201 '
                      f'--output {ROOT}/serve-{m} > serve-{m}.log 2>&1 < /dev/null & echo ok')
            servers[role] = j
        for role in servers:
            for _ in range(600):
                if ssh(role, f'test -f {ROOT}/serve-{m}/ready.json && echo y || true').strip() == 'y':
                    break
                time.sleep(3)
            else:
                sys.exit(f'{role} did not become ready: ' + ssh(role, f'tail -c 1000 {ROOT}/serve-{m}.log'))
        remotes = ' '.join(f'--remote {j}@{ips[r]}:9200/{ips[r]}:9201' for r, j in servers.items())
        local = f'--local-owners 1 --token-file {TOKEN}' if servers else ''
        out = f'{ROOT}/runs/{label}'
        hold = ("sudo setsid nohup python3 -c \"import os,struct,time; f=os.open('/dev/cpu_dma_latency', os.O_WRONLY); "
                "os.write(f, struct.pack('i', 10)); time.sleep(4*3600)\" > /dev/null 2>&1 < /dev/null &")
        for role in ['m1', *servers]:
            ssh(role, hold)
            ssh(role, f'cat > {ROOT}/cpu-sampler.sh; chmod +x {ROOT}/cpu-sampler.sh', stdin=SAMPLER)
            ssh(role, f'setsid nohup {ROOT}/cpu-sampler.sh > {ROOT}/cpu-{label}.log 2>&1 < /dev/null & echo $! > {ROOT}/cpu-{label}.pid')
        began = time.time()
        # The bench runs detached on m1 and writes its exit code to {out}.exit,
        # so a dropped SSH connection from this driver does not stop it.
        bench = (f'ulimit -n {OPEN_FILES}; SPANN_STORAGE_RUNTIME_WORKERS=16 ./{BINARY} --mode bench {common()} '
                 f'--parts {m} {local} {remotes} --queries {ROOT}/inputs/queries.jsonl '
                 f'--groundtruth {ROOT}/inputs/groundtruth.jsonl --config {ROOT}/inputs/config.json '
                 f'--passes cold,warm,cold --cold-threads 32 --output {out} --reader owners --no-diagnostics '
                 f'> {out}.stdout 2> {out}.stderr; echo $? > {out}.exit')
        ssh('m1', f"mkdir -p {ROOT}/runs; cd {ROOT}; rm -f {out}.exit; setsid nohup bash -c '{bench}' > /dev/null 2>&1 < /dev/null & echo ok")
        while time.time() - began < 4 * 3600:
            time.sleep(30)
            try:
                for role in ['m1', *servers]:
                    ssh(role, f'touch {pair.HEARTBEAT}')
                if ssh('m1', f'cat {out}.exit 2>/dev/null || true').strip():
                    break
            except (RuntimeError, subprocess.TimeoutExpired):
                continue
        else:
            sys.exit(f'{label} did not finish in 4 hours')
        seconds = time.time() - began
        for role in ['m1', *servers]:
            ssh(role, f'sudo pkill -f "[c]pu_dma_latency" || true; kill $(cat {ROOT}/cpu-{label}.pid) || true')
        server_status = {r: ssh(r, f'cat {ROOT}/serve-{m}/ready.json; cat {ROOT}/serve-{m}/status.json 2>/dev/null || true')
                         for r in servers}
        for role in servers:
            ssh(role, f'pkill -f "[{BINARY[0]}]{BINARY[1:]} --mode serve" || true')
        local_dir = out_root / label
        pair_ok = fetch(label)
        for role in ['m1', *servers]:
            (local_dir / f'cpu-{role}.log').write_text(ssh(role, f'cat {ROOT}/cpu-{label}.log'))
        (local_dir / 'run.json').write_text(json.dumps(dict(
            label=label, machines=m, roles=['m1', *servers], instance_type='r6i.4xlarge', binary=BINARY,
            generation=json.loads(ssh('m1', f'cat {ROOT}/inputs/generation.json')), seconds=seconds,
            servers=server_status, fetched=pair_ok), indent=1))
        print(label, 'done', round(seconds), 's', flush=True)


def fetch(label):
    """Copy one run's directory and its stdout and stderr from m1."""
    local_dir = HERE / 'runs' / label
    local_dir.mkdir(parents=True, exist_ok=True)
    remote = subprocess.Popen([*_ssh_prefix('m1'), f'tar -C {ROOT}/runs -czf - {label} {label}.stdout {label}.stderr'],
                              stdout=subprocess.PIPE)
    local = subprocess.run(['tar', '-C', str(local_dir), '-xzf', '-'], stdin=remote.stdout)
    return remote.wait() == 0 and local.returncode == 0


def restart():
    """Start the stopped fleet again. Each machine keeps its host key, so its
    new public address is pinned to the key already recorded for its old one."""
    old = machines()
    ids = {r: m['InstanceId'] for r, m in old.items()}
    pair.lifecycle.identity()
    pair.aws('ec2', 'start-instances', '--instance-ids', *ids.values())
    for _ in range(120):
        found = {i['InstanceId']: i for r in pair.aws('ec2', 'describe-instances', '--instance-ids', *ids.values())['Reservations']
                 for i in r['Instances']}
        if all(i['State']['Name'] == 'running' and i.get('PublicIpAddress') for i in found.values()):
            break
        time.sleep(5)
    else:
        sys.exit('fleet did not reach running')
    keys = {l.split()[0]: l.split()[1:3] for l in pair.KNOWN_HOSTS.read_text().splitlines() if l.strip()}
    for role, i in ids.items():
        ip = found[i]['PublicIpAddress']
        if ip in keys:
            continue
        kind, key = keys[old[role]['PublicIpAddress']]
        for _ in range(30):
            scan = subprocess.run(['ssh-keyscan', '-T', '5', '-t', kind.removeprefix('ssh-'), ip],
                                  capture_output=True, text=True).stdout
            if any(l.split()[1:3] == [kind, key] for l in scan.splitlines() if l and not l.startswith('#')):
                break
            time.sleep(5)
        else:
            sys.exit(f'{role} at {ip} does not present its pinned key')
        with pair.KNOWN_HOSTS.open('a') as f:
            f.write(f'{ip} {kind} {key}\n')
    pair.prepare(NAME)


# Spread coordination: every machine serves its part and also runs a bench
# that coordinates its own stream of queries, scanning its part locally and
# every other part remotely. Each server then answers the other benches'
# requests, up to (M - 1) x 32 at once.
SPREAD_HANDLERS = 256
LOAD_REPEATS = 10
START_MARGIN_SECONDS = 10.0
# Load-pass records keep only these fields when fetched; the full records are
# about 10 KB each and the load pass writes 10,000 per configuration.
SLIM = ('import json,sys\n'
        'keep=("config","pass","query_index","query_id","seconds","finished_unix","recall_fraction")\n'
        'with open(sys.argv[2],"w") as out:\n'
        '  for line in open(sys.argv[1]):\n'
        '    r=json.loads(line)\n'
        '    if r["pass"]=="load": r={k:r.get(k) for k in keep}\n'
        '    out.write(json.dumps(r)+"\\n")\n')


def on_all(roles, command, timeout=300):
    """Run `command(role)` on every role at once; returns {role: stdout}."""
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(len(roles)) as pool:
        return dict(zip(roles, pool.map(lambda r: ssh(r, command(r), timeout=timeout), roles)))


def poll(roles, command, timeout=60):
    """`on_all` that returns None instead of raising on an SSH failure."""
    try:
        return on_all(roles, command, timeout)
    except (RuntimeError, subprocess.TimeoutExpired):
        return None


def spread(counts):
    """Aggregate throughput with every machine coordinating queries. For M
    machines, machine j serves part j and runs a bench over all 1,000 queries,
    rotated to start at query (j - 1) * 1000 / M so machines do not walk the
    same queries in lockstep, with part j local and the other parts remote. A
    pass barrier starts each pass on every machine at the same moment (m1's
    clock plus START_MARGIN_SECONDS; EC2 clocks are synchronized), and passes
    are cold, warm and a 32-thread load pass over the queries repeated
    LOAD_REPEATS times."""
    ips = {r: m['PrivateIpAddress'] for r, m in machines().items()}
    config = (HERE / 'configs' / 'fleet-100m.json').read_text()
    steps = len(json.loads(config)['configurations']) * 3
    all_roles = [f'm{j}' for j in range(1, max(counts) + 1)]
    on_all(all_roles, lambda r: f'mkdir -p {ROOT}/inputs {ROOT}/runs')
    for role in all_roles:
        if ssh(role, f'test -f {ROOT}/inputs/queries.jsonl && test -f {ROOT}/inputs/groundtruth.jsonl && echo y || true').strip() != 'y':
            s3_get(role, 'gt-dev-100000000.exact.jsonl', f'{ROOT}/inputs/groundtruth.jsonl')
            s3_get(role, 'queries-dev1000.jsonl', f'{ROOT}/inputs/queries.jsonl')
        ssh(role, f'cat > {ROOT}/inputs/config.json', stdin=config)
        ssh(role, f'cat > {ROOT}/cpu-sampler.sh; chmod +x {ROOT}/cpu-sampler.sh', stdin=SAMPLER)
        ssh(role, f'cat > {ROOT}/slim.py', stdin=SLIM)
    clocks = on_all(all_roles, lambda r: 'chronyc tracking | grep -E "System time|Leap status" | tr "\\n" " "')
    print('clocks', json.dumps(clocks), flush=True)
    for m in counts:
        label = f'fleet-spread-{m}machines'
        local_dir = HERE / 'runs' / label
        if local_dir.exists():
            print('exists', label)
            continue
        roles = all_roles[:m]
        # Nothing from an earlier, interrupted run may join this one.
        on_all(all_roles, lambda r: (f'pkill -f "[{BINARY[0]}]{BINARY[1:]} --mode bench" || true; '
                                     f'pkill -f "[{BINARY[0]}]{BINARY[1:]} --mode serve" || true; '
                                     f'pkill -f "[c]pu-sampler" || true; sudo pkill -f "[c]pu_dma_latency" || true; '
                                     f'touch {pair.HEARTBEAT}'))
        time.sleep(3)
        if m > 1:
            on_all(roles, lambda r: f'rm -rf {ROOT}/serve-spread-{m}')
            on_all(roles, lambda r: (
                f'cd {ROOT}; ulimit -n {OPEN_FILES}; setsid nohup ./{BINARY} --mode serve {common()} --parts {m} '
                f'--local-owners {r[1:]} --scan-handlers {SPREAD_HANDLERS} --token-file {TOKEN} '
                f'--scan-data-bind {ips[r]}:9200 --scan-control-bind {ips[r]}:9201 --output {ROOT}/serve-spread-{m} '
                f'> serve-spread-{m}.log 2>&1 < /dev/null & echo ok'))
            for role in roles:
                for _ in range(600):
                    state = ssh(role, f'test -f {ROOT}/serve-spread-{m}/ready.json && echo y; '
                                      f'pgrep -f "[{BINARY[0]}]{BINARY[1:]} --mode serve" > /dev/null || echo dead; true')
                    if 'y' in state.split():
                        break
                    if 'dead' in state:
                        sys.exit(f'{role} server died: ' + ssh(role, f'tail -c 1000 {ROOT}/serve-spread-{m}.log'))
                    time.sleep(3)
                else:
                    sys.exit(f'{role} did not become ready: ' + ssh(role, f'tail -c 1000 {ROOT}/serve-spread-{m}.log'))
            # Every machine must reach every other machine's scan ports.
            reach = on_all(roles, lambda r: ' '.join(
                f'$(timeout 3 bash -c "</dev/tcp/{ips[o]}/9200" 2>/dev/null && echo {o}:ok || echo {o}:refused)'
                for o in roles if o != r).join(['echo ', '']))
            if any('refused' in v for v in reach.values()):
                sys.exit(f'machines cannot reach each other: {reach}')
        hold = ("sudo setsid nohup python3 -c \"import os,struct,time; f=os.open('/dev/cpu_dma_latency', os.O_WRONLY); "
                "os.write(f, struct.pack('i', 10)); time.sleep(4*3600)\" > /dev/null 2>&1 < /dev/null &")
        out = f'{ROOT}/runs/{label}'
        barrier = f'{ROOT}/barrier-{label}'

        def bench(role):
            j = int(role[1:])
            shift = (j - 1) * 1000 // m
            others = ' '.join(f'--remote {r[1:]}@{ips[r]}:9200/{ips[r]}:9201' for r in roles if r != role)
            local = f'--local-owners {j} --token-file {TOKEN}' if m > 1 else ''
            command = (f'ulimit -n {OPEN_FILES}; SPANN_STORAGE_RUNTIME_WORKERS=16 ./{BINARY} --mode bench {common()} '
                       f'--parts {m} {local} {others} --queries {ROOT}/inputs/queries-{label}.jsonl '
                       f'--groundtruth {ROOT}/inputs/groundtruth.jsonl --config {ROOT}/inputs/config.json '
                       f'--passes cold,warm,load --load-repeats {LOAD_REPEATS} --pass-barrier {barrier} --cold-threads 32 '
                       f'--scan-handlers {SPREAD_HANDLERS} '
                       f'--output {out} --reader owners --no-diagnostics > {out}.stdout 2> {out}.stderr; echo $? > {out}.exit')
            return (f"{hold} rm -rf {barrier} {out} {out}.exit {out}.slim; mkdir -p {barrier} {ROOT}/runs; cd {ROOT}; "
                    f"(tail -n +{shift + 1} inputs/queries.jsonl; head -n {shift} inputs/queries.jsonl) > inputs/queries-{label}.jsonl; "
                    f"setsid nohup {ROOT}/cpu-sampler.sh > {ROOT}/cpu-{label}.log 2>&1 < /dev/null & echo $! > {ROOT}/cpu-{label}.pid; "
                    f"setsid nohup bash -c '{command}' > /dev/null 2>&1 < /dev/null & echo ok")
        began = time.time()
        on_all(roles, bench)
        alive = (f'test -s {out}.exit && echo "exit=$(cat {out}.exit)"; '
                 f'pgrep -f "[{BINARY[0]}]{BINARY[1:]} --mode bench" > /dev/null || echo dead; ')
        failed = None
        margins = []
        last_touch = time.time()

        def check(state):
            # A bench that exited 0 is no longer running, which is fine; one
            # that is gone without an exit code, or exited non-zero, failed.
            bad = {r: v for r, v in state.items()
                   if ('exit=' in v and 'exit=0' not in v.split()) or ('dead' in v and 'exit=' not in v)}
            return f'bench stopped: {bad}' if bad else None

        for step in range(steps):
            while True:
                if time.time() - began > 4 * 3600:
                    failed = 'deadline of 4 hours passed'
                    break
                state = poll(roles, lambda r: alive + f'test -f {barrier}/ready-{step} && echo ready; true')
                if state is None:
                    time.sleep(3)
                    continue
                failed = check(state)
                if failed or all('ready' in v for v in state.values()):
                    break
                if time.time() - last_touch > 60:
                    poll(roles, lambda r: f'touch {pair.HEARTBEAT}')
                    last_touch = time.time()
                time.sleep(2)
            if failed:
                break
            start = float(ssh('m1', 'date +%s.%N')) + START_MARGIN_SECONDS
            on_all(roles, lambda r: f'echo {start:.3f} > {barrier}/go-{step}.tmp && mv {barrier}/go-{step}.tmp {barrier}/go-{step}')
            margins.append(round(start - float(ssh('m1', 'date +%s.%N')), 2))
        while not failed:
            if time.time() - began > 4 * 3600:
                failed = 'deadline of 4 hours passed'
                break
            state = poll(roles, lambda r: alive + f'touch {pair.HEARTBEAT}')
            if state is None:
                time.sleep(5)
                continue
            if all('exit=' in v for v in state.values()):
                failed = check(state)
                break
            failed = check(state)
            time.sleep(10)
        seconds = time.time() - began
        on_all(roles, lambda r: f'sudo pkill -f "[c]pu_dma_latency" || true; kill $(cat {ROOT}/cpu-{label}.pid) || true; '
                                f'pkill -f "[{BINARY[0]}]{BINARY[1:]} --mode bench" || true')
        servers = {}
        if m > 1:
            servers = on_all(roles, lambda r: f'cat {ROOT}/serve-spread-{m}/ready.json; cat {ROOT}/serve-spread-{m}/status.json 2>/dev/null || true; '
                                              f'grep -v "^{{" {ROOT}/serve-spread-{m}.log | tail -5 || true')
            on_all(roles, lambda r: f'pkill -f "[{BINARY[0]}]{BINARY[1:]} --mode serve" || true')
        # Slim the load-pass records on each machine before fetching.
        on_all(roles, lambda r: f'test -f {out}/queries.jsonl && python3 {ROOT}/slim.py {out}/queries.jsonl {out}/queries.slim && '
                                f'mv {out}/queries.slim {out}/queries.jsonl; true', timeout=1800)
        local_dir.mkdir(parents=True)
        fetched = {}
        for role in roles:
            d = local_dir / role
            d.mkdir()
            remote = subprocess.Popen([*_ssh_prefix(role), f'tar -C {ROOT}/runs -czf - {label} {label}.stdout {label}.stderr {label}.exit'],
                                      stdout=subprocess.PIPE)
            local = subprocess.run(['tar', '-C', str(d), '-xzf', '-'], stdin=remote.stdout)
            fetched[role] = remote.wait() == 0 and local.returncode == 0
            (d / 'cpu.log').write_text(ssh(role, f'cat {ROOT}/cpu-{label}.log'))
        (local_dir / 'run.json').write_text(json.dumps(dict(
            label=label, machines=m, roles=roles, instance_type='r6i.4xlarge', binary=BINARY, load_repeats=LOAD_REPEATS,
            scan_handlers=SPREAD_HANDLERS, start_margins_seconds=margins, clocks=clocks, seconds=seconds, failed=failed,
            servers=servers, fetched=fetched), indent=1))
        print(label, 'failed: ' + failed if failed else 'done', round(seconds), 's', flush=True)


def _ssh_prefix(role):
    ip = machines()[role]['PublicIpAddress']
    return ['ssh', '-i', pair.KEY, '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
            '-o', f'UserKnownHostsFile={pair.KNOWN_HOSTS}', f'ubuntu@{ip}']


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('action', choices=['launch', 'restart', 'stage', 'run', 'spread', 'fetch', 'stop'])
    p.add_argument('--machines', type=int, nargs='+', default=[1, 2, 4, 8])
    p.add_argument('--hours', type=float, default=6)
    a = p.parse_args()
    if a.action == 'launch':
        pair.launch(NAME, tuple(ROLES), 'r6i.4xlarge', 64, a.hours)
    elif a.action == 'restart':
        restart()
    elif a.action == 'fetch':
        for m in a.machines:
            print(m, fetch(f'fleet-100m-{m}machines'))
    elif a.action == 'stage':
        stage()
    elif a.action == 'run':
        run(a.machines)
    elif a.action == 'spread':
        spread(a.machines)
    else:
        pair.stop(NAME)


if __name__ == '__main__':
    main()
