"""Launch, prepare, watch or stop an extra pair of benchmark machines.

An extra pair runs isolated reader benchmarks beside the original two
machines. Each launch creates two r6i.16xlarge instances from the same image,
subnet, security groups, key and instance profile as the original pair, tagged
`purpose=hierarchical-spann-reader-bench`. The script trusts each new machine's
SSH host key only after matching it to the fingerprint the machine printed in
its EC2 console output. Each machine then gets the same two stops as the
original pair: an idle stop after three hours without a bench heartbeat and a
hard stop at the session deadline. A local watchdog also stops the pair at the
deadline while this Mac is awake.

The pair is described in `pairs/<name>.json` with the same `c` (query
machine) and `p` (peer) roles that `run-bench.py --pair <name>` uses.

A launch can instead create a single machine of another type with a larger
root volume, for example the build machine for the 100M FineWeb ladder:
  python3 pair.py launch --name builder --roles c --type r6i.32xlarge --volume-gb 1400 --hours 20

Usage:
  python3 pair.py launch --name pair2
  python3 pair.py prepare --name pair2   # finish setup of a launched pair
  python3 pair.py stop --name pair2
"""
import argparse
import datetime as dt
import json
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
OPS = HERE.parent
sys.path.insert(0, str(OPS))
sys.path.insert(0, str(HERE))
import lifecycle  # noqa: E402

KNOWN_HOSTS = Path('<known-hosts-file>')
KEY = '<ssh-key.pem>'
TEMPLATE = '<coordinator-instance>'
RATE = 4.032
SESSION_SECONDS = 12 * 3600
IDLE_SECONDS = 3 * 3600
HEARTBEAT = '/home/ubuntu/spann-two-machine-v1/reader-dev/heartbeat'
TOKEN = '/home/ubuntu/spann-two-machine-v1/epoch1/run-auth.token'
PURPOSE = 'hierarchical-spann-reader-bench'


def aws(*args):
    return lifecycle.aws(*args)


def ssh(ip, cmd, timeout=120, stdin=None):
    r = subprocess.run(['ssh', '-i', KEY, '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                        '-o', f'UserKnownHostsFile={KNOWN_HOSTS}', f'ubuntu@{ip}', cmd],
                       capture_output=True, text=True, timeout=timeout, input=stdin)
    if r.returncode:
        raise RuntimeError(f'{ip}: {r.stderr[-2000:]}')
    return r.stdout


def console_fingerprints(instance):
    """Host key fingerprints the instance printed while cloud-init generated
    its keys: a fingerprint line, then a randomart header naming the key type."""
    for _ in range(60):
        out = aws('ec2', 'get-console-output', '--instance-id', instance, '--latest').get('Output') or ''
        found, fp = set(), None
        for line in out.splitlines():
            m = re.search(r'(SHA256:[A-Za-z0-9+/=]+) root@', line)
            if m:
                fp = m.group(1)
                continue
            m = re.search(r'\+-+\[(\w+) \d+\]-+\+', line)
            if m and fp:
                found.add((fp, m.group(1).upper()))
                fp = None
        if any(kind == 'ED25519' for _, kind in found):
            return found
        time.sleep(10)
    raise RuntimeError(f'{instance} printed no host key fingerprints')


def pin(instance, ip):
    expected = console_fingerprints(instance)
    for _ in range(30):
        scan = subprocess.run(['ssh-keyscan', '-T', '5', '-t', 'ed25519', ip], capture_output=True, text=True).stdout
        lines = [l for l in scan.splitlines() if l and not l.startswith('#')]
        if lines:
            break
        time.sleep(5)
    else:
        raise RuntimeError(f'{ip} did not answer ssh-keyscan')
    line = lines[0]
    fp = subprocess.run(['ssh-keygen', '-lf', '-'], input=line, capture_output=True, text=True).stdout.split()[1]
    if (fp, 'ED25519') not in expected:
        raise RuntimeError(f'{instance} at {ip} presents {fp}, which its console output does not list')
    with KNOWN_HOSTS.open('a') as f:
        f.write(line.strip() + '\n')


RATES = {'r6i.16xlarge': 4.032, 'r6i.32xlarge': 8.064, 'r6i.4xlarge': 1.008, 'r6i.8xlarge': 2.016}


def launch(name, roles=('c', 'p'), itype='r6i.16xlarge', volume_gb=64, hours=SESSION_SECONDS / 3600):
    pair_file = HERE / 'pairs' / f'{name}.json'
    if pair_file.exists() and 'PublicIpAddress' in json.dumps(json.loads(pair_file.read_text())):
        sys.exit(f'{pair_file} exists')
    pair_file.parent.mkdir(exist_ok=True)
    who = lifecycle.identity()
    t = lifecycle.instances()[TEMPLATE]
    started = time.time()
    deadline = int(started + hours * 3600)
    window = OPS / f'window-reader-dev-{name}-{int(started)}'
    window.mkdir()
    ids = {}
    for role in roles:
        tags = [{'Key': 'Name', 'Value': f'reader-bench-{name}-{role}'}, {'Key': 'owner', 'Value': 'lyon'},
                {'Key': 'purpose', 'Value': PURPOSE}, {'Key': 'pair', 'Value': name}, {'Key': 'role', 'Value': role}]
        r = aws('ec2', 'run-instances', '--image-id', t['ImageId'], '--instance-type', itype,
                '--key-name', t['KeyName'], '--subnet-id', t['SubnetId'],
                '--security-group-ids', *[g['GroupId'] for g in t['SecurityGroups']],
                '--iam-instance-profile', f'Arn={t["IamInstanceProfile"]["Arn"]}',
                '--instance-initiated-shutdown-behavior', 'stop',
                '--block-device-mappings', json.dumps([{'DeviceName': '/dev/sda1', 'Ebs': {
                    'VolumeSize': volume_gb, 'VolumeType': 'gp3', 'Iops': 16000 if volume_gb > 64 else 3000,
                    'Throughput': 1000 if volume_gb > 64 else 125, 'DeleteOnTermination': True}}]),
                '--metadata-options', 'HttpTokens=required',
                '--tag-specifications', json.dumps([{'ResourceType': 'instance', 'Tags': tags},
                                                    {'ResourceType': 'volume', 'Tags': tags}]),
                '--count', '1')
        ids[role] = r['Instances'][0]['InstanceId']
        # Record IDs at once, so the watchdog and `stop` can find them even if
        # the rest of the launch fails.
        lifecycle.write(pair_file, {k: {'InstanceId': v} for k, v in ids.items()})
    intent = {'identity': who, 'pair': name, 'instances': ids, 'observed_start_unix': started, 'deadline_unix': deadline,
              'deadline_utc': dt.datetime.fromtimestamp(deadline, dt.timezone.utc).isoformat(),
              'idle_stop_seconds': IDLE_SECONDS, 'instance_type': itype, 'volume_gb': volume_gb,
              'rates_usd_hour': {i: RATES[itype] for i in ids.values()},
              'compute_cap_usd': hours * RATES[itype] * len(ids)}
    lifecycle.write(window / 'start-intent.json', intent)
    with (window / 'watch.log').open('a') as log:
        subprocess.Popen([sys.executable, str(Path(__file__).resolve()), 'watch', '--name', name,
                          '--deadline', str(deadline)], stdout=log, stderr=log, start_new_session=True)
    prepare(name)



def prepare(name):
    """Wait for a launched pair, pin its host keys, and install guards, the
    shared token and run directories. Safe to rerun on the same pair."""
    pair_file = HERE / 'pairs' / f'{name}.json'
    ids = {role: m['InstanceId'] for role, m in json.loads(pair_file.read_text()).items()}
    window = sorted(OPS.glob(f'window-reader-dev-{name}-*'))[-1]
    intent = json.loads((window / 'start-intent.json').read_text())
    deadline, started = intent['deadline_unix'], intent['observed_start_unix']
    for _ in range(120):
        found = {i['InstanceId']: i for r in aws('ec2', 'describe-instances', '--instance-ids', *ids.values())['Reservations']
                 for i in r['Instances']}
        if all(i['State']['Name'] == 'running' and i.get('PublicIpAddress') for i in found.values()):
            break
        time.sleep(5)
    else:
        raise RuntimeError('new instances did not reach running')
    pair = {role: {'InstanceId': i, 'PublicIpAddress': found[i]['PublicIpAddress'],
                   'PrivateIpAddress': found[i]['PrivateIpAddress']} for role, i in ids.items()}
    for role, m in pair.items():
        if not subprocess.run(['ssh-keygen', '-F', m['PublicIpAddress'], '-f', str(KNOWN_HOSTS)], capture_output=True).stdout:
            pin(m['InstanceId'], m['PublicIpAddress'])
    token = subprocess.run(['openssl', 'rand', '-hex', '32'], capture_output=True, text=True, check=True).stdout
    guard = (Path(HERE / 'session.py').read_text().split("GUARD = r'''")[1].split("'''")[0])
    for role, m in pair.items():
        ip = m['PublicIpAddress']
        script = guard.format(deadline=deadline, window=f'{name}-{int(started)}', instance=m['InstanceId'],
                              heartbeat=HEARTBEAT, idle=IDLE_SECONDS)
        for attempt in range(12):
            try:
                ssh(ip, "bash -s <<'GUARD_EOF'\n" + script + 'GUARD_EOF')
                break
            except RuntimeError:
                if attempt == 11:
                    raise
                time.sleep(10)
        ssh(ip, f'mkdir -p $(dirname {TOKEN}) /home/ubuntu/spann-two-machine-v1/reader-dev/bin && umask 077 && cat > {TOKEN}',
            stdin=token)
        ssh(ip, 'sudo install -d -o ubuntu -g ubuntu /run/spann-reader-dev /run/spann-reader35-input')
    lifecycle.write(pair_file, pair)
    print(json.dumps({'pair': name, 'window': str(window), 'deadline_utc': intent['deadline_utc'], 'machines': pair}))


def watch(name, deadline):
    time.sleep(max(0, deadline - time.time()))
    stop(name)


def stop(name):
    pair = json.loads((HERE / 'pairs' / f'{name}.json').read_text())
    ids = [m['InstanceId'] for m in pair.values()]
    lifecycle.identity()
    print(json.dumps(aws('ec2', 'stop-instances', '--instance-ids', *ids)))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('action', choices=['launch', 'prepare', 'watch', 'stop'])
    p.add_argument('--name', required=True)
    p.add_argument('--deadline', type=float)
    p.add_argument('--roles', default='c,p')
    p.add_argument('--type', default='r6i.16xlarge', choices=sorted(RATES))
    p.add_argument('--volume-gb', type=int, default=64)
    p.add_argument('--hours', type=float, default=SESSION_SECONDS / 3600)
    a = p.parse_args()
    if a.action == 'launch':
        launch(a.name, tuple(a.roles.split(',')), a.type, a.volume_gb, a.hours)
    elif a.action == 'prepare':
        prepare(a.name)
    elif a.action == 'watch':
        watch(a.name, a.deadline)
    else:
        stop(a.name)


if __name__ == '__main__':
    main()
