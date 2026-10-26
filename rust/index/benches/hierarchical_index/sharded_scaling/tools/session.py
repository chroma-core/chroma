"""Start or stop the two reader-development machines under a long, idle-aware guard.

A session runs both admitted machines for up to twelve hours. Each machine gets
two stops before any work runs on it: a hard stop at the session deadline, and
an idle stop when the bench heartbeat file is more than three hours old. The
existing local watchdog (`lifecycle.py watch`) also stops both machines at the
deadline while this Mac is awake. Starting checks the same account, tags,
volumes, hardware and stop-on-shutdown behavior as `lifecycle.py start`.

After a start, the machines have new public addresses. The script refreshes
`reader-fixtures35/hosts.json` and pins each machine's new address to the same
SSH host key it had before, so a changed key fails the start.

Usage: python3 session.py start | stop --window DIR
"""
import argparse
import datetime as dt
import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
OPS = HERE.parent
sys.path.insert(0, str(OPS))
import lifecycle  # noqa: E402

FIXTURES = OPS / 'reader-fixtures35'
KNOWN_HOSTS = Path('<known-hosts-file>')
KEY = '<ssh-key.pem>'
RATES = {'<coordinator-instance>': 4.032, '<peer-instance>': 4.032}
SESSION_SECONDS = 12 * 3600
IDLE_SECONDS = 3 * 3600
HEARTBEAT = '/home/ubuntu/spann-two-machine-v1/reader-dev/heartbeat'

GUARD = r'''set -euo pipefail
deadline={deadline}; window={window}; instance={instance}
token=$(curl --noproxy '*' -fsS --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
doc=$(curl --noproxy '*' -fsS --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document)
python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["accountId"]=="<aws-account-id>" and d["instanceId"]==sys.argv[1] and d["region"]=="us-east-1"' "$instance" <<< "$doc"
mkdir -p "$(dirname {heartbeat})"; touch {heartbeat}
sudo tee /usr/local/sbin/spann-reader-idle-stop >/dev/null <<'EOF'
#!/bin/sh
age=$(( $(date +%s) - $(stat -c %Y {heartbeat} 2>/dev/null || echo 0) ))
[ "$age" -gt {idle} ] && /sbin/shutdown -h now
exit 0
EOF
sudo chmod 755 /usr/local/sbin/spann-reader-idle-stop
sudo systemd-run --unit="spann-reader-$window-stop" --on-calendar="@$deadline" --timer-property=AccuracySec=1s /sbin/shutdown -h now
sudo systemd-run --unit="spann-reader-$window-idle" --on-active=10min --on-unit-active=10min --timer-property=AccuracySec=30s /usr/local/sbin/spann-reader-idle-stop
sudo systemctl is-active --quiet "spann-reader-$window-stop.timer"
sudo systemctl is-active --quiet "spann-reader-$window-idle.timer"
sudo systemctl list-timers --all --no-pager 'spann-reader-*'
'''


def pinned_keys():
    keys = {}
    for line in KNOWN_HOSTS.read_text().splitlines():
        host, kind, key = line.split()[:3]
        keys[host] = (kind, key)
    return keys


def pin(instance, old_ip, new_ip):
    kind, key = pinned_keys()[old_ip]
    scan = ''
    for _ in range(30):
        r = subprocess.run(['ssh-keyscan', '-T', '5', '-t', kind, new_ip], capture_output=True, text=True)
        scan = r.stdout
        if key in scan:
            break
        time.sleep(5)
    if not any(line.split()[1:3] == [kind, key] for line in scan.splitlines() if not line.startswith('#')):
        raise RuntimeError(f'{instance} at {new_ip} does not present its pinned {kind} key')
    if new_ip not in pinned_keys():
        with KNOWN_HOSTS.open('a') as f:
            f.write(f'{new_ip} {kind} {key}\n')


def ssh(ip, cmd, timeout=120):
    r = subprocess.run(['ssh', '-i', KEY, '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                        '-o', f'UserKnownHostsFile={KNOWN_HOSTS}', f'ubuntu@{ip}', cmd],
                       capture_output=True, text=True, timeout=timeout)
    if r.returncode:
        raise RuntimeError(f'{ip}: {r.stderr[-2000:]}')
    return r.stdout


def start():
    who = lifecycle.identity()
    before = lifecycle.instances()
    for name, instance in before.items():
        tags = {t['Key']: t['Value'] for t in instance.get('Tags', [])}
        volumes = {v['Ebs']['VolumeId'] for v in instance['BlockDeviceMappings']}
        if (instance['State']['Name'] != 'stopped' or instance['InstanceType'] != 'r6i.16xlarge'
                or tags.get('purpose') != 'hierarchical-spann-sharding' or tags.get('owner') != 'lyon'
                or volumes != lifecycle.BINDINGS[name]):
            raise RuntimeError(f'identity, hardware, stopped state or volumes differ: {name}')
        behavior = lifecycle.aws('ec2', 'describe-instance-attribute', '--instance-id', name,
                                 '--attribute', 'instanceInitiatedShutdownBehavior')
        if behavior.get('InstanceInitiatedShutdownBehavior', {}).get('Value') != 'stop':
            raise RuntimeError(f'OS shutdown must stop, not terminate: {name}')
    started = time.time()
    window_id = f'reader-dev-{int(started)}'
    window = OPS / f'window-{window_id}'
    window.mkdir()
    deadline = int(started + SESSION_SECONDS)
    old_hosts = json.loads((FIXTURES / 'hosts.json').read_text())
    lifecycle.write(window / 'start-intent.json', {
        'identity': who, 'instances': before, 'observed_start_unix': started, 'deadline_unix': deadline,
        'deadline_utc': dt.datetime.fromtimestamp(deadline, dt.timezone.utc).isoformat(),
        'idle_stop_seconds': IDLE_SECONDS, 'rates_usd_hour': RATES,
        'compute_cap_usd': SESSION_SECONDS * sum(RATES.values()) / 3600})
    with (window / 'watch.log').open('a') as log:
        child = subprocess.Popen([sys.executable, str(OPS / 'lifecycle.py'), 'watch', '--window', str(window),
                                  '--deadline', str(deadline)], env=lifecycle.ENV, stdout=log, stderr=log,
                                 start_new_session=True)
    for _ in range(50):
        if (window / 'watch-ready.json').exists() or child.poll() is not None:
            break
        time.sleep(.1)
    if child.poll() is not None:
        raise RuntimeError('local watchdog unavailable; nothing started')
    lifecycle.write(window / 'watch-pid.json', {'pid': child.pid, 'deadline': deadline})
    lifecycle.write(window / 'start-response.json',
                    lifecycle.aws('ec2', 'start-instances', '--instance-ids', *lifecycle.BINDINGS))
    for _ in range(120):
        running = lifecycle.instances()
        if all(i['State']['Name'] == 'running' and i.get('PublicIpAddress') for i in running.values()):
            break
        time.sleep(5)
    else:
        raise RuntimeError('machines did not reach running with public addresses')
    lifecycle.write(window / 'running-readback.json', running)
    for name, instance in running.items():
        pin(name, old_hosts[name]['PublicIpAddress'], instance['PublicIpAddress'])
    lifecycle.write(FIXTURES / 'hosts.json', running)
    guards = {}
    for name, instance in running.items():
        script = GUARD.format(deadline=deadline, window=window_id, instance=name, heartbeat=HEARTBEAT,
                              idle=IDLE_SECONDS)
        for attempt in range(12):
            try:
                guards[name] = ssh(instance['PublicIpAddress'], 'bash -s <<\'GUARD_EOF\'\n' + script + 'GUARD_EOF')
                break
            except RuntimeError:
                if attempt == 11:
                    raise
                time.sleep(10)
    lifecycle.write(window / 'guards.json', guards)
    print(json.dumps({'window': str(window), 'deadline_utc': dt.datetime.fromtimestamp(deadline, dt.timezone.utc).isoformat(),
                      'hosts': {k: v['PublicIpAddress'] for k, v in running.items()}}))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('action', choices=['start', 'stop'])
    p.add_argument('--window', type=Path)
    a = p.parse_args()
    if a.action == 'start':
        start()
    else:
        if not a.window or not (a.window / 'start-intent.json').exists():
            sys.exit('stop needs --window with a start-intent.json')
        lifecycle.stop(a.window.resolve(), 'stop')


if __name__ == '__main__':
    main()
