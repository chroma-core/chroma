#!/usr/bin/env python3
"""Control one explicitly admitted two-machine window; importing starts nothing."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
ACCOUNT = '079819354890'
BINDINGS = {
    'i-011ff180f03584f40': {'vol-08bda85902f848a75'},
    'i-00268d1e1dfea0357': {'vol-07951d3573187a716', 'vol-06c673c76a50340f8'},
}
ENV = dict(os.environ, AWS_CONFIG_FILE=str(ROOT/'aws-config'), KUBECONFIG='/Users/chroma/.config/cops/kube/config', AWS_PAGER='')
AWS = ['/opt/homebrew/bin/aws', '--profile', 'spann-sandbox', '--region', 'us-east-1']

def write(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')

def aws(*args):
    result = subprocess.run(AWS+list(args), env=ENV, capture_output=True, text=True, timeout=45)
    if result.returncode:
        raise RuntimeError(result.stderr[-4000:])
    return json.loads(result.stdout)

def identity():
    who = aws('sts', 'get-caller-identity')
    if who['Account'] != ACCOUNT:
        raise RuntimeError('Unexpected AWS account')
    return who

def instances():
    data = aws('ec2', 'describe-instances', '--instance-ids', *BINDINGS)
    found = {i['InstanceId']: i for r in data['Reservations'] for i in r['Instances']}
    if set(found) != set(BINDINGS):
        raise RuntimeError('Task instance set changed')
    return found

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def stop(window, prefix):
    identity()
    write(window/f'{prefix}-response.json', aws('ec2', 'stop-instances', '--instance-ids', *BINDINGS))
    for _ in range(90):
        observed = instances()
        write(window/f'{prefix}-readback.json', observed)
        if all(i['State']['Name'] == 'stopped' for i in observed.values()):
            receipt = json.loads((window/'start-intent.json').read_text())
            elapsed = max(0, time.time()-receipt['observed_start_unix'])
            write(window/'closed.json', {'api_states': {k:'stopped' for k in BINDINGS}, 'observed_unix':time.time(), 'elapsed_seconds_upper_bound':elapsed, 'compute_usd_upper_bound':elapsed*sum(receipt['rates_usd_hour'].values())/3600, 'additional_storage_monthly_usd':0, 'watchdog_retirement_safe':True})
            return
        time.sleep(2)
    raise RuntimeError('Both API stopped states were not established')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['start', 'watch', 'stop'])
    parser.add_argument('--window', required=True, type=Path)
    parser.add_argument('--admission', type=Path)
    parser.add_argument('--deadline', type=float)
    args = parser.parse_args()
    window = args.window.resolve()
    if args.action == 'watch':
        if args.deadline is None:
            raise RuntimeError('Watch requires an absolute deadline')
        write(window/'watch-ready.json', {'pid':os.getpid(), 'deadline':args.deadline})
        time.sleep(max(0, args.deadline-time.time()))
        if (window/'closed.json').exists():
            return
        for attempt in range(5):
            try:
                stop(window, 'deadline-stop')
                return
            except Exception as error:
                write(window/f'deadline-error-{attempt}.json', {'error':str(error)})
                time.sleep(10)
        raise RuntimeError('Absolute shutdown retries exhausted')
    if args.action == 'stop':
        if not (window/'start-intent.json').exists():
            raise RuntimeError('Unknown execution window')
        stop(window, 'stop')
        return
    if args.admission is None:
        raise RuntimeError('Start requires a frozen reviewed admission')
    admission = json.loads(args.admission.read_text())
    if not admission.get('root_authorized_execution') or not admission.get('combined_review_passed'):
        raise RuntimeError('Execution has not passed admission')
    seconds = admission['window_seconds']
    if not isinstance(seconds, int) or not 1 <= seconds <= 5400:
        raise RuntimeError('Invalid fixed window bound')
    rates = admission['rates_usd_hour']
    if set(rates) != set(BINDINGS) or any(not 0 < rate <= 10 for rate in rates.values()):
        raise RuntimeError('Missing or invalid task-specific rates')
    if seconds*sum(rates.values())/3600 > admission['compute_cap_usd']:
        raise RuntimeError('Window exceeds its admitted compute bound')
    if not admission.get('local_artifacts'):
        raise RuntimeError('No frozen artifacts')
    for item in admission['local_artifacts']:
        path = Path(item['path'])
        if not path.is_absolute() or digest(path) != item['sha256']:
            raise RuntimeError(f'Artifact hash mismatch: {path}')
    who = identity()
    before = instances()
    for name, instance in before.items():
        tags = {t['Key']:t['Value'] for t in instance.get('Tags', [])}
        volumes = {v['Ebs']['VolumeId'] for v in instance['BlockDeviceMappings']}
        if instance['State']['Name'] != 'stopped' or instance['InstanceType'] != 'r6i.16xlarge' or tags.get('purpose') != 'hierarchical-spann-sharding' or tags.get('owner') != 'lyon' or volumes != BINDINGS[name]:
            raise RuntimeError(f'Task identity, hardware, stopped state or preserved volumes differ: {name}')
    shutdown_attributes = {}
    for name in BINDINGS:
        attribute = aws('ec2', 'describe-instance-attribute', '--instance-id', name, '--attribute', 'instanceInitiatedShutdownBehavior')
        if attribute.get('InstanceId') != name or attribute.get('InstanceInitiatedShutdownBehavior', {}).get('Value') != 'stop':
            raise RuntimeError(f'OS shutdown must preserve the instance and disks: {name}')
        shutdown_attributes[name] = attribute
    window.mkdir(parents=True, exist_ok=False)
    write(window/'shutdown-behavior.json', shutdown_attributes)
    started = time.time()
    deadline = started+seconds
    write(window/'start-intent.json', {'identity':who, 'instances':before, 'observed_start_unix':started, 'deadline_unix':deadline, 'deadline_utc':dt.datetime.fromtimestamp(deadline, dt.timezone.utc).isoformat(), 'rates_usd_hour':rates, 'compute_cap_usd':admission['compute_cap_usd'], 'admission_path':str(args.admission.resolve()), 'admission_sha256':digest(args.admission), 'remote_guards_required_before_upload':True})
    with (window/'watch.log').open('a') as log:
        child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), 'watch', '--window', str(window), '--deadline', str(deadline)], env=ENV, stdout=log, stderr=log, start_new_session=True)
    for _ in range(50):
        if (window/'watch-ready.json').exists() or child.poll() is not None:
            break
        time.sleep(.1)
    if child.poll() is not None or not (window/'watch-ready.json').exists():
        raise RuntimeError('Local watchdog unavailable; no start attempted')
    write(window/'watch-pid.json', {'pid':child.pid, 'deadline':deadline})
    write(window/'start-response.json', aws('ec2', 'start-instances', '--instance-ids', *BINDINGS))
    print(json.dumps({'instances':list(BINDINGS), 'deadline':deadline, 'watchdog':child.pid, 'window':str(window)}))

if __name__ == '__main__':
    main()
