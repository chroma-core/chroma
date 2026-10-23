#!/usr/bin/env python3
"""Explicitly invoked fixed-window controls. Preparing this file starts nothing."""
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
INSTANCE = 'i-011ff180f03584f40'
ACCOUNT = '079819354890'
VOLUME = 'vol-08bda85902f848a75'
RATE = 4.032
ENV = dict(os.environ, AWS_CONFIG_FILE=str(ROOT/'aws-config'), KUBECONFIG='/Users/chroma/.config/cops/kube/config', AWS_PAGER='')
AWS = ['/opt/homebrew/bin/aws', '--profile', 'spann-sandbox', '--region', 'us-east-1']

def write(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')

def aws(*args):
    r = subprocess.run(AWS+list(args), env=ENV, capture_output=True, text=True, timeout=45)
    if r.returncode:
        raise RuntimeError(r.stderr[-4000:])
    return json.loads(r.stdout)

def identity():
    data = aws('sts', 'get-caller-identity')
    if data['Account'] != ACCOUNT:
        raise RuntimeError('Unexpected AWS account')
    return data

def instance():
    return aws('ec2', 'describe-instances', '--instance-ids', INSTANCE)['Reservations'][0]['Instances'][0]

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['start', 'watch', 'stop'])
    p.add_argument('--window', required=True, type=Path)
    p.add_argument('--admission', type=Path)
    p.add_argument('--deadline', type=float)
    args = p.parse_args()
    window = args.window.resolve()
    if args.action == 'watch':
        if args.deadline is None:
            raise RuntimeError('Watch requires absolute deadline')
        write(window/'watch-ready.json', {'pid':os.getpid(), 'deadline':args.deadline})
        time.sleep(max(0, args.deadline-time.time()))
        for attempt in range(5):
            try:
                identity()
                current = instance()
                if current['State']['Name'] != 'stopped':
                    write(window/'deadline-stop-response.json', aws('ec2', 'stop-instances', '--instance-ids', INSTANCE))
                write(window/'deadline-stop-readback.json', instance())
                return
            except Exception as e:
                write(window/'deadline-error.json', {'attempt':attempt,'error':str(e)})
                time.sleep(10)
        raise RuntimeError('Absolute shutdown retries exhausted')
    if args.action == 'stop':
        identity()
        write(window/'stop-response.json', aws('ec2', 'stop-instances', '--instance-ids', INSTANCE))
        for _ in range(60):
            current = instance()
            write(window/'stop-readback.json', current)
            if current['State']['Name'] == 'stopped':
                receipt = json.loads((window/'start-intent.json').read_text())
                elapsed = time.time()-receipt['observed_start_unix']
                write(window/'closed.json', {'api_state':'stopped','observed_unix':time.time(),'compute_seconds_upper_bound':elapsed,'compute_usd_upper_bound':elapsed*RATE/3600,'additional_storage_monthly_usd':0,'watchdog_retirement_safe':True})
                return
            time.sleep(2)
        raise RuntimeError('API stopped state not established')
    if args.admission is None:
        raise RuntimeError('Start requires root-frozen reviewed admission manifest')
    admission = json.loads(args.admission.read_text())
    if admission.get('root_authorized_execution') is not True or admission.get('combined_review_passed') is not True:
        raise RuntimeError('No frozen reviewed execution authorization')
    for item in admission['local_artifacts']:
        path = Path(item['path'])
        if not path.is_absolute() or digest(path) != item['sha256']:
            raise RuntimeError(f'Artifact hash mismatch: {path}')
    who = identity()
    i = instance()
    tags = {v['Key']:v['Value'] for v in i.get('Tags',[])}
    if i['State']['Name']!='stopped' or i['InstanceType']!='r6i.16xlarge' or tags.get('purpose')!='hierarchical-spann-sharding':
        raise RuntimeError('Instance does not match stopped dedicated approved worker')
    vols = [v['Ebs']['VolumeId'] for v in i['BlockDeviceMappings']]
    if vols != [VOLUME]:
        raise RuntimeError('Preserved volume binding changed')
    volume = aws('ec2','describe-volumes','--volume-ids',VOLUME)['Volumes'][0]
    if volume['Size']!=256 or volume['VolumeType']!='gp3':
        raise RuntimeError('Volume capacity/type changed')
    window.mkdir(parents=True, exist_ok=False)
    now = time.time()
    deadline = now+5400
    log = (window/'watch.log').open('a')
    child = subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'watch','--window',str(window),'--deadline',str(deadline)], env=ENV,stdout=log,stderr=log,start_new_session=True)
    for _ in range(50):
        if (window/'watch-ready.json').exists(): break
        if child.poll() is not None: raise RuntimeError('Watchdog failed')
        time.sleep(.1)
    if child.poll() is not None or not (window/'watch-ready.json').exists():
        raise RuntimeError('Watchdog unavailable')
    write(window/'start-intent.json', {'identity':who,'instance':i,'volume':volume,'observed_start_unix':now,'deadline_unix':deadline,'deadline_utc':dt.datetime.fromtimestamp(deadline,dt.timezone.utc).isoformat(),'rate_usd_hour':RATE,'compute_cap_usd':6.20,'nominal_90min_compute_usd':6.048,'watchdog_pid':child.pid,'admission_path':str(args.admission.resolve()),'admission_sha256':digest(args.admission),'remote_guard_required_before_upload':True})
    write(window/'start-response.json', aws('ec2','start-instances','--instance-ids',INSTANCE))
    print(json.dumps({'instance':INSTANCE,'deadline':deadline,'watchdog':child.pid,'window':str(window)}))

if __name__ == '__main__': main()
