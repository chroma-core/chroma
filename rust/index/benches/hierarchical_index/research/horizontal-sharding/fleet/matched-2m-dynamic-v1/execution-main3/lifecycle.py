"""One existing sandbox driver, fresh fixed two-hour deadline, independent stop process."""
import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
INSTANCE = 'i-00268d1e1dfea0357'
ENV = dict(os.environ, AWS_CONFIG_FILE='/private/tmp/spann-resume-aws-config',
           KUBECONFIG='/Users/chroma/.config/cops/kube/config', AWS_PAGER='')
AWS = ['/opt/homebrew/bin/aws', '--profile', 'spann-sandbox', '--region', 'us-east-1']
def aws(*args):
    r = subprocess.run(AWS+list(args), env=ENV, capture_output=True, text=True, timeout=45)
    if r.returncode:
        raise RuntimeError(r.stderr)
    return json.loads(r.stdout)
def write(name, value):
    (ROOT/name).write_text(json.dumps(value, indent=2)+'\n')
def identity():
    d = aws('sts', 'get-caller-identity')
    assert d['Account'] == '079819354890'
    return d

def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'watch':
        deadline = float(sys.argv[2])
        write('watchdog-ready.json', dict(pid=os.getpid(), deadline=deadline))
        time.sleep(max(0, deadline-time.time()))
        for attempt in range(5):
            try:
                identity()
                write('deadline-stop.json', aws('ec2','stop-instances','--instance-ids',INSTANCE))
                sys.exit(0)
            except Exception as e:
                write('deadline-stop-error.json', dict(attempt=attempt, error=str(e)))
                time.sleep(15)
        sys.exit(1)

    assert len(sys.argv) == 3 and sys.argv[1] == 'start'
    volume_id = sys.argv[2]
    assert volume_id.startswith('vol-') and volume_id != 'vol-07951d3573187a716'
    assert not (ROOT/'start-attempt.json').exists(), 'Never reuse this launch epoch'
    who = identity()
    raw = aws('ec2','describe-instances','--instance-ids',INSTANCE)
    i = raw['Reservations'][0]['Instances'][0]
    tags = {t['Key']:t['Value'] for t in i['Tags']}
    assert i['InstanceId']==INSTANCE and i['State']['Name']=='stopped'
    assert i['InstanceType']=='r6i.2xlarge'
    assert tags.get('purpose')=='hierarchical-spann-sharding' and tags.get('role')=='driver'
    assert len(i['BlockDeviceMappings']) == 2
    root_devices = [b for b in i['BlockDeviceMappings'] if b['DeviceName'] == i['RootDeviceName']]
    assert len(root_devices) == 1 and root_devices[0]['Ebs']['VolumeId'] == 'vol-07951d3573187a716'
    attached = {b['Ebs']['VolumeId'] for b in i['BlockDeviceMappings']}
    assert attached == {'vol-07951d3573187a716', volume_id}
    volume = aws('ec2','describe-volumes','--volume-ids',volume_id)['Volumes'][0]
    assert volume['VolumeId'] == volume_id
    vtags = {t['Key']:t['Value'] for t in volume.get('Tags',[])}
    assert volume['Size']==64 and volume['VolumeType']=='gp3' and volume['Encrypted']
    assert volume['AvailabilityZone']==i['Placement']['AvailabilityZone']
    assert vtags.get('purpose')=='hierarchical-spann-sharding' and vtags.get('epoch')=='matched-2m-dynamic-v1'
    assert len(volume['Attachments'])==1 and volume['Attachments'][0]['InstanceId']==INSTANCE
    assert volume['Attachments'][0]['State']=='attached'
    deadline = time.time()+7200
    log = (ROOT/'watchdog.log').open('a')
    p = subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'watch',str(deadline)],
                         stdout=log,stderr=log,start_new_session=True,env=ENV)
    for _ in range(50):
        if (ROOT/'watchdog-ready.json').exists():
            break
        if p.poll() is not None:
            raise RuntimeError('Stop watchdog failed before launch')
        time.sleep(.1)
    assert p.poll() is None and (ROOT/'watchdog-ready.json').exists()
    ready=json.loads((ROOT/'watchdog-ready.json').read_text())
    assert ready['pid']==p.pid and ready['deadline']==deadline
    write('start-attempt.json',dict(identity=who,instance=i,experiment_volume=volume,deadline_unix=deadline,
          deadline_utc=datetime.datetime.fromtimestamp(deadline,datetime.timezone.utc).isoformat(),
          watchdog_pid=p.pid,budget='One resized driver for a fixed7200second window; $0.504/hr implies $1.008 nominal compute, $1.10 allowance excluding stop latency. Dedicated64GiB storage costs $5.12/month retained. No other instance starts',
          remote_guard_requirement='Install a fixed UTC systemd shutdown before upload/build/integration; exact reviewed2Mrestore only; verify new volume filesystem/mount before any data writes'))
    write('start-response.json',aws('ec2','start-instances','--instance-ids',INSTANCE))
    print(json.dumps(dict(instance=INSTANCE,deadline_unix=deadline,watchdog_pid=p.pid)))

if __name__ == '__main__':
    main()
