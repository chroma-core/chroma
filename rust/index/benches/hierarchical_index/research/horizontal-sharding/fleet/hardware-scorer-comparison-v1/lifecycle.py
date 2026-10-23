"""One existing sandbox driver, fixed forty-five-minute deadline, independent stop process."""
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

assert sys.argv[1:] == ['start']
assert not (ROOT/'start-attempt.json').exists(), 'Never reuse this launch epoch'
who = identity()
raw = aws('ec2','describe-instances','--instance-ids',INSTANCE)
i = raw['Reservations'][0]['Instances'][0]
tags = {t['Key']:t['Value'] for t in i['Tags']}
assert i['InstanceId']==INSTANCE and i['State']['Name']=='stopped'
assert i['InstanceType']=='m6i.2xlarge'
assert tags.get('purpose')=='hierarchical-spann-sharding' and tags.get('role')=='driver'
assert i['BlockDeviceMappings'][0]['Ebs']['VolumeId']=='vol-07951d3573187a716'
deadline = time.time()+2700
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
write('start-attempt.json',dict(identity=who,instance=i,deadline_unix=deadline,
      deadline_utc=datetime.datetime.fromtimestamp(deadline,datetime.timezone.utc).isoformat(),
      watchdog_pid=p.pid,budget='One existing driver for at most 2700 seconds; fresh $0.384/hr rate implies $0.288 compute maximum, below $1 ceiling; no worker/prep start',
      remote_guard_requirement='Install a fixed UTC systemd shutdown before data restore or build'))
write('start-response.json',aws('ec2','start-instances','--instance-ids',INSTANCE))
print(json.dumps(dict(instance=INSTANCE,deadline_unix=deadline,watchdog_pid=p.pid)))
