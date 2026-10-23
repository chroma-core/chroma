import datetime,json,os,pathlib,subprocess,uuid
root=pathlib.Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding');base=root/'fleet/gpu-numeric-pilot';base.mkdir(exist_ok=True)
env=dict(os.environ,AWS_CONFIG_FILE=str(root/'fleet/aws-config'))
def aws(service,operation,*args,save=None):
 r=subprocess.run(['aws','--profile','spann-sandbox','--region','us-east-1','--no-cli-pager',service,operation,*args,'--output','json'],env=env,text=True,capture_output=True);
 if r.returncode:
  (base/(operation+"-error.txt")).write_text(r.stderr);raise RuntimeError(r.stderr)
 value=json.loads(r.stdout) if r.stdout.strip() else {}
 if save:(base/(save+'.json')).write_text(json.dumps(value,indent=2))
 return value
account='079819354890';assert aws('sts','get-caller-identity',save='identity')['Account']==account
image=aws('ec2','describe-images','--image-ids','ami-0a24e16849196176a',save='image')['Images'][0]
assert image['OwnerId']=='898082745236' and image['Architecture']=='x86_64' and image['State']=='available' and not image.get('ProductCodes')
sg=aws('ec2','describe-security-groups','--group-ids','sg-0c0f1b8e885af0221',save='security-group')['SecurityGroups'][0];assert sg['OwnerId']==account
subnet=aws('ec2','describe-subnets','--subnet-ids','subnet-06ac838283d771daa',save='subnet')['Subnets'][0];assert subnet['OwnerId']==account and subnet['VpcId']==sg['VpcId'] and subnet['AvailabilityZone']=='us-east-1d'
key=aws('ec2','describe-key-pairs','--key-names','lyon',save='key-pair')['KeyPairs'][0];assert key['KeyName']=='lyon'
archive=json.loads((root/'fleet/artifact-store/manifest.json').read_text());profile=aws('iam','get-instance-profile','--instance-profile-name',archive['profile'],save='profile')['InstanceProfile'];assert profile['Arn']==archive['profile_arn'] and profile['Roles'][0]['Arn']==archive['role_arn']
now=datetime.datetime.now(datetime.timezone.utc);deadline=now+datetime.timedelta(minutes=30);stop_at=deadline.strftime('%Y-%m-%d %H:%M:%S UTC')
userdata='''#!/bin/bash
set -euo pipefail
token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
identity=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document)
python3 -c 'import json,sys;r=json.loads(sys.argv[1]);assert r["accountId"]=="079819354890" and r["instanceType"]=="g6.2xlarge" and r["imageId"]=="ami-0a24e16849196176a"' "$identity"
mkdir -p /var/lib/spann-gpu-results
cat >/usr/local/sbin/spann-gpu-preserve-stop <<'GUARD'
#!/bin/bash
mkdir -p /var/lib/spann-gpu-results
for name in gpu-runtime-evidence gpu-pilot-result-v1; do
 if test -d /mnt/spann/$name; then cp -a /mnt/spann/$name /var/lib/spann-gpu-results/ || logger 'SPANN GPU: preservation failed; stopping at cost deadline'; fi
done
sync
shutdown -h now
GUARD
chmod +x /usr/local/sbin/spann-gpu-preserve-stop
cat >/etc/systemd/system/spann-gpu-stop.service <<'SERVICE'
[Unit]
Description=Preserve bounded numeric pilot evidence and stop GPU
[Service]
Type=oneshot
ExecStart=/usr/local/sbin/spann-gpu-preserve-stop
SERVICE
cat >/etc/systemd/system/spann-gpu-stop.timer <<'TIMER'
[Unit]
Description=Absolute thirty-minute sandbox GPU cost guard
[Timer]
OnCalendar=STOP_AT
Persistent=true
Unit=spann-gpu-stop.service
[Install]
WantedBy=timers.target
TIMER
systemctl daemon-reload
systemctl enable --now spann-gpu-stop.timer
if test "$(date +%s)" -ge "$(date -d 'STOP_AT' +%s)"; then /usr/local/sbin/spann-gpu-preserve-stop;exit;fi
mkdir -p /mnt/spann
mapfile -t disks < <(lsblk -dnpo NAME,MODEL | awk '/Amazon EC2 NVMe Instance Storage/ {print $1}')
if ((${#disks[@]}==1)); then
 disk=${disks[0]};test -z "$(lsblk -nro MOUNTPOINT "$disk" | tr -d '[:space:]')";test -z "$(wipefs -n "$disk")";mkfs.ext4 -F "$disk";mount "$disk" /mnt/spann
fi
chown ubuntu:ubuntu /mnt/spann
mkdir -p /mnt/spann/gpu-runtime-evidence
chown ubuntu:ubuntu /mnt/spann/gpu-runtime-evidence
printf '%s\\n' "$identity" >/mnt/spann/gpu-runtime-evidence/instance-identity.json
systemctl list-timers spann-gpu-stop.timer --no-pager >/mnt/spann/gpu-runtime-evidence/stop-guard.txt
nvidia-smi -q -x >/mnt/spann/gpu-runtime-evidence/bootstrap-nvidia-smi.xml
'''.replace('STOP_AT',stop_at)
(base/'bootstrap.sh').write_text(userdata)
token='spann-gpu-'+uuid.uuid4().hex;tags=[dict(Key=k,Value=v) for k,v in dict(Name=token,Project='hierarchical-spann',Owner='lyon-chroma',Purpose='bounded-numeric-conformance',Run=token,StopAt=deadline.isoformat()).items()]
request=dict(ImageId=image['ImageId'],InstanceType='g6.2xlarge',MinCount=1,MaxCount=1,KeyName='lyon',ClientToken=token,NetworkInterfaces=[dict(DeviceIndex=0,SubnetId=subnet['SubnetId'],Groups=[sg['GroupId']],AssociatePublicIpAddress=True)],BlockDeviceMappings=[dict(DeviceName=image['RootDeviceName'],Ebs=dict(VolumeSize=75,VolumeType='gp3',DeleteOnTermination=True,Encrypted=True))],MetadataOptions=dict(HttpTokens='required',HttpEndpoint='enabled'),IamInstanceProfile=dict(Arn=profile['Arn']),InstanceInitiatedShutdownBehavior='stop',TagSpecifications=[dict(ResourceType=kind,Tags=tags) for kind in ('instance','volume')])
(base/'request.json').write_text(json.dumps(request,indent=2));result=aws('ec2','run-instances','--cli-input-json',json.dumps(request),'--user-data','file://'+str(base/'bootstrap.sh'),save='launch')
instance=result['Instances'][0];manifest=dict(account=account,instance_id=instance['InstanceId'],type='g6.2xlarge',image=image['ImageId'],image_owner=image['OwnerId'],instance_profile=profile['Arn'],security_group=sg['GroupId'],subnet=subnet['SubnetId'],availability_zone=subnet['AvailabilityZone'],started_utc=now.isoformat(),absolute_stop_utc=deadline.isoformat(),hourly_compute_usd=.9776,compute_cap_estimate_usd=.4888,root_gib=75,root_delete_on_termination=True,source_price=str(root/'subtree/fineweb/gpu-g6-pricing.json'),role_scope='Only private artifact bucket sha256/ objects; no IAM/EC2/data-plane privileges.')
(base/'manifest.json').write_text(json.dumps(manifest,indent=2));print(json.dumps(manifest),flush=True)
