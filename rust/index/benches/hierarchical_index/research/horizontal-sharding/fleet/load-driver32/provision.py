import datetime,json,os,pathlib,subprocess,uuid
root=pathlib.Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding');out=root/'fleet/load-driver32';env=dict(os.environ,AWS_CONFIG_FILE=str(root/'fleet/aws-config'))
def aws(s,op,*args):
 r=subprocess.run(['aws','--profile','spann-sandbox','--region','us-east-1','--no-cli-pager',s,op,*args,'--output','json'],env=env,text=True,capture_output=True);(out/(op+'.stderr')).write_text(r.stderr);r.check_returncode();v=json.loads(r.stdout or '{}');(out/(op+'.json')).write_text(json.dumps(v,indent=2));return v
assert aws('sts','get-caller-identity')['Account']=='079819354890'
t=aws('ec2','describe-instance-types','--instance-types','m6i.8xlarge')['InstanceTypes'][0];assert t['VCpuInfo']['DefaultVCpus']==32 and t['MemoryInfo']['SizeInMiB']==131072
im=aws('ec2','describe-images','--image-ids','ami-0b6c6ebed2801a5cb')['Images'][0];assert im['OwnerId']=='099720109477' and im['State']=='available'
sg=aws('ec2','describe-security-groups','--group-ids','sg-0c0f1b8e885af0221')['SecurityGroups'][0];assert sg['OwnerId']=='079819354890'
sub=aws('ec2','describe-subnets','--subnet-ids','subnet-056d285a2fd71bfbb')['Subnets'][0];assert sub['OwnerId']=='079819354890' and sub['VpcId']==sg['VpcId']
profile=aws('iam','get-instance-profile','--instance-profile-name','spann-artifact-ecfcca4d')['InstanceProfile'];assert profile['Arn']=='arn:aws:iam::079819354890:instance-profile/spann-artifact-ecfcca4d'
filters=[dict(Type='TERM_MATCH',Field=k,Value=v) for k,v in dict(instanceType='m6i.8xlarge',location='US East (N. Virginia)',operatingSystem='Linux',tenancy='Shared',preInstalledSw='NA',capacitystatus='Used').items()]
price=aws('pricing','get-products','--service-code','AmazonEC2','--filters',json.dumps(filters));rates=[]
for raw in price['PriceList']:
 for term in json.loads(raw).get('terms',{}).get('OnDemand',{}).values():
  rates += [float(x['pricePerUnit']['USD']) for x in term['priceDimensions'].values() if x['unit']=='Hrs']
assert rates and len(set(rates))==1;hourly=rates[0]
deadline=datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=3);stop=deadline.strftime('%Y-%m-%d %H:%M:%S UTC')
user='''#!/bin/bash
set -euo pipefail
mkdir -p /mnt/spann/results /var/lib/spann-load-results
cat >/usr/local/sbin/spann-load-stop <<'STOP'
#!/bin/bash
cp -a /mnt/spann/results/. /var/lib/spann-load-results/ || true
sync
shutdown -h now
STOP
chmod +x /usr/local/sbin/spann-load-stop
printf '[Unit]\\nDescription=Bounded sandbox load driver\\n[Service]\\nType=oneshot\\nExecStart=/usr/local/sbin/spann-load-stop\\n' >/etc/systemd/system/spann-load-stop.service
printf '[Timer]\\nOnCalendar=DEADLINE\\nPersistent=true\\nUnit=spann-load-stop.service\\n[Install]\\nWantedBy=timers.target\\n' >/etc/systemd/system/spann-load-stop.timer
systemctl daemon-reload
systemctl enable --now spann-load-stop.timer
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y python3-venv awscli
chown -R ubuntu:ubuntu /mnt/spann
'''.replace('DEADLINE',stop)
(out/'bootstrap.sh').write_text(user)
a=json.load(open(root/'fleet/driver-request.json'));a[a.index('--instance-type')+1]='m6i.8xlarge';a[a.index('--client-token')+1]='spann-load32-'+uuid.uuid4().hex;a[a.index('--user-data')+1]='file://'+str(out/'bootstrap.sh');a[a.index('--tag-specifications')+1]=a[a.index('--tag-specifications')+1].replace('hierarchical-spann-driver-20261014','spann-load-driver32-20261015');a+=['--iam-instance-profile','Arn='+profile['Arn']]
(out/'request.json').write_text(json.dumps(a));r=aws(a[0],a[1],*a[2:]);i=r['Instances'][0];(out/'manifest.json').write_text(json.dumps(dict(instance_id=i['InstanceId'],deadline=deadline.isoformat(),hourly_usd=hourly,account='079819354890',type='m6i.8xlarge',vcpus=32,memory_gib=128,role='load-driver32',compute_cap_usd=3*hourly),indent=2));print((out/'manifest.json').read_text())
