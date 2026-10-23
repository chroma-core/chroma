import json,importlib.util
from pathlib import Path
p=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('life',p/'lifecycle.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
who=m.identity();m.write('identity.json',who)
i=m.aws('ec2','describe-instances','--instance-ids',m.INSTANCE);m.write('instance-before.json',i);v=i['Reservations'][0]['Instances'][0]
assert v['State']['Name']=='stopped' and v['InstanceType']=='r6i.2xlarge'
tags={t['Key']:t['Value'] for t in v['Tags']};assert tags['purpose']=='hierarchical-spann-sharding' and tags['role']=='driver'
vol=m.aws('ec2','describe-volumes','--volume-ids','vol-07951d3573187a716','vol-06c673c76a50340f8');m.write('volumes-before.json',vol)
for x in vol['Volumes']:
 assert x['VolumeType']=='gp3' and x['Encrypted'] and x['Iops']==3000 and x['Throughput']==125
 assert x['Attachments'][0]['InstanceId']==m.INSTANCE and x['Attachments'][0]['State']=='attached'
 assert x['Size']==(100 if x['VolumeId']=='vol-07951d3573187a716' else 64)
try:
 mods=m.aws('ec2','describe-volumes-modifications','--volume-ids','vol-07951d3573187a716')
except RuntimeError as e:
 if 'InvalidVolumeModification.NotFound' not in str(e):raise
 mods={'VolumesModifications':[],'api_no_previous_modification':str(e)}
m.write('modifications-before.json',mods)
assert all(x['ModificationState'] in ['completed','failed'] for x in mods['VolumesModifications'])
pricing=m.aws('pricing','get-products','--service-code','AmazonEC2','--filters','Type=TERM_MATCH,Field=volumeApiName,Value=gp3','Type=TERM_MATCH,Field=location,Value=US East (N. Virginia)','--max-results','100');m.write('gp3-pricing.json',pricing)
rates=[]
for raw in pricing['PriceList']:
 d=json.loads(raw)
 if d['product']['attributes'].get('usagetype')=='EBS:VolumeUsage.gp3':
  for t in d['terms']['OnDemand'].values():
   for r in t['priceDimensions'].values(): rates.append(float(r['pricePerUnit']['USD']))
assert rates==[.08],rates
m.write('preflight-complete.json',{'account':who['Account'],'root_volume':'vol-07951d3573187a716','root_before_gib':100,'root_after_gib':116,'gp3_usd_gib_month':rates[0],'incremental_monthly_usd':1.28,'all_checks_passed':True})
print(json.dumps({'preflight':'passed','account':who['Account'],'state':v['State']['Name'],'incremental_monthly_usd':1.28}))
