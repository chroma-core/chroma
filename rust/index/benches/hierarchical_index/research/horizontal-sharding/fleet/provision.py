import subprocess,json,os,pathlib,datetime
base=pathlib.Path(__file__).resolve().parent
os.environ['AWS_CONFIG_FILE']=str(base/'aws-config')
def aws(*args):return json.loads(subprocess.check_output(['aws','--profile','spann-sandbox','--region','us-east-1',*args],text=True))
def guard():
 assert aws('sts','get-caller-identity')['Account']=='079819354890'
tags=[{'Key':k,'Value':v} for k,v in {'purpose':'hierarchical-spann-sharding','owner':'lyon','run':'20261014'}.items()]
guard()
sg=aws('ec2','create-security-group','--group-name','hierarchical-spann-20261014','--description','Isolated SPANN sandbox pilot','--vpc-id','vpc-0fd11710adf364ab8','--tag-specifications',json.dumps([{'ResourceType':'security-group','Tags':tags}]))['GroupId']
(base/'security-group.json').write_text(json.dumps({'GroupId':sg}))
aws('ec2','authorize-security-group-ingress','--group-id',sg,'--ip-permissions',json.dumps([{'IpProtocol':'tcp','FromPort':22,'ToPort':22,'IpRanges':[{'CidrIp':'134.128.209.150/32'}]},{'IpProtocol':'-1','UserIdGroupPairs':[{'GroupId':sg}]}]))
fleet={'account':'079819354890','region':'us-east-1','profile':'spann-sandbox','aws_config_file':str(base/'aws-config'),'security_group':sg,'ssh_user':'ubuntu','ssh_key':'/Users/chroma/Documents/lyon.pem','created_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'instances':[]}
for role,kind,count in [('worker','r6id.16xlarge',2),('driver','m6i.2xlarge',1)]:
 guard()
 specs=[{'ResourceType':r,'Tags':tags+[{'Key':'Name','Value':'hierarchical-spann-'+role+'-20261014'},{'Key':'role','Value':role}]} for r in ['instance','volume']]
 args=['ec2','run-instances','--image-id','ami-0b6c6ebed2801a5cb','--instance-type',kind,'--count',str(count),'--key-name','lyon','--client-token','hierarchical-spann-20261014-'+role,'--network-interfaces',json.dumps([{'DeviceIndex':0,'SubnetId':'subnet-056d285a2fd71bfbb','Groups':[sg],'AssociatePublicIpAddress':True}]),'--block-device-mappings',json.dumps([{'DeviceName':'/dev/sda1','Ebs':{'VolumeSize':100,'VolumeType':'gp3','DeleteOnTermination':True,'Encrypted':True}}]),'--metadata-options','HttpTokens=required,HttpEndpoint=enabled','--instance-initiated-shutdown-behavior','stop','--user-data','file://'+str(base/'bootstrap.sh'),'--tag-specifications',json.dumps(specs)]
 (base/(role+'-request.json')).write_text(json.dumps(args,indent=2))
 result=aws(*args);(base/(role+'-launch.json')).write_text(json.dumps(result,indent=2))
 fleet['instances'] += [{'role':role,'type':kind,'id':i['InstanceId'],'private_ip':i['PrivateIpAddress']} for i in result['Instances']]
 (base/'fleet.json').write_text(json.dumps(fleet,indent=2))
print(json.dumps(fleet,indent=2))
