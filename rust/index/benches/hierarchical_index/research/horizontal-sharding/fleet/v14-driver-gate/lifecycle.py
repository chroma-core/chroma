import os,sys,json,time,subprocess,pathlib,datetime
P=pathlib.Path(__file__).resolve().parent
E=dict(os.environ,AWS_CONFIG_FILE='/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/fleet/aws-config')
A=['/opt/homebrew/bin/aws','--profile','spann-sandbox','--region','us-east-1']
def aws(*x):
 r=subprocess.run(A+list(x),env=E,capture_output=True,text=True,check=True); return json.loads(r.stdout)
if len(sys.argv)>1:
 deadline=float(sys.argv[1]);time.sleep(max(0,deadline-time.time()))
 identity=aws('sts','get-caller-identity');assert identity['Account']=='079819354890'
 r=aws('ec2','stop-instances','--instance-ids','i-00268d1e1dfea0357');(P/'deadline-stop.json').write_text(json.dumps(r,indent=2));sys.exit()
identity=aws('sts','get-caller-identity');assert identity['Account']=='079819354890'
r=aws('ec2','describe-instances','--instance-ids','i-00268d1e1dfea0357');i=r['Reservations'][0]['Instances'][0];assert i['State']['Name']=='stopped' and i['InstanceType']=='m6i.2xlarge'
started=time.time();deadline=started+1200
log=open(P/'watchdog.log','a');guard=subprocess.Popen([sys.executable,str(pathlib.Path(__file__).resolve()),str(deadline)],stdout=log,stderr=log,start_new_session=True)
receipt={'account':identity,'prestart':r,'restart_requested_unix':started,'absolute_stop_unix':deadline,'absolute_stop_utc':datetime.datetime.fromtimestamp(deadline,datetime.timezone.utc).isoformat(),'watchdog_pid':guard.pid,'rate_usd_hour':.384,'compute_ceiling_usd':.128,'workers_and_prep_restart_authorized':False}
(P/'restart-guard.json').write_text(json.dumps(receipt,indent=2))
result=aws('ec2','start-instances','--instance-ids','i-00268d1e1dfea0357');(P/'start-response.json').write_text(json.dumps(result,indent=2));print(json.dumps(receipt))
