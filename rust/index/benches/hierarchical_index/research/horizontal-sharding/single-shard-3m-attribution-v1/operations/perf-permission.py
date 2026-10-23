#!/usr/bin/env python3
"""Temporarily permit only per-process userspace sampling on the exact worker."""
import argparse,json,pathlib,subprocess,time,urllib.request
p=argparse.ArgumentParser();p.add_argument('action',choices=['enable','restore']);p.add_argument('--window-id',required=True);p.add_argument('--receipt',required=True,type=pathlib.Path);a=p.parse_args();assert a.window_id.replace('-','').isalnum()
request=urllib.request.Request('http://169.254.169.254/latest/api/token',method='PUT',headers={'X-aws-ec2-metadata-token-ttl-seconds':'60'})
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
with opener.open(request,timeout=5) as r:token=r.read().decode()
request=urllib.request.Request('http://169.254.169.254/latest/dynamic/instance-identity/document',headers={'X-aws-ec2-metadata-token':token})
with opener.open(request,timeout=5) as r:identity=json.load(r)
assert identity['instanceId']=='i-011ff180f03584f40' and identity['accountId']=='079819354890' and identity['region']=='us-east-1'
setting=pathlib.Path('/proc/sys/kernel/perf_event_paranoid')
if a.action=='enable':
 subprocess.run(['systemctl','is-active','--quiet',f'spann-single-shard-3m-{a.window_id}-stop.timer'],check=True)
 assert not a.receipt.exists() and setting.read_text().strip()=='4'
 report={'window_id':a.window_id,'instance':identity['instanceId'],'account':identity['accountId'],'original':'4','temporary':'2','persistent_config_modified':False,'restored':False,'enabled_unix':time.time()};a.receipt.parent.mkdir(parents=True,exist_ok=True);a.receipt.write_text(json.dumps(report,indent=2)+'\n')
 subprocess.run(['sudo','-n','sysctl','-w','kernel.perf_event_paranoid=2'],check=True);assert setting.read_text().strip()=='2'
else:
 report=json.loads(a.receipt.read_text());assert report['instance']==identity['instanceId'] and report['account']==identity['accountId'] and report['window_id']==a.window_id and report['original']=='4' and report['temporary']=='2'
 current=setting.read_text().strip();assert current in ['2','4'],'Unexpectedcurrentperfpermission;refusetoclobber'
 if current=='2':subprocess.run(['sudo','-n','sysctl','-w','kernel.perf_event_paranoid=4'],check=True)
 after=setting.read_text().strip();assert after=='4'
 report['restored']=True;report['restore_before_readback']=current;report['restore_after_readback']=after;report['restore_idempotent']=current=='4';report['restored_unix']=time.time();a.receipt.write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report))
