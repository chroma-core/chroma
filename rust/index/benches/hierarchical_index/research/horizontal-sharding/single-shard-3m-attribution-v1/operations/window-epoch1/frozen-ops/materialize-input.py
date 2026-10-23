#!/usr/bin/env python3
"""Restore only a frozen canonical FineWeb prefix directly on the approved worker."""
import argparse,hashlib,json,os,pathlib,shutil,subprocess,time
import boto3
from botocore.config import Config
p=argparse.ArgumentParser();p.add_argument('--plan',required=True,type=pathlib.Path);p.add_argument('--output-dir',required=True,type=pathlib.Path);p.add_argument('--window-id',required=True);p.add_argument('--deadline',required=True,type=float);a=p.parse_args()
assert a.window_id.replace('-','').isalnum()
subprocess.run(['systemctl','is-active','--quiet',f'spann-single-shard-3m-{a.window_id}-stop.timer'],check=True)
assert time.time()+300<a.deadline
plan=json.loads(a.plan.read_text());assert plan['rows']==3000000 and plan['dimension']==768 and plan['vector_bytes']==9216000000
identity=boto3.client('sts',region_name='us-east-1').get_caller_identity();assert identity['Account']=='079819354890' and identity['Arn'].startswith('arn:aws:sts::079819354890:assumed-role/spann-artifact-ecfcca4d/')
s3=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=10,read_timeout=30,retries={'max_attempts':0}))
a.output_dir.mkdir(parents=True,exist_ok=False)
assert shutil.disk_usage(a.output_dir).free>=80*1024**3
out=a.output_dir/'fineweb3m.f32';temporary=a.output_dir/'fineweb3m.f32.incomplete';written=0;whole=hashlib.sha256();chunks=[];first=hashlib.sha256();started=time.time()
try:
 with temporary.open('xb') as dest:
  for c in plan['chunks']:
   assert time.time()+120<a.deadline and c['start_id']*768*4==written
   v=c['artifact'];assert v['bucket']=='spann-artifacts-079819354890-20261014-ecfcca4d' and v['expected_bucket_owner']=='079819354890'
   response=s3.get_object(Bucket=v['bucket'],Key=v['key'],VersionId=v['version_id'],ExpectedBucketOwner=v['expected_bucket_owner'],ChecksumMode='ENABLED')
   assert response['ContentLength']==v['bytes'] and response['VersionId']==v['version_id']
   h=hashlib.sha256();received=0;emitted=0;take=c['take_rows']*768*4
   body=response['Body']
   try:
    while True:
     block=body.read(8*1024**2)
     if not block:break
     assert time.time()<a.deadline
     h.update(block);received+=len(block);assert received<=v['bytes']
     data=block[:max(0,take-emitted)]
     if data:dest.write(data);whole.update(data);written+=len(data);emitted+=len(data)
     if c['start_id']==0:first.update(data)
   finally:body.close()
   assert received==v['bytes'] and h.hexdigest()==v['sha256'] and emitted==take
   row={'start_id':c['start_id'],'received_bytes':received,'emitted_bytes':emitted,'whole_chunk_sha256':h.hexdigest(),'version_id':v['version_id']};chunks.append(row)
   with (a.output_dir/'chunks.jsonl').open('a') as log:log.write(json.dumps(row)+'\n')
  dest.flush();os.fsync(dest.fileno())
 assert written==9216000000 and first.hexdigest()=='5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c'
 temporary.rename(out)
 report={'status':'passed','path':str(out.resolve()),'rows':3000000,'dimension':768,'bytes':written,'sha256':whole.hexdigest(),'first100K_sha256':first.hexdigest(),'plan_sha256':hashlib.sha256(a.plan.read_bytes()).hexdigest(),'full_chunk_count':len(chunks),'network_payload_bytes':sum(c['received_bytes'] for c in chunks),'identity':identity,'seconds':time.time()-started,'no_mac_payload':True,'no_source_redecode':True}
 (a.output_dir/'input.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
except BaseException as exc:
 (a.output_dir/'failure.json').write_text(json.dumps({'error':str(exc),'written_bytes':written,'verified_chunks':len(chunks),'seconds':time.time()-started},indent=2)+'\n');raise
