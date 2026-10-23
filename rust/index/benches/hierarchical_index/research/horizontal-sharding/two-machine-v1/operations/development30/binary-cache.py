#!/usr/bin/env python3
"""Move verified research executables between owners through the existing task bucket."""
import argparse,gzip,hashlib,json,re,shutil,time
from pathlib import Path
BUCKET='spann-artifacts-079819354890-20261014-ecfcca4d';ACCOUNT='079819354890'
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
 return h.hexdigest()
def main():
 import boto3
 from botocore.config import Config
 a=argparse.ArgumentParser();a.add_argument('mode',choices=['upload','download']);a.add_argument('path');a.add_argument('sha256');a.add_argument('--deadline',type=int,required=True);v=a.parse_args();p=Path(v.path)
 assert re.fullmatch(r'/home/ubuntu/spann-two-machine-v1/development30/(baseline[0-9]*|candidate|transport1)/two_machine_(spann|reader)',str(p));assert re.fullmatch('[0-9a-f]{64}',v.sha256);assert time.time()+60<v.deadline
 session=boto3.Session(region_name='us-east-1');assert session.client('sts').get_caller_identity()['Account']==ACCOUNT
 s3=session.client('s3',config=Config(connect_timeout=10,read_timeout=60,retries={'max_attempts':2}));key='sha256/binary-cache/'+v.sha256+'.gz';tmp=p.with_name(p.name+'.verified-transfer.gz')
 if v.mode=='upload':
  assert digest(p)==v.sha256
  with p.open('rb') as source,tmp.open('xb') as target,gzip.GzipFile(filename='',mode='wb',fileobj=target,mtime=0,compresslevel=1) as zipped:shutil.copyfileobj(source,zipped,8*1024*1024)
  from botocore.exceptions import ClientError
  try:
   existing=s3.head_object(Bucket=BUCKET,Key=key,ExpectedBucketOwner=ACCOUNT);assert existing['Metadata']['binary-sha256']==v.sha256
  except ClientError as error:
   if error.response['ResponseMetadata']['HTTPStatusCode']!=404:raise
   with tmp.open('rb') as body:s3.put_object(Bucket=BUCKET,Key=key,Body=body,ExpectedBucketOwner=ACCOUNT,Metadata={'binary-sha256':v.sha256})
 else:
  assert not p.exists();p.parent.mkdir(parents=True,exist_ok=True)
  response=s3.get_object(Bucket=BUCKET,Key=key,ExpectedBucketOwner=ACCOUNT);assert response['Metadata']['binary-sha256']==v.sha256
  with tmp.open('xb') as target:shutil.copyfileobj(response['Body'],target,8*1024*1024)
  expanded=p.with_name(p.name+'.verified-transfer')
  with gzip.open(tmp,'rb') as source,expanded.open('xb') as target:shutil.copyfileobj(source,target,8*1024*1024)
  assert digest(expanded)==v.sha256;expanded.chmod(0o755);expanded.rename(p)
 assert time.time()<v.deadline
 print(json.dumps({'mode':v.mode,'sha256':v.sha256,'compressed_bytes':tmp.stat().st_size,'key':key}))
if __name__=='__main__':main()
