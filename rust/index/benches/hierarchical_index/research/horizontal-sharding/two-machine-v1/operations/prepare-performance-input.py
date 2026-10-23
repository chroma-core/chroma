#!/usr/bin/env python3
"""Materialize the pinned 3M corpus and its 100K prefix on one guarded task peer.

Importing performs no AWS calls. The operator supplies the fixed execution
window and outer process timeout. Existing output and failed bytes are retained.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import urllib.parse
import urllib.request

ACCOUNT="079819354890"
REGION="us-east-1"
BUCKET="spann-artifacts-079819354890-20261014-ecfcca4d"
INSTANCES={"i-011ff180f03584f40","i-00268d1e1dfea0357"}
ROWS,DIM=3_000_000,768
WIDTH=DIM*4
TOTAL_BYTES=ROWS*WIDTH
FIRST_ROWS=100_000
FIRST_SHA="5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c"
TOTAL_SHA="18fb3c71c45bb70d28730f8edfdbd2ae90a3361b150a4871f09d870cb6684aba"
MIN_FREE=12*1024**3
LOCK=Path("/home/ubuntu/spann-benchmark.lock")


def write(path,value):
    with open(path,"x") as file:
        json.dump(value,file,indent=2)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())


def validate_plan(plan):
    if plan.get("rows")!=ROWS or plan.get("dimension")!=DIM or plan.get("vector_bytes")!=TOTAL_BYTES or plan.get("chunk_count")!=32 or len(plan.get("chunks",[]))!=32:
        raise ValueError("input must be the reviewed 3M/768D/32-object plan")
    rows=0
    for index,chunk in enumerate(plan["chunks"]):
        artifact=chunk["artifact"]
        if chunk["start_id"]!=rows or not 0<chunk["take_rows"]<=artifact["bytes"]//WIDTH or artifact["bytes"]%WIDTH or artifact["bytes"]!=chunk["full_chunk_rows"]*WIDTH:
            raise ValueError("pinned chunks are not a contiguous canonical prefix")
        digest=artifact["sha256"]
        if artifact["bucket"]!=BUCKET or artifact["expected_bucket_owner"]!=ACCOUNT or not re.fullmatch(r"[0-9a-f]{64}",digest) or artifact["key"]!="sha256/"+digest[:2]+"/"+digest or not artifact.get("version_id") or artifact["version_id"]=="null":
            raise ValueError("input object identity is outside the pinned sandbox plan")
        if index==0 and (chunk["take_rows"]!=FIRST_ROWS or artifact["bytes"]!=FIRST_ROWS*WIDTH or digest!=FIRST_SHA):
            raise ValueError("the first object must be exactly the reviewed 100K input")
        rows+=chunk["take_rows"]
    if rows!=ROWS:
        raise ValueError("pinned input does not contain exactly 3M rows")


def identity(expected):
    if expected not in INSTANCES:
        raise ValueError("expected instance is outside this task")
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request=urllib.request.Request("http://169.254.169.254/latest/api/token",method="PUT",headers={"X-aws-ec2-metadata-token-ttl-seconds":"60"})
    with opener.open(request,timeout=5) as response:
        token=response.read().decode()
    request=urllib.request.Request("http://169.254.169.254/latest/dynamic/instance-identity/document",headers={"X-aws-ec2-metadata-token":token})
    with opener.open(request,timeout=5) as response:
        result=json.load(response)
    if (result["accountId"],result["region"],result["instanceId"])!=(ACCOUNT,REGION,expected):
        raise ValueError("fresh IMDS identity differs from the exact task peer")
    return result


def command(argv):
    return subprocess.check_output(argv,text=True,timeout=5).strip()


def guard(window,deadline):
    if not re.fullmatch(r"[a-z0-9-]{1,100}",window) or not time.time()+120<deadline<=time.time()+5400:
        raise ValueError("a fresh existing two-machine window is required")
    unit="spann-two-machine-"+window+"-stop"
    fields=dict(line.split("=",1) for line in command(["systemctl","show",unit+".timer","-p","ActiveState","-p","NextElapseUSecRealtime","-p","Triggers"]).splitlines())
    if fields.get("ActiveState")!="active" or fields.get("Triggers")!=unit+".service":
        raise ValueError("exact remote shutdown timer is not active")
    actual=int(command(["date","-d",fields["NextElapseUSecRealtime"],"+%s"]))
    if abs(actual-deadline)>2:
        raise ValueError("remote shutdown timer has a different absolute deadline")
    service=dict(line.split("=",1) for line in command(["systemctl","show",unit+".service","-p","ExecStart","-p","User"]).splitlines())
    if service.get("User") not in ("","root") or not re.search(r"argv\[\]=/(?:usr/)?sbin/shutdown -h now(?: ;|$)",service.get("ExecStart","")):
        raise ValueError("timer does not invoke the fixed root shutdown action")
    return {"unit":unit,"deadline_unix":actual,"timer":fields,"service":service}


def locator(chunk):
    artifact=chunk["artifact"]
    return "s3://"+artifact["bucket"]+"/"+artifact["key"]+"?versionId="+urllib.parse.quote(artifact["version_id"],safe="")+"#start_id="+str(chunk["start_id"])+"&take_rows="+str(chunk["take_rows"])


def descriptor(path,rows,digest,locators):
    return {"path":str(path.resolve()),"rows":rows,"dimensions":DIM,"stride_bytes":WIDTH,"offset_bytes":0,"first_id":0,"file_bytes":rows*WIDTH,"sha256":digest,"immutable_locators":locators}


def materialize(s3,plan,out,deadline):
    """Stream each pinned object once, teeing its first100K rows to a second file."""
    temporary=out/"fineweb3m.f32.incomplete"
    first_temporary=out/"fineweb100k.f32.incomplete"
    written,first_written,network=0,0,0
    whole,first=hashlib.sha256(),hashlib.sha256()
    chunks=[]
    started=time.time()
    try:
        with temporary.open("xb") as dest,first_temporary.open("xb") as first_dest:
            for chunk in plan["chunks"]:
                if time.time()+30>=deadline:
                    raise TimeoutError("insufficient time before the fixed input deadline")
                artifact=chunk["artifact"]
                response=s3.get_object(Bucket=artifact["bucket"],Key=artifact["key"],VersionId=artifact["version_id"],ExpectedBucketOwner=ACCOUNT,ChecksumMode="ENABLED")
                body=response["Body"]
                try:
                    if response["ContentLength"]!=artifact["bytes"] or response.get("VersionId")!=artifact["version_id"]:
                        raise ValueError("downloaded object length/version differs from plan")
                    received,emitted,take=0,0,chunk["take_rows"]*WIDTH
                    digest=hashlib.sha256()
                    while True:
                        if time.time()>=deadline:
                            raise TimeoutError("absolute input deadline reached")
                        block=body.read(8*1024**2)
                        if not block:
                            break
                        digest.update(block)
                        received+=len(block)
                        network+=len(block)
                        if received>artifact["bytes"]:
                            raise ValueError("download exceeded pinned object length")
                        data=block[:max(0,take-emitted)]
                        if data:
                            dest.write(data)
                            whole.update(data)
                            written+=len(data)
                            emitted+=len(data)
                            if chunk["start_id"]==0:
                                first_dest.write(data)
                                first.update(data)
                                first_written+=len(data)
                    if received!=artifact["bytes"] or digest.hexdigest()!=artifact["sha256"] or emitted!=take:
                        raise ValueError("pinned chunk SHA-256 or prefix extent mismatch")
                finally:
                    body.close()
                receipt={"start_id":chunk["start_id"],"take_rows":chunk["take_rows"],"received_bytes":received,"emitted_bytes":emitted,"sha256":digest.hexdigest(),"version_id":artifact["version_id"],"locator":locator(chunk)}
                chunks.append(receipt)
                with (out/"chunks.jsonl").open("a") as log:
                    log.write(json.dumps(receipt)+"\n")
                    log.flush()
                    os.fsync(log.fileno())
            dest.flush();os.fsync(dest.fileno())
            first_dest.flush();os.fsync(first_dest.fileno())
        if written!=TOTAL_BYTES or first_written!=FIRST_ROWS*WIDTH or whole.hexdigest()!=TOTAL_SHA or first.hexdigest()!=FIRST_SHA:
            raise ValueError("assembled 3M or 100K canonical bytes fail the required whole-file SHA-256")
        full=out/"fineweb3m.f32"
        prefix=out/"fineweb100k.f32"
        temporary.rename(full);first_temporary.rename(prefix)
        write(out/"3m.json",descriptor(full,ROWS,TOTAL_SHA,[locator(c) for c in plan["chunks"]]))
        write(out/"100k.json",descriptor(prefix,FIRST_ROWS,FIRST_SHA,[locator(plan["chunks"][0])]))
        result={"status":"passed","rows":ROWS,"dimension":DIM,"bytes":written,"sha256":whole.hexdigest(),"first100K_sha256":first.hexdigest(),"first100K_bytes":first_written,"chunk_count":len(chunks),"network_payload_bytes":network,"source_downloads":len(chunks),"first100K_additional_downloads":0,"seconds":time.time()-started,"path":str(full.resolve()),"first100K_path":str(prefix.resolve()),"descriptors":{"100K":str((out/"100k.json").resolve()),"3M":str((out/"3m.json").resolve())},"no_mac_payload":True,"no_source_redecode":True}
        write(out/"input.json",result)
        return result
    except BaseException as error:
        write(out/"failure.json",{"error":str(error),"written_bytes":written,"first100K_written_bytes":first_written,"network_payload_bytes":network,"verified_chunks":len(chunks),"seconds":time.time()-started})
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan",required=True,type=Path)
    parser.add_argument("--output-dir",required=True,type=Path)
    parser.add_argument("--plan-sha256",required=True)
    parser.add_argument("--deadline",required=True,type=int)
    parser.add_argument("--window-id",required=True)
    parser.add_argument("--expected-instance",required=True)
    args=parser.parse_args()
    plan_bytes=args.plan.read_bytes()
    if not re.fullmatch(r"[0-9a-f]{64}",args.plan_sha256) or hashlib.sha256(plan_bytes).hexdigest()!=args.plan_sha256:
        raise ValueError("reviewed input plan SHA-256 mismatch")
    plan=json.loads(plan_bytes)
    validate_plan(plan)
    args.output_dir.mkdir(parents=True,exist_ok=False)
    with LOCK.open("a") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        actual=identity(args.expected_instance)
        shutdown=guard(args.window_id,args.deadline)
        free=shutil.disk_usage(args.output_dir).free
        if free<MIN_FREE:
            raise ValueError("at least12GiB free is required; retained files remain untouched")
        import boto3
        from botocore.config import Config
        session=boto3.Session(region_name=REGION)
        caller=session.client("sts",region_name=REGION).get_caller_identity()
        if caller["Account"]!=ACCOUNT or not caller["Arn"].startswith("arn:aws:sts::"+ACCOUNT+":assumed-role/spann-artifact-ecfcca4d/"):
            raise ValueError("unexpected sandbox artifact role")
        write(args.output_dir/"environment.json",{"imds_identity":actual,"sts_identity":caller,"remote_guard":shutdown,"free_bytes":free,"minimum_free_bytes":MIN_FREE,"plan_sha256":args.plan_sha256,"deadline_unix":args.deadline,"operator_controls_outer_timeout":True})
        s3=session.client("s3",region_name=REGION,config=Config(connect_timeout=10,read_timeout=30,retries={"max_attempts":0}))
        print(json.dumps(materialize(s3,plan,args.output_dir,args.deadline)))


if __name__=="__main__":
    main()
