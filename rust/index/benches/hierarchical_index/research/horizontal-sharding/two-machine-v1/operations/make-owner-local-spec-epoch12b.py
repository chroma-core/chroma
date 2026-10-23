#!/usr/bin/env python3
"""Prepare exactly one instrumented100K reference or distributed profile arm."""
import argparse,copy,hashlib,importlib.util,json,re,time
from pathlib import Path
HERE=Path(__file__).resolve().parent
source=importlib.util.spec_from_file_location("frozen_spec",HERE/"make-performance-spec-epoch7.py")
frozen=importlib.util.module_from_spec(source);source.loader.exec_module(frozen)
job_source=importlib.util.spec_from_file_location("epoch12_job",HERE/"remote-job-epoch12b.py")
remote_job=importlib.util.module_from_spec(job_source);job_source.loader.exec_module(remote_job)
WORKER,PEER=frozen.WORKER,frozen.PEER
BASE="/home/ubuntu/spann-two-machine-v1/epoch12b"
DESCRIPTOR="/home/ubuntu/spann-two-machine-v1/epoch2/input/100k.json"
INPUT="/home/ubuntu/spann-two-machine-v1/epoch2/input/fineweb100k.f32"

def artifact_bindings():
 path=HERE/"runtime-binaries-epoch12b.json";body=path.read_bytes();hashes=json.loads(body)
 for name in ("two_machine_spann","two_machine_reader","reference"):
  if not re.fullmatch(r"[0-9a-f]{64}",hashes.get(name,"")):raise ValueError("exact rebuilt binary hashes are required")
 return hashes,hashlib.sha256(body).hexdigest()

def generate(args,now):
 if args.arm not in ("100k-reference","100k-distributed"):raise ValueError("select one explicit fixed100K profile arm")
 if args.arm=="100k-reference" and args.mode!="run":raise ValueError("reference has no peer process")
 if not re.fullmatch(r"epoch12-[a-z0-9-]{1,53}",args.window_id):raise ValueError("fresh epoch12 window required")
 if (args.base,args.corpus_descriptor,args.input_file)!=(BASE,DESCRIPTOR,INPUT):raise ValueError("exact retained100K input/fresh outputs required")
 if not now<args.job_deadline<=now+590:raise ValueError("absolute job bound cannot exceed590seconds")
 original=copy.copy(args);original.base="/home/ubuntu/spann-two-machine-v1/epoch7"
 original.arm="100k-baseline-reference" if args.arm=="100k-reference" else "100k-baseline-distributed"
 value=frozen.generate(original,now);hashes,binding=artifact_bindings();owner=1 if args.mode=="run" else 2
 value["job_id"]=args.arm+f"-owner{owner}"
 executable="reference" if args.arm=="100k-reference" else "two_machine_spann"
 value["binary"]=BASE+"/build/"+("reference.bin" if executable=="reference" else executable)
 value["binary_sha256"]=hashes[executable];value["reader"]=BASE+"/build/two_machine_reader";value["reader_sha256"]=hashes["two_machine_reader"]
 output_flag="--output-dir" if value["kind"]=="reference" else "--output"
 value["args"][output_flag]=BASE+"/"+value["job_id"]
 if value["kind"]!="reference":
  value["args"]["--reader-binary"]=value["reader"]
  value["args"]["--run"]=int.from_bytes(hashlib.sha256((args.window_id+":"+args.arm).encode()).digest()[:16],"big")
 if args.mode=="run":value["profile_gate"]=value["args"][output_flag]+"-profile-gate"
 value["provenance"].update(arm=args.arm,artifact_hash_file_sha256=binding,
  collector_script_sha256=hashlib.sha256((HERE/"remote-profile-epoch12b.py").read_bytes()).hexdigest(),
  profile_scope="entire first timed balance using acknowledged perf enable before go and done after timer; bounded gate waits excluded")
 remote_job.validate(value,now);return value

def main():
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument("--arm",choices=("100k-reference","100k-distributed"),required=True)
 parser.add_argument("--mode",choices=("run","serve"),required=True)
 for flag in ("window-id","worker-ip","peer-ip"):parser.add_argument("--"+flag,required=True)
 for flag in ("window-deadline","job-deadline"):parser.add_argument("--"+flag,type=int,required=True)
 parser.add_argument("--base",default=BASE);parser.add_argument("--corpus-descriptor",default=DESCRIPTOR);parser.add_argument("--input-file",default=INPUT)
 parser.add_argument("--output",type=Path,required=True);args=parser.parse_args();value=generate(args,time.time())
 body=(json.dumps(value,indent=2,sort_keys=True)+"\n").encode();args.output.parent.mkdir(parents=True,exist_ok=True)
 sidecar=args.output.with_name(args.output.name+".sha256")
 if args.output.exists() or sidecar.exists():raise ValueError("preserve existing spec/sidecar")
 with args.output.open("xb") as file:file.write(body)
 with sidecar.open("x") as file:file.write(hashlib.sha256(body).hexdigest()+"  "+args.output.name+"\n")
if __name__=="__main__":main()
