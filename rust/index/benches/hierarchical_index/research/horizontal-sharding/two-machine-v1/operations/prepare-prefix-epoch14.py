#!/usr/bin/env python3
"""Copy a fixed500K prefix from the retained verified3M file; no network download."""
import argparse,fcntl,hashlib,importlib.util,json,os,shutil,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--window-id',required=True);p.add_argument('--deadline',type=int,required=True);p.add_argument('--instance',required=True);a=p.parse_args()
base=Path('/home/ubuntu/spann-two-machine-v1/epoch14')
s=importlib.util.spec_from_file_location('job',base/'remote-job.py');j=importlib.util.module_from_spec(s);s.loader.exec_module(j)
identity=j.imds(a.instance)
assert a.window_id.startswith('epoch14-') and 0<a.deadline-time.time()<=1800
j.shutdown_guard({'window_id':a.window_id,'window_deadline_unix':a.deadline})
lock=Path('/home/ubuntu/spann-benchmark.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
assert shutil.disk_usage(base).free>=4*1024**3
source=json.loads(Path('/home/ubuntu/spann-two-machine-v1/epoch2/input/3m.json').read_text())
expected='18fb3c71c45bb70d28730f8edfdbd2ae90a3361b150a4871f09d870cb6684aba'
assert source['rows']==3000000 and source['dimensions']==768 and source['stride_bytes']==3072 and source['offset_bytes']==0 and source['first_id']==0 and source['file_bytes']==9216000000 and source['sha256']==expected
path=Path(source['path']);assert path.is_absolute() and path.stat().st_size==9216000000
out=base/'input';out.mkdir(exist_ok=False);target=out/'fineweb500k.f32';full=hashlib.sha256();prefix=hashlib.sha256();remaining=1536000000;started=time.time()
with path.open('rb') as src,target.open('xb') as dst:
 while True:
  assert time.time()<min(a.deadline-30,started+150)
  block=src.read(8*1024**2)
  if not block:break
  full.update(block);take=block[:remaining];dst.write(take);prefix.update(take);remaining-=len(take)
 dst.flush();os.fsync(dst.fileno())
assert full.hexdigest()==expected and remaining==0 and target.stat().st_size==1536000000
value={k:v for k,v in source.items() if k!='immutable_locators'}
value.update(path=str(target),rows=500000,file_bytes=1536000000,sha256=prefix.hexdigest(),immutable_locators=source.get('immutable_locators',[])[:5])
(out/'500k.json').write_text(json.dumps(value,indent=2)+'\n')
receipt={'identity':identity,'source_path':str(path),'source_sha256':full.hexdigest(),'prefix_sha256':prefix.hexdigest(),'prefix_bytes':1536000000,'rows':500000,'elapsed_seconds':time.time()-started,'network_download_bytes':0,'descriptor':value}
(base/'prefix-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
