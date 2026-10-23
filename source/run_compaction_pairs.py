import concurrent.futures,hashlib,json,pathlib,subprocess,time,sys
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.subtree_compaction import compact
base=pathlib.Path('/mnt/spann');oldpath=base/'results/fineweb1m128-h2-a16-c16/case.json';old=json.load(open(oldpath));prepared=base/'fineweb-append100k-128-v1/input.json';prep=json.load(open(prepared));slots={x['subtree_id']:x for x in prep['subtrees']};assert hashlib.sha256(prepared.read_bytes()).hexdigest()=='aa9bcbc44a3006ab8933ccfc7d27a26033b29c5853c5b18e9afc12739816e64c'
subprocess.run(['scp','-r',str(prepared.parent),'ubuntu@172.31.65.187:/mnt/spann/'],check=True)
root=base/'results/compaction-paired-v1';root.mkdir(exist_ok=False)
def remote(host,code):
 r=subprocess.run(['ssh',host,'python3 -'],input=code,text=True,capture_output=True,check=True);return json.loads(r.stdout)
for pair,arms in enumerate(((2,1),(1,2),(2,1)),1):
 for hosts in arms:
  name=f'fineweb-compaction-p{pair}-h{hosts}-v1';stage=base/name;stage.mkdir();case=base/'results'/name;started=time.monotonic();entries={}
  for sid,e in old['entries'].items():
   host=e['host'] if hosts==2 else 'ubuntu@172.31.69.252'
   source=e['index'] if host==e['host'] else '/mnt/spann/fineweb1m128-h2-a16-c16/cohost/'+sid
   entries[sid]=dict(host=host,index=str(stage/'indexes'/sid),vectors=str(prepared.parent/slots[sid]['vectors']['path']),id_map=str(prepared.parent/slots[sid]['local_to_global']['path']),source=source)
  clone=[]
  for host in sorted({e['host'] for e in entries.values()}):
   code='''import pathlib,shutil,hashlib,json
rows=INPUT
out=[]
for e in rows:
 a=pathlib.Path(e['source']);b=pathlib.Path(e['index']);assert not b.exists();shutil.copytree(a,b);files=[]
 for p in a.rglob('*'):
  if p.is_file():
   assert not p.is_symlink();q=b/p.relative_to(a);sha=hashlib.sha256(p.read_bytes()).hexdigest();assert sha==hashlib.sha256(q.read_bytes()).hexdigest();files.append(dict(path=str(p.relative_to(a)),sha256=sha,bytes=p.stat().st_size))
 out.append(dict(index=str(b),files=files))
print(json.dumps(out))
'''.replace('INPUT',repr([e for e in entries.values() if e['host']==host]));clone.append(dict(host=host,owners=remote(host,code)))
  (stage/'clone-inventory.json').write_text(json.dumps(clone))
  for e in entries.values():e.pop('source')
  config=dict(binary=old['binary'],measured_binary=old['measured_binary'],cpu_affinity='0-7,32-39',jobs_per_host=16,generation=2,timeout=300,entries=entries)
  cfg=stage/'config.json';cfg.write_text(json.dumps(config));queries=stage/'queries.jsonl';queries.write_text((base/'fineweb1m-append100k-integration-h1-v3/queries.jsonl').read_text());staging=time.monotonic()-started
  try:
   r=compact(oldpath,prepared,cfg,case,queries)
   summary={k:r[k] for k in ('status','construction_s','publication_s','through_publication_s','old_reader_checks','tree_sha256')};summary.update(name=name,pair=pair,hosts=hosts,staging_s=staging)
   (stage/'summary.json').write_text(json.dumps(summary));print(json.dumps(summary),flush=True)
   with (root/'completed.jsonl').open('a') as f:f.write(json.dumps(summary)+'\n')
  except BaseException as e:
   with (root/'failures.jsonl').open('a') as f:f.write(json.dumps(dict(name=name,error=str(e)))+'\n')
   raise
