import concurrent.futures,hashlib,json,pathlib,subprocess,time
base=pathlib.Path('/mnt/spann');old=json.load(open(base/'results/fineweb1m128-h2-a16-c16/case.json'));name='fineweb1m-append100k-integration-h1-v1';dest=base/name
assert not dest.exists();dest.mkdir()
start=time.monotonic()
subprocess.run(['scp','-r','ubuntu@172.31.69.252:/mnt/spann/fineweb-append100k-128-v1',str(base)],check=True)
p=base/'fineweb-append100k-128-v1/input.json';assert hashlib.sha256(p.read_bytes()).hexdigest()=='aa9bcbc44a3006ab8933ccfc7d27a26033b29c5853c5b18e9afc12739816e64c';prep=json.load(open(p))
entries={}
for sid,e in old['entries'].items():
 source=e['index'] if e['host']=='ubuntu@172.31.69.252' else '/mnt/spann/fineweb1m128-h2-a16-c16/cohost/'+sid
 entries[sid]=dict(host='ubuntu@172.31.69.252',index='/mnt/spann/'+name+'/indexes/'+sid,vectors='/mnt/spann/fineweb-append100k-128-v1/owners/'+sid+'/vectors.f32',id_map='/mnt/spann/fineweb-append100k-128-v1/owners/'+sid+'/local-to-global.u64le',source=source)
code='''import pathlib,shutil,hashlib,json,time
rows=INPUT
out=[]
for e in rows:
 a=pathlib.Path(e['source']);b=pathlib.Path(e['index']);assert not b.exists();shutil.copytree(a,b)
 files=0;size=0
 for p in a.rglob('*'):
  if p.is_file():
   assert not p.is_symlink();q=b/p.relative_to(a);assert hashlib.sha256(p.read_bytes()).digest()==hashlib.sha256(q.read_bytes()).digest();files+=1;size+=p.stat().st_size
 out.append(dict(index=str(b),files=files,bytes=size))
print(json.dumps(out))
'''.replace('INPUT',repr(list(entries.values())))
res=subprocess.run(['ssh','ubuntu@172.31.69.252','python3 -'],input=code,text=True,capture_output=True,check=True);(dest/'clone-inventory.json').write_text(res.stdout)
for e in entries.values():e.pop('source')
config=dict(binary='/mnt/spann/hierarchical_spann_shard-audited-v3',measured_binary='/mnt/spann/measured_worker_v4',cpu_affinity='0-7,32-39',jobs_per_host=16,generation=2,timeout=300,entries=entries)
(dest/'config.json').write_text(json.dumps(config));q=json.loads(open(base/'fineweb1m-pilot-data/calibration.jsonl').readline());q.update(k=100,beam_width=512,candidate_budget=1000);(dest/'queries.jsonl').write_text(json.dumps(q)+'\n')
(dest/'staging.json').write_text(json.dumps(dict(elapsed_s=time.monotonic()-start,prepared_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),clone_count=len(entries),timing_scope='Input staging and complete fresh-clone hash verification, excluded from compaction.')))
print(dest,flush=True)
