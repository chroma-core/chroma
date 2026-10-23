"""Stage a new guarded compaction case; this never launches writers."""
import concurrent.futures,hashlib,json,pathlib,subprocess,sys,time
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.build import command
from tools.hierarchical_sharding.guarded_compaction_backend import validate_jobs
base=pathlib.Path('/mnt/spann');name=sys.argv[1];hosts_count=int(sys.argv[2]);assert hosts_count in (1,2);stage=base/name;stage.mkdir(exist_ok=False);start=time.monotonic();old=json.load(open(base/'results/fineweb1m128-h2-a16-c16/case.json'));prepared=base/'fineweb-append100k-128-v1/input.json';prep=json.load(open(prepared));assert hashlib.sha256(prepared.read_bytes()).hexdigest()=='aa9bcbc44a3006ab8933ccfc7d27a26033b29c5853c5b18e9afc12739816e64c';slots={x['subtree_id']:x for x in prep['subtrees']};entries={};mask='0-7,32-39'
for sid,e in old['entries'].items():
 host=e['host'] if hosts_count==2 else old['host_order'][0];source=e['index'] if host==e['host'] else '/mnt/spann/fineweb1m128-h2-a16-c16/cohost/'+sid
 entries[sid]=dict(host=host,index=str(stage/'indexes'/sid),source=source,vectors=str(prepared.parent/slots[sid]['vectors']['path']),id_map=str(prepared.parent/slots[sid]['local_to_global']['path']))
def remote(host,code):
 r=subprocess.run(['ssh',host,'python3 -'],input=code,text=True,capture_output=True,check=True);return json.loads(r.stdout)
clones=[]
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
'''.replace('INPUT',repr([e for e in entries.values() if e['host']==host]));clones.append(dict(host=host,owners=remote(host,code)))
(stage/'clone-inventory.json').write_text(json.dumps(clones))
for e in entries.values():e.pop('source')
jobs=[]
for slot in prep['subtrees']:
 sid=slot['subtree_id'];e=entries[sid];assert slot['append_row_count']>0
 jobs.append(dict(host=e['host'],binary=old['measured_binary'],vectors=e['vectors'],id_map=e['id_map'],checkpoint_dir=e['index'],shard_id=sid,source_id=prep['source_id'],dimension=prep['dimension'],row_count=slot['append_row_count'],global_id_start=slot['local_start'],id_stride=1,generation=2,metric=prep['metric'],threads=1,runtime_threads=2,cpu_affinity=mask,checkpoint_rows=slot['append_row_count'],boundary_hash=slot['routing_boundary_hash'],resume=True))
batch=dict(hosts=[],heartbeat_interval_s=10,poll_interval_s=.2,control_timeout_s=5,timeout_s=600)
for number,host in enumerate(sorted({e['host'] for e in entries.values()})):
 local=[j for j in jobs if j['host']==host];run_id=name+'-'+str(number);output=stage/('supervisor-'+str(number));config=dict(run_id=run_id,cpu_affinity=mask,lease_path=str(stage/('lease-'+str(number)+'.json')),lease_ttl_s=60,absolute_deadline_unix=time.time()+900,guard=dict(commands=[command(dict(j,host='local')) for j in local],concurrency=16,scopes=[dict(path=str(stage),role='fresh-clones-and-supervisor-control'),dict(path=str(prepared.parent),role='canonical-public-delta-input')],abort_plan='verified-process-groups-no-detach',sample_interval_s=1,termination_grace_s=2,timeout_s=600,limits=dict(scope_bytes_cap=64*1024**3,scope_stop_margin_bytes=8*1024**3,disk_free_floor_bytes=512*1024**3,disk_stop_margin_bytes=64*1024**3,rss_cap_bytes=64*1024**3,rss_stop_margin_bytes=8*1024**3)))
 path=stage/('host-'+str(number)+'.json');payload=json.dumps(config,sort_keys=True).encode();path.write_bytes(payload);remote(host,'import pathlib,json\np=pathlib.Path('+repr(str(path))+');assert not p.exists();p.write_bytes('+repr(payload)+')\nprint("true")\n');batch['hosts'].append(dict(host=host,config_path=str(path),local_config_path=str(path),config_sha256=hashlib.sha256(payload).hexdigest(),run_id=run_id,output_path=str(output),source_root='/mnt/spann/source',job_shard_ids=[j['shard_id'] for j in local]))
validate_jobs(jobs,batch,mask,16);batchpath=stage/'batch.json';batchpath.write_text(json.dumps(batch));config=dict(binary=old['binary'],measured_binary=old['measured_binary'],cpu_affinity=mask,jobs_per_host=16,generation=2,timeout=300,verification_max_hosts=8,guarded_batch_config=str(batchpath),entries=entries);(stage/'config.json').write_text(json.dumps(config));(stage/'queries.jsonl').write_text((base/'fineweb1m-append100k-integration-h1-v3/queries.jsonl').read_text());(stage/'staging.json').write_text(json.dumps(dict(status='staged',elapsed_s=time.monotonic()-start,prepared_sha256=hashlib.sha256(prepared.read_bytes()).hexdigest(),host_count=hosts_count)));print(stage,flush=True)

import tarfile
files=sorted(pathlib.Path("/mnt/spann/source/tools/hierarchical_sharding").glob("*.py"))
(stage/"producer-source.json").write_text(json.dumps(dict(captured_unix=time.time(),files={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}),indent=2))
with tarfile.open(stage/"producer-source.tar.gz","w:gz") as archive:
 for p in files:archive.add(p,arcname=str(p.relative_to("/mnt/spann/source")),recursive=False)
