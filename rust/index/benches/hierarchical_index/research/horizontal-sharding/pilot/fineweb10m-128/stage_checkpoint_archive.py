"""Stage exact immutable-store archival configs without starting archives."""
import json,pathlib,hashlib,subprocess,time
base=pathlib.Path('/mnt/spann');root=base/'checkpoint-archive-v1';root.mkdir(exist_ok=False);cases=['fineweb10m128-h2-a16-c16-v2','fineweb10m128-h1-a64-c64-v5-r1','fineweb10m128-h2-a64-c64-v5-r1'];query=json.loads((base/'fineweb-pilot-queries/test.jsonl').open().readline())['vector'];assert len(query)==768
for number,(host,instance) in enumerate([('ubuntu@172.31.69.252','i-09b86223b5620b35d'),('ubuntu@172.31.65.187','i-011ff180f03584f40')]):
 stores=[];scopes=[dict(path=str(root),role='all-archive-private-tars-restores-expanded-proofs-control')]
 for name in cases:
  case=json.loads((base/'results'/name/'case.json').read_text());tree=json.loads(pathlib.Path(case['tree']).read_text());slots=[]
  for s in tree['subtrees']:
   entry=case['entries'][s['subtree_id']]
   if entry['host']!=host:continue
   slots.append(dict(subtree_id=s['subtree_id'],generation=s['generation'],vector_count=s['vector_count'],snapshot_sha256=s['snapshot']['sha256'],map_sha256=s['local_to_global']['sha256']))
  if not slots:continue
  source=str(base/name/'indexes');stores.append(dict(name=name,source=source,tree_sha256=case['tree_sha256'],slots=slots));scopes.append(dict(path=source,role='immutable-source-owned-checkpoints-including-private-unreferenced-files'))
 cfg=dict(expected_instance_id=instance,binary='/mnt/spann/hierarchical_spann_shard-memory-v4',binary_sha256='7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158',bucket='spann-artifacts-079819354890-20261014-ecfcca4d',output=str(root/('worker-'+str(number)+'-data')),query=query,stores=stores)
 config=root/('worker-'+str(number)+'-config.json');config.write_text(json.dumps(cfg,indent=2));guard=dict(abort_plan='verified-process-groups-no-detach',commands=[['/mnt/spann/pilot-venv/bin/python',str(root/'producer/archive_checkpoint_epoch.py'),'--config',str(config)]],concurrency=1,timeout_s=3600,sample_interval_s=2,termination_grace_s=3,scopes=scopes,limits=dict(scope_bytes_cap=512*1024**3,scope_stop_margin_bytes=64*1024**3,disk_free_floor_bytes=512*1024**3,disk_stop_margin_bytes=128*1024**3,rss_cap_bytes=4*1024**3,rss_stop_margin_bytes=512*1024**2));guardpath=root/('worker-'+str(number)+'-guard.json');guardpath.write_text(json.dumps(guard,indent=2))
 code='import pathlib,json,sys\nsys.path.insert(0,"/mnt/spann/source")\nfrom tools.hierarchical_sharding.resource_guard import inventory\nr=pathlib.Path('+repr(str(root))+');r.mkdir(exist_ok=False)\n(r/"producer").mkdir()\npathlib.Path('+repr(str(config))+').write_text('+repr(config.read_text())+')\npathlib.Path('+repr(str(guardpath))+').write_text('+repr(guardpath.read_text())+')\nprint(json.dumps(inventory('+repr(scopes)+',1000000)))\n';r=subprocess.run(['ssh',host,'python3 -'],input=code,text=True,capture_output=True,check=True);(root/('worker-'+str(number)+'-inventory.json')).write_text(r.stdout);print(json.dumps(dict(host=host,stores=len(stores),owners=sum(len(s['slots']) for s in stores),inventory=json.loads(r.stdout))),flush=True)
