import tarfile,json,hashlib,sys
from pathlib import Path
sys.path.insert(0,'/private/tmp/chroma-hierarchical-spann-sharding')
from tools.hierarchical_sharding.subtree_guarded_build import jobs_for
from tools.hierarchical_sharding.build import command
from tools.hierarchical_sharding.guarded_compaction_backend import covered
p=Path(__file__).resolve().parents[1];archive=p/'10m-config-admission-v1.tar.gz';base='fineweb10m128-h2-a16-c16-v1/'
with tarfile.open(archive) as t:
 raw=lambda n:t.extractfile(n).read()
 load=lambda n:json.loads(raw(n))
 config=load(base+'config.json');batch=load(base+'batch.json');prepared=load('fineweb10m-128-v1/input.json');staging=load(base+'staging.json');jobs,entries=jobs_for(prepared,config)
 assert len(jobs)==128 and sum(j['row_count'] for j in jobs)==10000000 and config['checkpoint_rows']==10000
 loads={h:0 for h in config['host_order']};slots=prepared['subtrees'];byid={s['subtree_id']:s for s in slots}
 for s in sorted(slots,key=lambda s:(-s['vector_count'],s['subtree_id'])):
  h=min(loads,key=lambda h:(loads[h],config['host_order'].index(h)));assert entries[s['subtree_id']]['host']==h;loads[h]+=s['vector_count']
 assert loads==staging['assigned_rows']
 details=[]
 for i,e in enumerate(batch['hosts']):
  payload=raw(base+f'host-{i}.json');assert hashlib.sha256(payload).hexdigest()==e['config_sha256'];h=json.loads(payload);g=h['guard'];scope=g['scopes'];owners=e['job_shard_ids']
  assert h['run_id']==e['run_id'] and h['cpu_affinity']==config['cpu_affinity']=='0-7,32-39' and g['concurrency']==config['jobs_per_host']==16
  assert h['lease_ttl_s']==60 and g['sample_interval_s']==1 and g['timeout_s']==3300 and g['abort_plan']=='verified-process-groups-no-detach'
  selected=[next(j for j in jobs if j['host']==e['host'] and j['shard_id']==sid) for sid in owners]
  assert len(selected)==64 and g['commands']==[command(dict(j,host='local')) for j in selected]
  for j in selected:
   assert j['threads']==1 and j['runtime_threads']==2 and j['checkpoint_rows']==10000
   for f in ['vectors','id_map','checkpoint_dir']:assert covered(j[f],scope)
  assert covered(e['output_path'],scope) and covered(h['lease_path'],scope)
  lim=g['limits'];assert lim==dict(disk_free_floor_bytes=512*1024**3,disk_stop_margin_bytes=128*1024**3,rss_cap_bytes=64*1024**3,rss_stop_margin_bytes=8*1024**3,scope_bytes_cap=256*1024**3,scope_stop_margin_bytes=32*1024**3)
  inv=staging['scope_inventories'][i]['inventory'];free=min(x['available_bytes'] for x in inv['filesystems'].values());assert free>640*1024**3
  details.append(dict(host=e['host'],owners=64,rows=loads[e['host']],expected_staged_vector_and_map_bytes=sum(byid[sid]['vectors']['bytes']+byid[sid]['local_to_global']['bytes'] for sid in owners),prestage_available_bytes=free,absolute_deadline_unix=h['absolute_deadline_unix']))
 report=dict(status='configuration_passed_launch_conditional',owners=128,rows=10000000,checkpoint_rows=10000,hosts=details,archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),effective_stop_thresholds=dict(owned_unique_inode_logical_gib=224,writer_rss_gib=56,filesystem_available_gib=640),launch_requirements=['All actual staged input, binary and wrapper SHA checks pass.','Final filesystem available space exceeds 640 GiB and owned logical bytes remain below 224 GiB on both hosts.','The deadline leaves enough time for the intended run, and worker-clock leases renew with a 60-second lifetime.','The hardened backend source is deployed on the driver and both workers; remote path ancestry admission passes.'],limitations=['The input scopes are empty in this pre-staging archive; this review does not certify completed staging.','The guard records allocated bytes separately. Scopes include all private objects; sampled logical caps are not hard quotas.','Existing unrelated stores affect filesystem free space but remain outside owned byte counts. No deletion is authorized.'])
(p/'guarded-initial-independent-review/10m-config-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
