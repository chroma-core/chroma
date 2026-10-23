import pathlib,subprocess,sys,json,time
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.subtree_compaction import compact
base=pathlib.Path('/mnt/spann');root=base/'results/guarded-compaction-extra-v1';root.mkdir(exist_ok=False)
for pair,arms in ((2,(1,2)),(3,(2,1))):
 for hosts in arms:
  if (root/'stop-before-next-run').exists():
   (root/'stopped-before-next-run.json').write_text(json.dumps(dict(status='yielded-to-10M',pair=pair,hosts=hosts,time_unix=time.time())));sys.exit(0)
  name=f'fineweb-compaction-guarded-p{pair}-h{hosts}-v1';subprocess.run(['python3','/mnt/spann/stage_guarded_compaction.py',name,str(hosts)],check=True)
  case=base/'results'/name;stage=base/name
  try:
   r=compact(base/'results/fineweb1m128-h2-a16-c16/case.json',base/'fineweb-append100k-128-v1/input.json',stage/'config.json',case,stage/'queries.jsonl');summary={k:r[k] for k in ('status','construction_s','publication_s','through_publication_s','old_reader_checks','tree_sha256')};summary.update(name=name,pair=pair,hosts=hosts,host_writer_spans=r['index_build']['host_writer_spans'],driver_launch_control_s=r['index_build']['driver_launch_control_s'],metadata_log_collection_s=r['index_build']['metadata_log_collection_s']);print(json.dumps(summary),flush=True)
   with (root/'completed.jsonl').open('a') as f:f.write(json.dumps(summary)+'\n')
  except BaseException as e:
   with (root/'failures.jsonl').open('a') as f:f.write(json.dumps(dict(name=name,error=str(e)))+'\n')
   raise
