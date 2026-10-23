"""Independently bind preserved owner inventories to the published 1M tree."""
import hashlib
import json
from pathlib import Path
import tarfile
BASE=Path(__file__).resolve().parents[1]
tree=json.loads((BASE/'fineweb1m-publication-metadata/fineweb1m128-h2-a16-c16/tree-1.json').read_text())
slots={s['subtree_id']:s for s in tree['subtrees']}
rows={}; archives={}; hosts={}
keys=('current_live_objects','retained_union_live_objects','physical_object_store','total_owned_checkpoint_regular_files')
for host in (0,1):
 path=BASE/f'fineweb1m-storage-worker{host}.tar.gz';archives[path.name]=hashlib.sha256(path.read_bytes()).hexdigest()
 group=[]
 with tarfile.open(path) as archive:
  for member in archive:
   if not member.isfile() or not member.name.endswith('.json'):continue
   record=json.load(archive.extractfile(member))
   if 'report' not in record:continue
   report=record['report'];sid=record['subtree_id'];slot=slots[sid]
   assert sid not in rows and record['exit_status']==0
   assert report['current_manifest_sha256']==record['expected_current_sha256']==slot['snapshot']['sha256']
   assert report['current_generation']==slot['generation']==1
   assert report['identity']['shard_id']==sid
   assert report['identity']['source_id']==tree['source']['source_id']
   assert report['no_cleanup'] and not report['read_work']['raw_block_payloads_read']
   assert len(report['generations'])==1
   assert report['generations'][0]['vector_count']==slot['vector_count']
   assert report['generations'][0]['id_mapping']['sha256']==slot['local_to_global']['sha256']
   rows[sid]=report;group.append(report)
 hosts[str(host)]={key:{field:sum(r[key][field] for r in group) for field in ('logical_file_bytes','unique_inode_file_bytes','allocated_bytes_unix','files')} for key in keys}
 hosts[str(host)]['owners']=len(group)
assert set(rows)==set(slots) and len(rows)==128
summary={key:{field:sum(r[key][field] for r in rows.values()) for field in ('logical_file_bytes','unique_inode_file_bytes','allocated_bytes_unix','files')} for key in keys}
summary.update(status='passed',owners=len(rows),vector_count=sum(s['vector_count'] for s in slots.values()),hosts=hosts,archives=archives,unknown_paths=sum(len(r['unknown_paths']) for r in rows.values()),unreferenced_logical_bytes=sum(r['not_referenced_by_enumerated_retained_manifests']['summary']['logical_file_bytes'] for r in rows.values()))
summary['logical_object_bytes_per_vector']=summary['current_live_objects']['logical_file_bytes']/summary['vector_count']
summary['physical_over_current_object_ratio']=summary['physical_object_store']['logical_file_bytes']/summary['current_live_objects']['logical_file_bytes']
summary['limits']=['Only one committed generation and one flush per owner are measured.','Current objects include stale logical entries inside reachable blocks.','Staging copies and peak build scratch are unmeasured.','Allocated sums do not deduplicate cross-file reflink extents.','These measurements do not establish repeated-checkpoint or 10M capacity bounds.']
(BASE/'storage-audit-v1/independent-inventory-review.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
