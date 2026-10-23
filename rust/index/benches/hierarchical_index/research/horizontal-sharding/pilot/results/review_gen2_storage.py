import tarfile,json,hashlib
from pathlib import Path
p=Path(__file__).resolve().parent
rows={}; totals={}
fields=['current_live_objects','retained_union_live_objects','physical_object_store','total_owned_checkpoint_regular_files']
for h in (0,1):
 with tarfile.open(p/f'load32-worker{h}-resources.tar.gz',mode='r|gz') as t:
  for m in t:
   if '/compacted-storage-v1/' not in m.name or not m.name.endswith('.json'):continue
   x=json.load(t.extractfile(m))
   if 'report' not in x:continue
   r=x['report'];sid=x['subtree_id'];assert sid not in rows and x['exit_status']==0
   assert r['current_generation']==2 and r['current_manifest_sha256']==x['expected_current_sha256'] and set(g['generation'] for g in r['generations'])=={1,2}
   assert not r['unknown_paths'] and not r['not_referenced_by_enumerated_retained_manifests']['paths']
   for f in fields:
    d=totals.setdefault(f,{})
    for k in ['logical_file_bytes','unique_inode_file_bytes','allocated_bytes_unix','files']:d[k]=d.get(k,0)+r[f][k]
   for g in r['generations']:
    k=f'generation_{g["generation"]}';d=totals.setdefault(k,{'vectors':0,'logical_file_bytes':0});d['vectors']+=g['vector_count'];d['logical_file_bytes']+=g['live_objects']['logical_file_bytes']
   rows[sid]={'manifest_sha256':r['current_manifest_sha256'],'host':h}
assert set(rows)==set(map(str,range(128))) and totals['generation_1']['vectors']==1000000 and totals['generation_2']['vectors']==1100000
out=dict(status='passed',owners=128,totals=totals,unknown_paths=0,unreferenced_objects=0,no_cleanup=True,limits=['Only selected pair3 two-host cloned checkpoint directories are inventoried. Source vectors, staging copies, other case clones and peak scratch are excluded.','Two retained public generations and one append commit are observed; this does not bound multiple private checkpoints or taller trees.','Allocated-byte sums may double-count reflink-shared extents. Raw block payloads are not independently validated.'],owner_bindings=rows)
(p/'gen2-storage-independent-review.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(totals,indent=2))
