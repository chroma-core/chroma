import collections,hashlib,json,pathlib,statistics,sys
import numpy as np
ROOT=pathlib.Path(__file__).resolve().parent
SHA=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
CANON=lambda x:(json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
HASH=lambda x:hashlib.sha256(CANON(x)).hexdigest()
results=[];common=None
for directory in sorted(p for p in ROOT.iterdir() if p.is_dir()):
 case=json.loads((directory/'case.json').read_text());tree=json.loads((directory/'tree-1.json').read_text());pointer=json.loads((directory/'current.json').read_text());inspections=json.loads((directory/'remote-reopen.json').read_text());jobs=json.loads((directory/'jobs.json').read_text());build=json.loads((directory/'build/build.json').read_text());outcome=json.loads((directory/'build-outcome.json').read_text())
 assert HASH(tree)==case['tree_sha256']==pointer['tree_sha256'];assert pointer['tree_generation']==tree['tree_generation']==1
 assert json.loads((directory/pointer['manifest']).read_text())==tree
 assert len(tree['subtrees'])==len(inspections)==len(jobs)==len(build['jobs'])==128
 assert build['complete'] and outcome['complete'] and outcome['fully_converged'] and not outcome['balance_warnings']
 assert all(r['status']=='ok' and r['returncode']==0 for r in build['jobs'])
 slots={s['subtree_id']:s for s in tree['subtrees']};remote={s['subtree_id']:s for s in inspections};jobmap={s['shard_id']:s for s in jobs};assert len(slots)==len(remote)==len(jobmap)==128 and set(slots)==set(remote)==set(jobmap)
 maps=[];counts=[];audits=[];construction=set();roots=0
 for sid,slot in slots.items():
  snapshot=directory/slot['snapshot']['path'];mapping=directory/slot['local_to_global']['path'];m=json.loads(snapshot.read_text());r=remote[sid];response=r['root_response'];a=m['integrity_audit'];coverage=a['coverage']
  assert SHA(snapshot)==slot['snapshot']['sha256']==r['snapshot_sha256'];assert snapshot.stat().st_size==slot['snapshot']['bytes']
  assert SHA(mapping)==slot['local_to_global']['sha256']==r['mapping_sha256']==m['id_mapping']['sha256'];assert mapping.stat().st_size==slot['local_to_global']['bytes']==m['id_mapping']['bytes']==8*slot['vector_count']
  assert m==r['manifest'];assert m['generation']==slot['generation']==r['generation']==response['generation']==1
  assert m['shard_id']==response['shard_id']==sid and m['source_id']==tree['source']['source_id']
  assert m['dimension']==response['dimension']==tree['dimension']==768 and m['metric']==response['metric']==tree['metric']=='ip'
  assert m['vector_count']==response['vector_count']==m['id_mapping']['count']==slot['vector_count']==jobmap[sid]['row_count']
  assert m['root_ref']==slot['root_ref'];assert response['root']['node_id']==slot['root_ref']['id'] and response['root']['is_leaf']==(slot['root_ref']['kind']=='leaf')
  assert response['id_mapping']['sha256']==r['mapping_sha256'];assert a['generation']==m['generation'] and a['block_ids']==m['block_ids'] and a['id_map_sha256']==m['id_mapping']['sha256']
  assert coverage['expected_rows']==coverage['unique_live_rows']==slot['vector_count'];assert coverage['physical_entries']==coverage['stale_entries']+coverage['duplicate_current_entries']+coverage['unique_live_rows'];assert coverage['raw_embedding_reads']==0
  assert coverage['reachable_nodes']+coverage['unreachable_nodes']==coverage['stored_nodes'];assert len(m['block_ids'])==5
  assert m['routing_boundary_hash']==slot['routing_boundary_hash']==HASH({'model_sha256':tree['model']['sha256'],'centroid_index':slot['centroid_index']})
  maps.append(np.fromfile(mapping,dtype='<u8'));counts.append(slot['vector_count']);audits.append(coverage);construction.add(HASH(m['construction']));roots+=len(m['block_ids'])
 combined=np.sort(np.concatenate(maps));assert np.array_equal(combined,np.arange(tree['source']['row_count'],dtype=np.uint64));assert sum(counts)==1000000
 assert len(construction)==1
 signature=(tree['construction_epoch'],tree['model']['sha256'],tree['source']['source_id'],tuple(sorted((s['subtree_id'],s['vector_count'],s['local_to_global']['sha256'],s['routing_boundary_hash']) for s in slots.values())),next(iter(construction)))
 assert common is None or common==signature;common=signature
 stats=dict(owner_min=min(counts),owner_max=max(counts),owner_median=statistics.median(counts),owner_mean=statistics.mean(counts),largest_fraction=max(counts)/1000000,largest_over_mean=max(counts)/statistics.mean(counts),checkpoint_rows_equal_owner_rows=all(j['checkpoint_rows']==j['row_count'] for j in jobs),physical_entries=sum(a['physical_entries'] for a in audits),duplicate_current_entries=sum(a['duplicate_current_entries'] for a in audits),stale_entries=sum(a['stale_entries'] for a in audits),stored_nodes=sum(a['stored_nodes'] for a in audits),unreachable_nodes=sum(a['unreachable_nodes'] for a in audits),maximum_leaf_entries=max(a['peak_leaf_entries'] for a in audits))
 results.append(dict(case=directory.name,tree_sha256=HASH(tree),snapshots_verified=128,mapping_rows_verified=len(combined),root_ids_bound=roots,stats=stats))
report=dict(status='passed',cases=len(results),snapshot_map_and_audit_bindings_verified=128*len(results),dense_global_mapping_rows_verified=1000000*len(results),shared_prepared_ownership_and_construction_config=True,results=results,limits=['Index block data is intentionally absent; audits and root responses are verified as immutable published evidence, not independently rerun.','Upper model payload is absent; the common model checksum and every routing-boundary binding are verified.','Committed live block bytes, physical checkpoint-store bytes, staging copies and peak scratch remain unmeasured pending storage inventory.','Every job flushes after the entire owner input in this1M matrix; it does not measure repeated private checkpoints at a fixed smaller row interval.'])
(ROOT/'independent-publication-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
