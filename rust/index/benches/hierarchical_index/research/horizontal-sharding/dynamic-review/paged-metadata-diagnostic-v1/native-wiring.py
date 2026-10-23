"""Exercise the real optional native policy on a bounded synthetic input."""
import argparse,pathlib,json,hashlib,struct,subprocess,os,signal,time
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',required=True);a=p.parse_args();out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=False)
n=65;dim=32;data=b''.join(struct.pack('<f',-0.0 if d==2 else (i%8)*10+d/32)for i in range(n)for d in range(dim));source=out/'input.f32';source.write_bytes(data);sha=hashlib.sha256(data).hexdigest()
base=dict(protocol='reference-owner-parallel-v1',source_id='owner-stable-native-129-v1',vectors=dict(path=str(source),bytes=len(data),sha256=sha),prefix_sha256=sha,rows=n,dimension=dim,metric='l2',config=dict(branching_factor=4,split_threshold=16,merge_threshold=0,write_beam_tau=2.,write_beam_min=2,write_beam_max=4,write_rng_epsilon=0.,write_rng_factor=1.,reassign_neighbor_count=8,write_navigation='fp',fp_npa=True),treatment='owner',wide_identity=True,threads=4,balance_every=33,owner_slots=2,move_above=32,predicate_batching=True,batched_scrub_validation=True,batched_split_input_validation=True,bounded_vectors=True,staging_rows=64,staging_working_bytes=1048576,split_rows=256,split_working_bytes=4194304,max_owner_physical_rows=512,max_audit_nodes=512,max_audit_rows=256,construction_observation=True,topology_cache_entries=2,topology_cache_payload_bytes=1024,topology_record_payload_bytes=512,topology_wave_rows=4,full_records=dict(node_cache_entries=2,node_cache_bytes=1024,node_record_bytes=512,node_pin_bytes=1048576,node_dirty_records=256,node_dirty_bytes=131072,queue_cache_pages_per_home=1,queue_max_active_pages=256,placement_cache_pages_per_owner=1,placement_active_pages_per_owner=256,placement_pages_per_phase=8,placement_moves_per_phase=16))
base['stable_insert']=dict(rows=64,worker_bytes=1048576)
policy=dict(clean_records=8,dirty_records=128,pin_records=256,record_bytes=256,working_bytes=100352)
receipts=[];digests=[]
cases=[('default',None,True),('paged',policy,True),('missing-stable',policy,False),('missing-full-records',policy,False),('budget',dict(policy,working_bytes=100351),False),('overflow',dict(policy,clean_records=2**64-1),False),('pin-cap',dict(policy,pin_records=63),False)]
for name,selected,valid in cases:
 spec=dict(base,output=str(out/name))
 if selected is not None:spec['paged_metadata']=selected
 if name=='missing-stable':spec.pop('stable_insert')
 if name=='missing-full-records':spec.pop('full_records')
 path=out/(name+'.spec.json');path.write_text(json.dumps(spec,indent=2)+'\n');start=time.monotonic()
 with open(out/(name+'.stdout'),'wb')as stdout,open(out/(name+'.stderr'),'wb')as stderr:
  child=subprocess.Popen([a.binary,'reference-owner-diagnostic','--spec',str(path)],stdout=stdout,stderr=stderr,start_new_session=True,env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4'))
  try:code=child.wait(timeout=30)
  except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait();raise
 receipts.append(dict(name=name,exit_code=code,seconds=time.monotonic()-start))
 if not valid:assert code!=0 and not (out/name).exists();continue
 assert code==0,(out/(name+'.stderr')).read_text()[-4000:]
 r=json.loads((out/name/'result.json').read_text());assert r['census']['current_rows']==n and r['census']['vector_bits_verified'] and r['census']['query_exact_distance_bits_verified']
 assert r['after_save']['resident_mutation_nodes']==0
 descriptor=json.loads((out/name/'snapshot64.json').read_text())
 if selected:
  effective=r['effective_paged_metadata_policy'];assert effective['aggregate_accounted_bytes']==100352 and effective['home_ids']==[0] and effective['working_bytes']==100352
  assert r['after_save']['vector_metadata']['clean_record_cap']==8
  assert descriptor['format']=='hierarchical-spann-posting64-paged-metadata-v1' and list(descriptor['metadata_roots'])==['0'] and descriptor['owner_slots']==2
  assert r['saved_reader_metadata']['saved']['cache_cap']==8
 else:assert r['effective_paged_metadata_policy'] is None and descriptor['metadata_roots'] is None and descriptor['format']=='hierarchical-spann-posting64-v1'
 digests.append(r['census']['all_current_vector_bits_sha256'])
assert digests[0]==digests[1]
result=dict(status='passed',receipts=receipts,binary_sha256=hashlib.sha256(pathlib.Path(a.binary).read_bytes()).hexdigest(),input_sha256=sha,vector_bit_digest=digests[0],scope='65-row dev-profile native option/validation/integrity wiring. No throughput or quality comparison; independent construction schedules.')
(out/'wiring-result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
