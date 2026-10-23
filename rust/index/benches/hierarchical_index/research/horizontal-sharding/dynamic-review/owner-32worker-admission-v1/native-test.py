"""Exercise the real optional native policy on a bounded synthetic input."""
import argparse,pathlib,json,hashlib,struct,subprocess,os,signal,time
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',required=True);a=p.parse_args();out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=False)
n=65;dim=32;data=b''.join(struct.pack('<f',-0.0 if d==2 else (i%8)*10+d/32)for i in range(n)for d in range(dim));source=out/'input.f32';source.write_bytes(data);sha=hashlib.sha256(data).hexdigest()
base=dict(protocol='reference-owner-parallel-v1',source_id='owner-stable-native-129-v1',vectors=dict(path=str(source),bytes=len(data),sha256=sha),prefix_sha256=sha,rows=n,dimension=dim,metric='l2',config=dict(branching_factor=4,split_threshold=16,merge_threshold=0,write_beam_tau=2.,write_beam_min=2,write_beam_max=4,write_rng_epsilon=0.,write_rng_factor=1.,reassign_neighbor_count=8,write_navigation='fp',fp_npa=True),treatment='owner',wide_identity=True,threads=4,balance_every=33,owner_slots=2,move_above=32,predicate_batching=True,batched_scrub_validation=True,batched_split_input_validation=True,bounded_vectors=True,staging_rows=64,staging_working_bytes=1048576,split_rows=256,split_working_bytes=4194304,max_owner_physical_rows=512,max_audit_nodes=512,max_audit_rows=256,construction_observation=True,topology_cache_entries=2,topology_cache_payload_bytes=1024,topology_record_payload_bytes=512,topology_wave_rows=4,full_records=dict(node_cache_entries=2,node_cache_bytes=1024,node_record_bytes=512,node_pin_bytes=1048576,node_dirty_records=256,node_dirty_bytes=131072,queue_cache_pages_per_home=1,queue_max_active_pages=256,placement_cache_pages_per_owner=1,placement_active_pages_per_owner=256,placement_pages_per_phase=8,placement_moves_per_phase=16))
base['stable_insert']=dict(rows=64,worker_bytes=1048576)
metadata_policy=dict(clean_records=8,dirty_records=128,pin_records=256,record_bytes=256,working_bytes=100352)
byte_policy=dict(target_bytes=4194304,quantum_bytes=4096)
base.update(threads=32,owner_slots=1,parallel_working_bytes=256*1024*1024)
receipts=[]
for name,extra,valid in [('worker32',{},True),('reject33',{'threads':33},False),('reject-parallel-budget',{'parallel_working_bytes':1},False),('reject-parallel-overflow',{'stable_insert':{'rows':64,'worker_bytes':18446744073709551615}},False)]:
 spec=dict(base,output=str(out/name),**extra)
 path=out/(name+'.spec.json');path.write_text(json.dumps(spec,indent=2)+'\n')
 with open(out/(name+'.stdout'),'wb')as stdout,open(out/(name+'.stderr'),'wb')as stderr:
  child=subprocess.Popen([a.binary,'reference-owner-diagnostic','--spec',str(path)],stdout=stdout,stderr=stderr,start_new_session=True,env=dict(os.environ,TOKIO_WORKER_THREADS='32',RAYON_NUM_THREADS='32'))
  try:code=child.wait(timeout=30)
  except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait();raise
 if not valid:assert code!=0 and not(out/name).exists();receipts.append(dict(name=name,exit_code=code,rejected_before_output=True));continue
 assert code==0,(out/(name+'.stderr')).read_text()[-4000:]
 r=json.loads((out/name/'result.json').read_text());assert r['census']['current_rows']==65 and r['census']['vector_bits_verified'] and r['census']['query_exact_distance_bits_verified']
 assert r['spec']['threads']==32 and r['spec']['owner_slots']==1
 assert r['after_save']['resident_mutation_nodes']==0
 assert r['after_save']['vector_store']['configured_fetch_workers']==32 and r['after_save']['vector_store']['configured_readers_per_owner']==32
 assert r['parallel_working_admission']['workers']==32 and r['parallel_working_admission']['modeled_bytes']<=256*1024*1024
 receipts.append(dict(name=name,exit_code=code,census=65,exact_bits=True,admission=r['parallel_working_admission'],stable_peak_windows=r['after_save']['stable_insertion']['peak_active_windows'],peak_fetch_workers=r['after_save']['vector_store']['peak_fetch_workers'],actual_splits=r['after_save']['counts']['splits']))
result=dict(status='passed',cases=receipts,binary_sha256=hashlib.sha256(pathlib.Path(a.binary).read_bytes()).hexdigest(),scope='Tiny 32-thread single-owner admission and saved correctness wiring; not evidence of 32 physical cores or throughput.')
(out/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
