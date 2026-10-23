"""Exercise explicit activation and reporting through the real native entrypoint."""
import argparse,hashlib,json,pathlib,struct,subprocess,time,os,signal
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',required=True);a=p.parse_args();out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=False)
n=64;dim=32;data=b''.join(struct.pack('<f',-0.0 if d==2 else ((i*17+d*13)%97)/19.0+(100*(i//8) if d==0 else 0)) for i in range(n) for d in range(dim));source=out/'synthetic.f32';source.write_bytes(data);digest=hashlib.sha256(data).hexdigest()
config=dict(branching_factor=4,split_threshold=16,merge_threshold=4,write_beam_tau=2.,write_beam_min=2,write_beam_max=4,write_rng_epsilon=0.,write_rng_factor=1.,reassign_neighbor_count=8,write_navigation='fp',fp_npa=True)
policy=dict(node_cache_entries=2,node_cache_bytes=1024,node_record_bytes=512,node_pin_bytes=131072,node_dirty_records=128,node_dirty_bytes=65536,queue_cache_pages_per_home=1,queue_max_active_pages=128,placement_cache_pages_per_owner=1,placement_active_pages_per_owner=128,placement_pages_per_phase=8,placement_moves_per_phase=16)
base=dict(protocol='reference-owner-parallel-v1',source_id='full-record-native-wiring-64x32-v1',vectors=dict(path=str(source),bytes=len(data),sha256=digest),prefix_sha256=digest,rows=n,dimension=dim,metric='l2',config=config,treatment='owner',wide_identity=True,threads=4,balance_every=17,owner_slots=2,move_above=16,predicate_batching=True,batched_scrub_validation=True,batched_split_input_validation=True,bounded_vectors=True,staging_rows=8,staging_working_bytes=1048576,split_rows=64,split_working_bytes=4194304,max_owner_physical_rows=512,max_audit_nodes=512,max_audit_rows=128,construction_observation=True,topology_cache_entries=2,topology_cache_payload_bytes=1024,topology_record_payload_bytes=512,topology_wave_rows=2)
results={};receipts=[]
for arm in ['resident-default','full-records','invalid-overlap']:
 spec=dict(base,output=str(out/arm))
 if arm!='resident-default':spec['full_records']=policy
 if arm=='invalid-overlap':spec['live_topology']=True
 path=out/(arm+'.spec.json');path.write_text(json.dumps(spec,indent=2)+'\n');start=time.monotonic()
 with open(out/(arm+'.stdout'),'wb')as stdout,open(out/(arm+'.stderr'),'wb')as stderr:
  child=subprocess.Popen([a.binary,'reference-owner-diagnostic','--spec',str(path)],stdout=stdout,stderr=stderr,start_new_session=True)
  try:code=child.wait(timeout=30)
  except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait();code=124
 receipts.append(dict(arm=arm,exit_code=code,seconds=time.monotonic()-start))
 if arm=='invalid-overlap':assert code!=0 and not (out/arm).exists();continue
 assert code==0,(out/(arm+'.stderr')).read_text()[-2000:]
 r=json.loads((out/arm/'result.json').read_text());results[arm]=r;assert r['census']['current_rows']==n and r['census']['vector_bits_verified'] and r['census']['query_exact_distance_bits_verified'];assert r['phase_count']==4;assert r['timing_accounting']['unbucketed_elapsed_seconds']>=0;assert 'census_memory_before'in r and 'census_memory_after'in r
 if arm=='full-records':
  assert r['spec']['full_records']==policy and r['before_save']['resident_mutation_nodes']==0
  for phase in r['phases']:
   assert phase['topology_after_add']['policy']=='fixed-home-full-mutation-records-v1'
   assert phase['topology_after_placement']['cache_entries']<=2
   assert phase['placement_after_phase']['catalog']['policy']=='bounded-owner-page-greedy-v1'
  report=r['after_save']['full_records'];assert report['point_reads']>0 and report['point_miss_elapsed_ns']>0 and report['flush_calls']>0 and report['flush_call_elapsed_ns']>=report['flush_gate_wait_ns']
  assert r['after_save']['pending_balance']==0
 else:assert r['spec']['full_records']is None and r['before_save']['resident_mutation_nodes']>0
assert results['resident-default']['census']['all_current_vector_bits_sha256']==results['full-records']['census']['all_current_vector_bits_sha256']
summary=dict(status='passed',receipts=receipts,rows=n,dimension=dim,source_sha256=digest,binary_sha256=hashlib.sha256(pathlib.Path(a.binary).read_bytes()).hexdigest(),full_policy=policy,census_bit_digest=results['full-records']['census']['all_current_vector_bits_sha256'],scope='Unoptimized native wiring/integrity only. Independent construction schedules do not establish tree equality, quality or performance. RSS values may be null on macOS.')
(out/'wiring-result.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary))
