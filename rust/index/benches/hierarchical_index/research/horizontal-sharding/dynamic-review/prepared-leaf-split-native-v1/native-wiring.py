"""Exercise the real optional native policy on a bounded synthetic input."""
import argparse,pathlib,json,hashlib,struct,subprocess,os,signal,time
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',required=True);a=p.parse_args();out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=False)
n=65;dim=32;data=b''.join(struct.pack('<f',-0.0 if d==2 else (i%8)*10+d/32)for i in range(n)for d in range(dim));source=out/'input.f32';source.write_bytes(data);sha=hashlib.sha256(data).hexdigest()
base=dict(protocol='reference-owner-parallel-v1',source_id='owner-stable-native-129-v1',vectors=dict(path=str(source),bytes=len(data),sha256=sha),prefix_sha256=sha,rows=n,dimension=dim,metric='l2',config=dict(branching_factor=4,split_threshold=16,merge_threshold=0,write_beam_tau=2.,write_beam_min=2,write_beam_max=4,write_rng_epsilon=0.,write_rng_factor=1.,reassign_neighbor_count=8,write_navigation='fp',fp_npa=True),treatment='owner',wide_identity=True,threads=4,balance_every=33,owner_slots=2,move_above=32,predicate_batching=True,batched_scrub_validation=True,batched_split_input_validation=True,bounded_vectors=True,staging_rows=64,staging_working_bytes=1048576,split_rows=256,split_working_bytes=4194304,max_owner_physical_rows=512,max_audit_nodes=512,max_audit_rows=256,construction_observation=True,topology_cache_entries=2,topology_cache_payload_bytes=1024,topology_record_payload_bytes=512,topology_wave_rows=4,full_records=dict(node_cache_entries=2,node_cache_bytes=1024,node_record_bytes=512,node_pin_bytes=1048576,node_dirty_records=256,node_dirty_bytes=131072,queue_cache_pages_per_home=1,queue_max_active_pages=256,placement_cache_pages_per_owner=1,placement_active_pages_per_owner=256,placement_pages_per_phase=8,placement_moves_per_phase=16))
base['stable_insert']=dict(rows=64,worker_bytes=1048576)

base.update(owner_slots=1,immutable_handle_batching=False,owner_eligibility_prefix=False)

base.update(single_owner_local_scrub=True)

receipts=[];digests=[]
base.update(decoded_vector_cache=dict(payload_bytes_per_owner=402653184,aggregate_payload_bytes=402653184),single_owner_local_reassignment=False)
cases=[('default',{},True),('off',dict(prepared_leaf_split=False),True),('on',dict(prepared_leaf_split=True),True),('multi-owner',dict(prepared_leaf_split=True,owner_slots=2,decoded_vector_cache=dict(payload_bytes_per_owner=402653184,aggregate_payload_bytes=805306368)),False),('local-reassignment',dict(prepared_leaf_split=True,single_owner_local_reassignment=True),False),('paged',dict(prepared_leaf_split=True,paged_metadata=dict(clean_records=8,dirty_records=128,pin_records=256,record_bytes=256,working_bytes=1048576)),False),('no-full',dict(prepared_leaf_split=True,full_records=None),False),('reference',dict(prepared_leaf_split=True,treatment='reference',decoded_vector_cache=None),False)]
for name,delta,valid in cases:
 spec=dict(base,output=str(out/name),**delta);path=out/(name+'.spec.json');path.write_text(json.dumps(spec,indent=2)+'\n');start=time.monotonic()
 with open(out/(name+'.stdout'),'wb')as stdout,open(out/(name+'.stderr'),'wb')as stderr:
  child=subprocess.Popen([a.binary,'reference-owner-diagnostic','--spec',str(path)],stdout=stdout,stderr=stderr,start_new_session=True,env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4'))
  try:code=child.wait(timeout=30)
  except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait();raise
 receipts.append(dict(name=name,exit_code=code,seconds=time.monotonic()-start))
 if not valid:
  assert code!=0 and not(out/name).exists()
  assert 'prepared split requires bounded full-record wide nonpaged single owner with local reassignment OFF' in (out/(name+'.stderr')).read_text()
  continue
 assert code==0,(out/(name+'.stderr')).read_text()[-4000:]
 result=json.loads((out/name/'result.json').read_text());report=result['before_save'];policy=report['prepared_leaf_split'];assert result['census']['current_rows']==n and result['census']['vector_bits_verified'] and result['census']['query_exact_distance_bits_verified'];assert report['resident_mutation_nodes']==0
 assert report['cleanup']['effective_local'] and report['immutable_handle_resolution_mode']=='owner-scalar' and not report['owner_eligibility_prefix']['enabled'] and not report['local_reassignment']['effective']
 enabled=name=='on';assert result['spec']['prepared_leaf_split']==enabled and policy['enabled']==enabled
 if enabled:assert policy['captured']>0 and policy['installed_before_flush']>0
 else:assert policy['captured']==0 and policy['installed_before_flush']==0 and policy['discarded_before_detach']==0
 timing=report['split_preparation_timing'];assert timing['clustering_elapsed_ns']>0 and timing['quantization_elapsed_ns']>0
 assert report['vector_store']['split_working_rejections']==0
 receipts[-1].update(prepared_leaf_split=policy,split_preparation_timing=timing,counts=report['counts']);digests.append(result['census']['all_current_vector_bits_sha256'])
expected=hashlib.sha256(b''.join(struct.pack('<Q',i)+data[i*dim*4:(i+1)*dim*4] for i in range(n))).hexdigest();assert len(set(digests))==1 and digests[0]==expected
(out/'wiring-result.json').write_text(json.dumps(dict(status='passed',receipts=receipts,binary_sha256=hashlib.sha256(pathlib.Path(a.binary).read_bytes()).hexdigest(),input_sha256=sha,vector_bit_digest=digests[0],scope='65-row default/off/on native policy and full saved-bit/query integrity; invalid policies reject before output. Independent stochastic builds are not fixed-work speed evidence.'),indent=2)+'\n');print(json.dumps(receipts))
