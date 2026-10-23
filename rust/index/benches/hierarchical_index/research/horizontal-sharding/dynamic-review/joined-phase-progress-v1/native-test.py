"""Exercise the real optional native policy on a bounded synthetic input."""
import argparse,pathlib,json,hashlib,struct,subprocess,os,signal,time
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',required=True);a=p.parse_args();out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=False)
n=65;dim=32;data=b''.join(struct.pack('<f',-0.0 if d==2 else (i%8)*10+d/32)for i in range(n)for d in range(dim));source=out/'input.f32';source.write_bytes(data);sha=hashlib.sha256(data).hexdigest()
base=dict(protocol='reference-owner-parallel-v1',source_id='owner-stable-native-129-v1',vectors=dict(path=str(source),bytes=len(data),sha256=sha),prefix_sha256=sha,rows=n,dimension=dim,metric='l2',config=dict(branching_factor=4,split_threshold=16,merge_threshold=0,write_beam_tau=2.,write_beam_min=2,write_beam_max=4,write_rng_epsilon=0.,write_rng_factor=1.,reassign_neighbor_count=8,write_navigation='fp',fp_npa=True),treatment='owner',wide_identity=True,threads=4,balance_every=33,owner_slots=2,move_above=32,predicate_batching=True,batched_scrub_validation=True,batched_split_input_validation=True,bounded_vectors=True,staging_rows=64,staging_working_bytes=1048576,split_rows=256,split_working_bytes=4194304,max_owner_physical_rows=512,max_audit_nodes=512,max_audit_rows=256,construction_observation=True,topology_cache_entries=2,topology_cache_payload_bytes=1024,topology_record_payload_bytes=512,topology_wave_rows=4,full_records=dict(node_cache_entries=2,node_cache_bytes=1024,node_record_bytes=512,node_pin_bytes=1048576,node_dirty_records=256,node_dirty_bytes=131072,queue_cache_pages_per_home=1,queue_max_active_pages=256,placement_cache_pages_per_owner=1,placement_active_pages_per_owner=256,placement_pages_per_phase=8,placement_moves_per_phase=16))
base['stable_insert']=dict(rows=64,worker_bytes=1048576)
metadata_policy=dict(clean_records=8,dirty_records=128,pin_records=256,record_bytes=256,working_bytes=100352)
byte_policy=dict(target_bytes=4194304,quantum_bytes=4096)
receipts=[]
for name,paged,cadence in [('default',False,33),('paged',True,33),('ring-overflow',True,1)]:
 spec=dict(base,output=str(out/name),balance_every=cadence)
 if paged:spec['paged_metadata']=metadata_policy
 path=out/(name+'.spec.json');path.write_text(json.dumps(spec,indent=2)+'\n')
 with open(out/(name+'.stdout'),'wb') as stdout,open(out/(name+'.stderr'),'wb') as stderr:
  child=subprocess.Popen([a.binary,'reference-owner-diagnostic','--spec',str(path)],stdout=stdout,stderr=stderr,start_new_session=True,env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4'))
  try:code=child.wait(timeout=30)
  except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait();raise
 assert code==0,(out/(name+'.stderr')).read_text()[-4000:]
 r=json.loads((out/name/'result.json').read_text())
 lines=[line for line in (out/(name+'.stderr')).read_bytes().splitlines(keepends=True) if b'"progress_stage":"joined-phase-summary"' in line]
 summaries=[json.loads(line) for line in lines];phases=(n+cadence-1)//cadence
 assert len(lines)==2*phases and all(len(line)<=4096 for line in lines)
 assert [(v['phase'],v['event']) for v in summaries]==[(i,e)for i in range(1,phases+1)for e in ('balance','placement')]
 last=summaries[-1]
 for k,v in last['cumulative_work'].items():assert v==r['after_save']['counts'][k],(k,v,r['after_save']['counts'][k])
 for k,terminal in [('add','add_seconds'),('balance','balance_seconds'),('placement','placement_seconds'),('copy_nested_add','copy_seconds_nested_add'),('setup','setup_seconds')]:assert last['cumulative_seconds'][k]==r[terminal]
 for v in summaries:
  if v['event']=='balance':assert v['current_owner_rows_after_placement'] is None
 assert last['current_owner_rows_after_placement']==[v['current_rows']for v in r['after_save']['owners']]
 assert last['cumulative_placement_leaf_moves']==r['after_save']['placement_moves']
 assert r['census']['current_rows']==65 and r['census']['vector_bits_verified'] and r['census']['query_exact_distance_bits_verified']
 assert len(r['phases'])==min(32,phases)
 for retained in r['phases']:
  matched=next(v for v in summaries if v['completed_rows']==retained['rows'] and v['event']=='balance')
  assert all(value==retained['counts_after_balance'][key]for key,value in matched['cumulative_work'].items())
 vector=last['cumulative_vector_cost'];terminal_vector=r['after_save']['vector_store']
 for field,terminal_field in [('logical_vector_gets','logical_gets'),('reader_opens','reader_opens'),('copied_return_bytes','copied_return_bytes')]:assert vector[field]==terminal_vector[terminal_field]
 for field,terminal_field in [('provider_decode_attempts','decode_attempts'),('provider_encoded_bytes','encoded_bytes'),('provider_decode_elapsed_ns','decode_ns')]:assert vector[field]==terminal_vector['provider_traffic'].get(terminal_field,0)
 assert vector['fetch_batch_calls'] is None
 for key,value in last['cumulative_structural_mutex'].items():assert value==r['after_save']['structural_mutex_timing'][key]
 for name_op,values in last['cumulative_owner_operations'].items():
  assert all(isinstance(v,int) and not isinstance(v,bool) and v>=0 for v in values.values())
  assert values['calls']<=r['after_save']['messages']['operations'][name_op]['calls']
 assert last['cumulative_owner_operations']
 receipts.append(dict(name=name,phase_count=phases,progress_lines=len(lines),max_line_bytes=max(map(len,lines)),terminal_ring=len(r['phases']),status='passed'))
result=dict(status='passed',cases=receipts,binary_sha256=hashlib.sha256(pathlib.Path(a.binary).read_bytes()).hexdigest(),scope='Tiny native counter/timer reconciliation and bounded line/ring behavior only; no performance claim.')
(out/'test-result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
