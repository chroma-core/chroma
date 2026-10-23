"""Small real-entry-point wiring check; no cloud, network or corpus input."""
import hashlib,json,pathlib,struct,subprocess,time
ROOT=pathlib.Path(__file__).resolve().parent

def pin(path):
 data=path.read_bytes();return dict(path=str(path),bytes=len(data),sha256=hashlib.sha256(data).hexdigest())

def main():
 artifacts=[json.loads(line) for line in (ROOT/'build.jsonl').read_text().splitlines() if line.startswith('{')]
 native=pathlib.Path(next(a['executable'] for a in artifacts if a.get('reason')=='compiler-artifact' and a.get('target',{}).get('name')=='hierarchical_spann_shard' and a.get('executable')))
 source=ROOT/'synthetic64.f32le';source.write_bytes(b''.join(struct.pack('<f',((r*17+d*13)%97)/19+(r%8)*4*(d==0)) for r in range(64) for d in range(32)))
 descriptor=pin(source)
 config=dict(branching_factor=4,split_threshold=16,merge_threshold=4,write_beam_tau=2.,write_beam_min=2,write_beam_max=4,write_rng_epsilon=0.,write_rng_factor=1.,reassign_neighbor_count=8,write_navigation='fp',fp_npa=True)
 results=[]
 for live in [False,True]:
  name='live' if live else 'resident';output=ROOT/(name+'-output')
  spec=dict(protocol='reference-owner-parallel-v1',source_id='synthetic64x32-topology-wiring-v1',vectors=descriptor,prefix_sha256=descriptor['sha256'],rows=64,dimension=32,metric='l2',config=config,treatment='owner',threads=2,balance_every=32,owner_slots=2,move_above=16,predicate_batching=True,construction_observation=True,batched_scrub_validation=True,batched_split_input_validation=True,bounded_vectors=True,wide_identity=True,live_topology=live,staging_rows=16,staging_working_bytes=1048576,split_rows=64,split_working_bytes=16777216,max_owner_physical_rows=1024,max_audit_nodes=256,max_audit_rows=1024,topology_cache_entries=2,topology_cache_payload_bytes=512,topology_record_payload_bytes=512,topology_wave_rows=2,topology_mutation_working_bytes=1048576,output=str(output))
  path=ROOT/(name+'-spec.json');path.write_text(json.dumps(spec,indent=2)+'\n');start=time.monotonic()
  with (ROOT/(name+'.stdout')).open('w') as out,(ROOT/(name+'.stderr')).open('w') as err:
   child=subprocess.run([str(native),'reference-owner-diagnostic','--spec',str(path)],stdout=out,stderr=err,timeout=20)
  assert child.returncode==0,(name,child.returncode)
  result=json.loads((output/'result.json').read_text());assert result['status']=='passed';assert result['census']['current_rows']==64 and result['census']['vector_bits_verified'] and result['census']['query_exact_distance_bits_verified'];assert result['phase_count']==2 and len(result['queries'])==4
  assert result['saved_reader_at_open']['typed_metadata_and_centroid_point_reads']==3
  assert result['after_save']['placement_moves']>0
  if live:
   topology=result['after_save']['live_topology'];assert topology['evictions']>0 and topology['durable_root_commits']>1;assert topology['peak_cache_entries']<=2 and topology['peak_cache_payload_bytes']<=512;assert result['after_save']['resident_kernel_centroid_floats']==0
  else:assert result['after_save']['live_topology'] is None and result['after_save']['resident_kernel_centroid_floats']>0
  results.append(dict(mode=name,elapsed=time.monotonic()-start,result=pin(output/'result.json'),spec=pin(path),counts=result['after_save']['counts'],shape=result['after_save']['shape']))
 output=dict(status='passed',native=pin(native),input=descriptor,cells=results,scope='Independent trees; vector bits and routed exact distances verified in each cell, not tree/query equality or performance.')
 (ROOT/'native-wiring-results.json').write_text(json.dumps(output,indent=2)+'\n');print(json.dumps(output))

if __name__=='__main__':main()
