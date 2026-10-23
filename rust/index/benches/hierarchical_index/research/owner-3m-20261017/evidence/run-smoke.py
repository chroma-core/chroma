import pathlib,json,subprocess,os,time,hashlib,glob
import numpy as np
r=pathlib.Path('/home/ubuntu/owner3m'); assert time.time()+500<1792281037
assert subprocess.check_output(['systemctl','is-active','owner3m-20261017-stop.timer'],text=True).strip()=='active'
a=np.random.RandomState(42).normal(size=(16384,1024)).astype('<f4'); p=r/'smoke.f32';a.tofile(p); sha=hashlib.sha256(p.read_bytes()).hexdigest()
bins=[p for p in pathlib.Path('/home/ubuntu/reference3m/source/target/release/deps').glob('hierarchical_spann_shard-*') if p.is_file() and os.access(p,os.X_OK)]
assert len(bins)==1,bins
binary=bins[0]
s=dict(protocol='reference-owner-parallel-v1',source_id='synthetic-smoke1024D32workers',vectors=dict(path=str(p),bytes=p.stat().st_size,sha256=sha),prefix_sha256=sha,rows=16384,dimension=1024,metric='l2',config=dict(branching_factor=100,split_threshold=2048,merge_threshold=512,write_beam_tau=.5,write_beam_min=10,write_beam_max=16,write_rng_epsilon=0,write_rng_factor=1,reassign_neighbor_count=32,write_navigation='fp',fp_npa=True),treatment='owner',threads=32,balance_every=1000000,owner_slots=1,move_above=16384,predicate_batching=True,construction_observation=True,batched_scrub_validation=True,batched_split_input_validation=True,bounded_vectors=True,wide_identity=True,wikipedia_matched=True,stable_insert=dict(rows=64,worker_bytes=8388608),staging_rows=8192,staging_working_bytes=268435456,split_rows=131072,split_working_bytes=34359738368,max_owner_physical_rows=12000000,max_audit_nodes=4096,max_audit_rows=3000000,topology_cache_entries=8,topology_cache_payload_bytes=65536,topology_record_payload_bytes=8192,topology_wave_rows=8,full_records=dict(node_cache_entries=8,node_cache_bytes=65536,node_record_bytes=8192,node_pin_bytes=134217728,node_dirty_records=8192,node_dirty_bytes=67108864,queue_cache_pages_per_home=1,queue_max_active_pages=4096,placement_cache_pages_per_owner=1,placement_active_pages_per_owner=4096,placement_pages_per_phase=256,placement_moves_per_phase=4096))
# The small admission test uses a single balance cut at its end.
s['balance_every']=16384
for owners in [1,4]:
 s['owner_slots']=owners;s['output']=str(r/f'smoke-{owners}');spec=r/f'smoke-{owners}.json';spec.write_text(json.dumps(s,indent=2)+'\n'); t=time.monotonic()
 with (r/f'smoke-{owners}.log').open('x') as log:
  rc=subprocess.run(['/usr/bin/time','-v','-o',str(r/f'smoke-{owners}.time'),'timeout','--signal=TERM','--kill-after=15','180',str(binary),'reference-owner-diagnostic','--spec',str(spec)],stdout=log,stderr=subprocess.STDOUT).returncode
 (r/f'smoke-{owners}-exit.json').write_text(json.dumps(dict(exit_code=rc,elapsed_seconds=time.monotonic()-t,binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest()))+'\n')
 if rc: raise SystemExit(rc)
print('Both correctness smoke arms passed')
