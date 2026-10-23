"""Run only synthetic local native construction; never access a corpus or network."""
import argparse,hashlib,json,pathlib,struct,subprocess,time
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',required=True);a=p.parse_args();out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=False)
n=259;dim=32;data=b''.join(struct.pack('<f',-0.0 if d==2 else ((i*17+d*13)%97)/19.0) for i in range(n) for d in range(dim));f=out/'synthetic.f32';f.write_bytes(data);sha=hashlib.sha256(data).hexdigest()
config=dict(branching_factor=4,split_threshold=128,merge_threshold=16,write_beam_tau=2.,write_beam_min=8,write_beam_max=16,write_rng_epsilon=0.,write_rng_factor=1.,reassign_neighbor_count=16,write_navigation='fp',fp_npa=True)
results={};receipts=[]
for arm in ['reference','owner','bad-prefix']:
 spec=dict(protocol='reference-owner-parallel-v1',source_id='synthetic-chunk-stream-v1',vectors=dict(path=str(f),bytes=len(data),sha256=sha),prefix_sha256=sha if arm!='bad-prefix' else '0'*64,rows=n,dimension=dim,metric='l2',config=config,treatment='reference' if arm=='reference' else 'owner',threads=4,balance_every=193,owner_slots=2,move_above=96,predicate_batching=arm!='reference',batched_scrub_validation=arm!='reference',batched_split_input_validation=arm!='reference',bounded_vectors=arm!='reference',split_rows=1024,split_working_bytes=4*1024*1024,max_owner_physical_rows=8192,max_audit_nodes=10000,max_audit_rows=100000,publish_checkpoint=True,output=str(out/arm))
 path=out/(arm+'.spec.json');path.write_text(json.dumps(spec,indent=2)+'\n');t=time.monotonic();run=subprocess.run([a.binary,'reference-owner-diagnostic','--spec',str(path)],capture_output=True,timeout=45);elapsed=time.monotonic()-t
 (out/(arm+'.stdout')).write_bytes(run.stdout);(out/(arm+'.stderr')).write_bytes(run.stderr);receipts.append(dict(arm=arm,exit_code=run.returncode,seconds=elapsed))
 if arm=='bad-prefix':assert run.returncode!=0 and not (out/arm).exists();continue
 assert run.returncode==0,run.stderr.decode()[-2000:];r=json.loads((out/arm/'result.json').read_text());results[arm]=r
 assert r['census']['unique_live_rows']==n
 assert r['source_read_chunk_rows']==64 and r['phase_count']==2 and r['streaming_read_calls']==6
 assert (out/arm/'store'/'manifest.json').exists()
 if arm=='owner':
  v=r['owner_report']['bounded_vector_store'];assert v['staged_storage_chunks']==6 and v['peak_pending_handles']<=64;assert r['owner_report']['shared_embedding_cache_logical_bytes']==0
assert results['reference']['all_id_exact_fp_distance_bits']==results['owner']['all_id_exact_fp_distance_bits']
assert results['reference']['all_id_exact_fp_distance_sha256']==results['owner']['all_id_exact_fp_distance_sha256']
(out/'smoke-result.json').write_text(json.dumps(dict(status='passed',rows=n,logical_cuts=[193,66],storage_chunks=[64,64,64,1,64,2],receipts=receipts,all_exact_distance_bits_equal=True),indent=2)+'\n')
print(json.dumps(receipts))
