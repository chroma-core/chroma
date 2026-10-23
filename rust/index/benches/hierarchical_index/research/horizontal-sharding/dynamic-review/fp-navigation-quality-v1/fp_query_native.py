from pathlib import Path
import json,hashlib,shutil,subprocess,os,time,struct
W=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN');O=W/'horizontal-sharding/dynamic-review/fp-navigation-quality-v1';R=O/'local-native';R.mkdir();base=O.parent/'consolidated-owner-construction-v1/local-native';binary=max((p for p in Path('/private/tmp/spann-write-check/debug/deps').glob('hierarchical_spann_shard-*') if p.is_file() and not p.suffix),key=lambda p:p.stat().st_mtime);kept=O/'native-writer';shutil.copyfile(binary,kept);kept.chmod(0o755)
def d(p):b=p.read_bytes();return dict(bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
queries=[json.loads(v) for v in (base/'queries.jsonl').read_text().splitlines()];results=[]
for arm in ['reference','owner']:
 store=base/arm/'store';before={str(p.relative_to(store)):d(p) for p in store.rglob('*') if p.is_file()};outputs={}
 for policy in [None,'one-bit','fp']:
  label=arm+'-'+(policy or 'default');requests=[dict(request_id=q['query_id'],generation=1,op='query',query=q['vector'],k=8,beam_min=64,beam_max=64,rerank_factor=8) for q in queries];command=[str(kept),'read','--checkpoint-dir',str(store),'--profile-query-work','--profile-rerank-pins','--trace-query-stages']+(['--query-navigation',policy] if policy else []);(R/(label+'-requests.jsonl')).write_text(''.join(json.dumps(v)+'\n' for v in requests));start=time.monotonic()
  with (R/(label+'-requests.jsonl')).open('rb') as stdin,(R/(label+'.stdout')).open('wb') as stdout,(R/(label+'.stderr')).open('wb') as stderr:p=subprocess.run(command,stdin=stdin,stdout=stdout,stderr=stderr,timeout=10,env=dict(os.environ,TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2'))
  results.append(dict(command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-start));assert p.returncode==0,(label,(R/(label+'.stderr')).read_text())
  values=[json.loads(v) for v in (R/(label+'.stdout')).read_text().splitlines()];assert len(values)==3 and all('error' not in v for v in values);outputs[policy]=values
 for off,one,fp in zip(outputs[None],outputs['one-bit'],outputs['fp']):
  assert off['ids']==one['ids']==fp['ids'] and off['distances']==one['distances']==fp['distances'];assert 'query_navigation_policy' not in off and one['query_navigation_policy']=='one-bit' and one['fp_navigation_cache'] is None
  assert off['query_work']['counts']==one['query_work']['counts'];assert one['query_navigation_snapshot']==fp['query_navigation_snapshot'];assert fp['query_navigation_policy']=='fp';cache=fp['fp_navigation_cache'];assert cache['dimension']==32 and cache['owned_f32_payload_bytes']==cache['nodes']*128 and cache['point_gets']==cache['nodes'] and cache['vector_access']['access_overflow'] is False
  assert cache==outputs['fp'][0]['fp_navigation_cache']
  for value in [off,one,fp]:
   c=value['query_work']['counts'];assert c['posting_rows_scored_before_dedup']==c['distinct_candidates_before_trim']==c['candidates_after_trim']==c['exact_embedding_gets']==c['exact_vectors_reranked']==64;assert c['centroid_distance_evaluations']>0 and c['expanded_internal_nodes']>0
   assert value['query_work']['returned_distance_bits']==[struct.unpack('<I',struct.pack('<f',v))[0] for v in value['distances']];assert set(value['query_stage_trace']['pretrim_candidate_ids'])==set(value['query_stage_trace']['retained_candidate_ids'])==set(range(64))
 assert before=={str(p.relative_to(store)):d(p) for p in store.rglob('*') if p.is_file()}
command=[str(kept),'read','--checkpoint-dir','/NONEXISTENT-MUST-NOT-OPEN','--query-navigation','fp'];p=subprocess.run(command,capture_output=True,timeout=5);assert p.returncode!=0 and b'explicit query navigation requires work profile' in p.stderr;(R/'invalid-policy.stderr').write_bytes(p.stderr)
(R/'test-result.json').write_text(json.dumps(dict(status='passed',binary=d(kept),processes=results,scope='SixactualARMreaderprocesses/18queries overunchanged64savedfixtures, plusinvalidpolicyreject beforeopen. Default/explicitonebit parity andFP full-leaf/fullcandidate exactreturn parity, actualevalcounts/cacheloads/generationmetadata. Syntheticcorrectness, no realdata orlatencyclaim.'),indent=2)+'\n');print('passed')
