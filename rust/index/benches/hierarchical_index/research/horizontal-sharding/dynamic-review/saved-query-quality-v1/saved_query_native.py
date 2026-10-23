from pathlib import Path
import json,hashlib,shutil,subprocess,os,time,struct
W=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN');O=W/'horizontal-sharding/dynamic-review/saved-query-quality-v1';R=O/'local-native';R.mkdir();base=O.parent/'consolidated-owner-construction-v1/local-native';binary=max((p for p in Path('/private/tmp/spann-write-check/debug/deps').glob('hierarchical_spann_shard-*') if p.is_file() and not p.suffix),key=lambda p:p.stat().st_mtime);kept=O/'native-writer';shutil.copyfile(binary,kept);kept.chmod(0o755)
def d(p):b=p.read_bytes();return dict(bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
queries=[json.loads(v) for v in (base/'queries.jsonl').read_text().splitlines()];results=[]
for arm in ['reference','owner']:
 store=base/arm/'store';before={str(p.relative_to(store)):d(p) for p in store.rglob('*') if p.is_file()};outputs={}
 for enabled in [False,True]:
  label=arm+('-on' if enabled else '-off');requests=[dict(request_id=q['query_id'],generation=1,op='query',query=q['vector'],k=8,beam_min=64,beam_max=64,rerank_factor=8) for q in queries];command=[str(kept),'read','--checkpoint-dir',str(store)]+(['--profile-query-work','--profile-rerank-pins'] if enabled else []);(R/(label+'-requests.jsonl')).write_text(''.join(json.dumps(v)+'\n' for v in requests));start=time.monotonic()
  with (R/(label+'-requests.jsonl')).open('rb') as stdin,(R/(label+'.stdout')).open('wb') as stdout,(R/(label+'.stderr')).open('wb') as stderr:p=subprocess.run(command,stdin=stdin,stdout=stdout,stderr=stderr,timeout=10,env=dict(os.environ,TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2'))
  results.append(dict(command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-start));assert p.returncode==0
  values=[json.loads(v) for v in (R/(label+'.stdout')).read_text().splitlines()];assert len(values)==3 and all('error' not in v for v in values);outputs[enabled]=values
 for off,on in zip(outputs[False],outputs[True]):
  assert off['ids']==on['ids'] and off['distances']==on['distances'];assert 'query_work' not in off and 'query_work_policy' not in off
  work=on['query_work'];c=work['counts'];assert c['posting_rows_scored_before_dedup']==c['distinct_candidates_before_trim']==c['candidates_after_trim']==c['exact_embedding_gets']==c['exact_vectors_reranked']==64;assert c['nonempty_leaves_scored']<=c['selected_leaves'] and c['selected_leaves']>1;assert work['returned_results']==8
  assert work['returned_distance_bits']==[struct.unpack('<I',struct.pack('<f',v))[0] for v in on['distances']];assert on['vector_pin_observation']['access_overflow'] is False
 assert before=={str(p.relative_to(store)):d(p) for p in store.rglob('*') if p.is_file()}
(R/'test-result.json').write_text(json.dumps(dict(status='passed',binary=d(kept),processes=results,scope='FouractualARMreaderprocesses/12queries over unchanged accepted64savedfixtures. Profileoff/on exactIDs/FPbits/raw64rows+rerank/returnedcounts andvectorobserverbounds; sourceRusttest separately proves raw39rows/distinct7>trim4. No realdata/ANNperformanceclaim.'),indent=2)+'\n');print('passed')
