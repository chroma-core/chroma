from pathlib import Path
import os,json,hashlib,subprocess,shutil,time
W=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN');O=W/'horizontal-sharding/dynamic-review/owner-count-attribution-v1';R=O/'local-native';R.mkdir()
old=json.load(open(O.parent/'build-speed-diagnostic-v1/local-native-final/authoritative-spec.json'));binary=max((p for p in Path('/private/tmp/spann-write-check/debug/deps').glob('hierarchical_spann_shard-*') if p.is_file() and not p.suffix),key=lambda p:p.stat().st_mtime);kept=O/'local-native-binary';shutil.copyfile(binary,kept);kept.chmod(0o755)
results=[];values=[]
for label in ['reference','owner1','owner4']:
 config=old['config']|dict(branching_factor=3,split_threshold=4,merge_threshold=1)
 spec=dict(protocol='reference-owner-parallel-v1',source_id='retained-synthetic64x32',vectors=old['vectors'],prefix_sha256=old['prefix_sha256'],rows=64,dimension=32,metric='l2',config=config,treatment=('reference' if label=='reference' else 'owner'),predicate_batching=False,construction_observation=True,skip_placement=(label!='owner4'),threads=2,balance_every=32,owner_slots=(4 if label=='owner4' else 1),move_above=8,output=str(R/label))
 path=R/(label+'-spec.json');path.write_text(json.dumps(spec,indent=2)+'\n');start=time.monotonic();command=[str(kept),'reference-owner-diagnostic','--spec',str(path)]
 with (R/(label+'.stdout')).open('wb') as out,(R/(label+'.stderr')).open('wb') as err:p=subprocess.run(command,stdout=out,stderr=err,timeout=30,env=dict(os.environ,TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2'))
 result=dict(treatment=label,command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-start);results.append(result)
 assert p.returncode==0
 v=json.load(open(R/label/'result.json'));assert v['spec']==spec and v['spec_sha256']==hashlib.sha256(path.read_bytes()).hexdigest();assert v['census']['unique_live_rows']==64 and v['census']['duplicate_current_entries']==v['census']['unreachable_nodes']==0
 for phase in v['phases']:
  assert phase['counts_after_balance']['current_version_requests']>0
  assert phase['counts_after_balance']['adds']>=phase['counts_after_add']['adds']
 if label!='reference':
  report=v['owner_report'];assert report['central_mutable_posting_rows']==report['central_current_versions']==0
  assert report['message_timing']['enabled'] is True and report['message_timing']['queued']==0
  assert report['message_timing']['operations']['current']['calls']>0
  assert report['message_timing']['operations']['predicate']['calls']>0
  assert (report['placement_moves']>0)==(label=='owner4')
  assert sum(o['current'] for o in report['owners'])==64
  assert sum(o['current']>0 for o in report['owners'])==(4 if label=='owner4' else 1)
 else:assert v['reference_shape']['reachable_nodes']==v['reference_shape']['nodes']
 values.append(v)
assert len(values)==3 and all(v['all_id_exact_fp_distance_bits']==values[0]['all_id_exact_fp_distance_bits'] for v in values)
(R/'test-result.json').write_text(json.dumps(dict(status='passed',results=results,binary=dict(path=str(kept),bytes=kept.stat().st_size,sha256=hashlib.sha256(kept.read_bytes()).hexdigest()),input=old['vectors'],scope='Three actual localARM native commands, synthetic64x32. Exact fullID bits/census/policy/worktiming checks; no quality/performanceclaim.'),indent=2)+'\n');print(results)
