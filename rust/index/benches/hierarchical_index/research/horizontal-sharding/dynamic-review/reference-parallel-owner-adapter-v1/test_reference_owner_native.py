from pathlib import Path
import os,json,hashlib,subprocess,shutil,time
W=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN');O=W/'horizontal-sharding/dynamic-review/reference-parallel-owner-adapter-v1';R=O/'local-native';R.mkdir()
old=json.load(open(O.parent/'build-speed-diagnostic-v1/local-native-final/authoritative-spec.json'));binary=max((p for p in Path('/private/tmp/spann-write-check/debug/deps').glob('hierarchical_spann_shard-*') if p.is_file() and not p.suffix),key=lambda p:p.stat().st_mtime);kept=O/'local-native-binary';shutil.copyfile(binary,kept);kept.chmod(0o755)
results=[];values=[]
for label in ['reference','owner']:
 config=old['config']|dict(branching_factor=3,split_threshold=4,merge_threshold=1)
 spec=dict(protocol='reference-owner-parallel-v1',source_id='retained-synthetic64x32',vectors=old['vectors'],prefix_sha256=old['prefix_sha256'],rows=64,dimension=32,metric='l2',config=config,treatment=label,threads=2,balance_every=32,owner_slots=4,move_above=8,output=str(R/label))
 path=R/(label+'-spec.json');path.write_text(json.dumps(spec,indent=2)+'\n');start=time.monotonic();command=[str(kept),'reference-owner-diagnostic','--spec',str(path)]
 with (R/(label+'.stdout')).open('wb') as out,(R/(label+'.stderr')).open('wb') as err:p=subprocess.run(command,stdout=out,stderr=err,timeout=30,env=dict(os.environ,TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2'))
 result=dict(treatment=label,command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-start);results.append(result)
 if p.returncode:break
 v=json.load(open(R/label/'result.json'));assert v['spec']==spec and v['spec_sha256']==hashlib.sha256(path.read_bytes()).hexdigest();assert v['census']['unique_live_rows']==64 and v['census']['duplicate_current_entries']==v['census']['unreachable_nodes']==0
 assert len(v['all_id_exact_fp_distance_bits'])==64;values.append(v)
 if label=='owner':
  report=v['owner_report'];assert report['central_mutable_posting_rows']==report['central_current_versions']==0 and report['placement_moves']>0 and report['owner_predicates']>0
  assert sum(o['current'] for o in report['owners'])==64 and sum(o['current']>0 for o in report['owners'])>1
assert len(values)==2 and values[0]['all_id_exact_fp_distance_bits']==values[1]['all_id_exact_fp_distance_bits']
(R/'test-result.json').write_text(json.dumps(dict(status='passed',results=results,binary=dict(path=str(kept),bytes=kept.stat().st_size,sha256=hashlib.sha256(kept.read_bytes()).hexdigest()),input=old['vectors'],scope='Actual native command, synthetic64x32 localARMdev, independent parallel schedules. Fullcensus/rawbit verification eacharm and all-ID FPdistance parity; no ANNquality or performance comparison.'),indent=2)+'\n');print(results)
