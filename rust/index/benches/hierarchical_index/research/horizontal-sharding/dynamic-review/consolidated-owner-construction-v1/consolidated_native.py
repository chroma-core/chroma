from pathlib import Path
import os,json,hashlib,subprocess,shutil,time,struct,itertools,math
W=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN');O=W/'horizontal-sharding/dynamic-review/consolidated-owner-construction-v1';R=O/'local-native';R.mkdir()
def descriptor(p):b=p.read_bytes();return dict(path=str(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
old=json.load(open(O.parent/'build-speed-diagnostic-v1/local-native-final/authoritative-spec.json'));binary=max((p for p in Path('/private/tmp/spann-write-check/debug/deps').glob('hierarchical_spann_shard-*') if p.is_file() and not p.suffix),key=lambda p:p.stat().st_mtime);kept=O/'native-writer';shutil.copyfile(binary,kept);kept.chmod(0o755)
results=[]
def run(name,command):
 start=time.monotonic()
 with (R/(name+'.stdout')).open('wb') as out,(R/(name+'.stderr')).open('wb') as err:p=subprocess.run(command,stdout=out,stderr=err,timeout=30,env=dict(os.environ,TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2'))
 receipt=dict(command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-start,stdout=descriptor(R/(name+'.stdout')),stderr=descriptor(R/(name+'.stderr')));results.append(receipt);assert p.returncode==0,receipt
 return json.loads((R/(name+'.stdout')).read_text().splitlines()[-1])
raw=Path(old['vectors']['path']).read_bytes();values=[struct.unpack('<32f',raw[i:i+128]) for i in range(0,len(raw),128)];assert len(values)==64
queries=[[float(struct.unpack('<f',struct.pack('<f',math.sin((i+1)*(q+1))*.37))[0]) for i in range(32)] for q in range(3)]
expected=[]
for q,vector in enumerate(queries):
 ordered=sorted(range(64),key=lambda i:(sum((a-b)**2 for a,b in zip(vector,values[i])),i));expected.append(dict(query_id=f'heldout-{q}',ids=ordered[:8],distances=[sum((a-b)**2 for a,b in zip(vector,values[i])) for i in ordered[:8]]))
gt=R/'gt.jsonl';gt.write_text(''.join(json.dumps(v)+'\n' for v in expected));querypath=R/'queries.jsonl';querypath.write_text(''.join(json.dumps(dict(query_id=f'heldout-{i}',vector=v))+'\n' for i,v in enumerate(queries)))
outputs=[]
for label in ['reference','owner']:
 spec=dict(protocol='reference-owner-parallel-v1',source_id='retained-synthetic64x32',vectors=old['vectors'],prefix_sha256=old['prefix_sha256'],rows=64,dimension=32,metric='l2',config=old['config']|dict(branching_factor=3,split_threshold=4,merge_threshold=1),treatment=label,predicate_batching=label=='owner',construction_observation=True,skip_placement=False,direct_current_reads=False,batched_scrub_validation=label=='owner',publish_checkpoint=True,threads=2,balance_every=32,owner_slots=4,move_above=8,output=str(R/label))
 path=R/(label+'-spec.json');path.write_text(json.dumps(spec,indent=2)+'\n');v=run(label,[str(kept),'reference-owner-diagnostic','--spec',str(path)]);outputs.append(v);assert v['spec']==spec
 pub=v['checkpoint_publication'];manifest=Path(pub['manifest_path']);assert descriptor(manifest)['sha256']==pub['manifest_sha256'];m=json.loads(manifest.read_bytes());map_path=manifest.parent/m['id_mapping']['path'];assert map_path.read_bytes()==b''.join(struct.pack('<Q',i) for i in range(64));assert pub['map_write_bytes']==pub['map_verify_bytes']==512
 assert m['construction_input']['spec_sha256']==hashlib.sha256(path.read_bytes()).hexdigest() and m['construction_input']['prefix_sha256']==old['prefix_sha256']
 assert m['construction']['config']==spec['config'];assert m['block_ids']==v['roots'];before=descriptor(manifest)
 ds=dict(format_version=1,source_id=spec['source_id'],dimension=32,metric='l2',global_rows=64,max_map_rows=64,ground_truth=descriptor(gt),query_vectors=descriptor(querypath),selected_queries=[dict(query_id=x['query_id'],expected=8) for x in expected],owners=[dict(owner_id='global',generation=1,snapshot=before)])
 dp=R/(label+'-diagnostic-spec.json');dp.write_text(json.dumps(ds,indent=2)+'\n');report=run(label+'-diagnostic',[str(kept),'diagnose-ground-truth','--spec',str(dp)]);assert descriptor(manifest)==before
 (R/(label+'-report.json')).write_text(json.dumps(report,indent=2)+'\n');assert report['raw_embedding_reads']==0
 leaves=report['owners'][0]['census']['leaves'];union=set().union(*(set(x['ids']) for x in expected));actual=[i for leaf in leaves for i in leaf['gt_ids']];assert len(actual)==len(set(actual)) and set(actual)==union
 # Independent exhaustive subset oracle on GT-bearing leaves, not the Rust DP/greedy implementation.
 for truth,qr in zip(expected,report['queries']):
  target=set(truth['ids']);weighted=[(len(target.intersection(leaf['gt_ids'])),leaf['live_rows']) for leaf in leaves if target.intersection(leaf['gt_ids'])];assert len(weighted)<=8
  options=[(sum(a for a,b in subset),len(subset),sum(b for a,b in subset)) for n in range(len(weighted)+1) for subset in itertools.combinations(weighted,n)]
  assert qr['covered']==qr['expected']==8
  for oracle in qr['oracles']:
   eligible=[x for x in options if x[0]>=oracle['target_hits']];assert oracle['minimum_leaves']==min(x[1] for x in eligible);assert oracle['minimum_live_rows']==min(x[2] for x in eligible)
 if label=='owner':
  assert v['construction_policy']=='owner-batched-v1';r=v['owner_report'];assert r['predicate_batch_computed_rows']>0 and r['scrub_validation_rows']>0 and r['direct_current_reads']==0 and r['placement_moves']>0
assert outputs[0]['all_id_exact_fp_distance_bits']==outputs[1]['all_id_exact_fp_distance_bits']
(R/'test-result.json').write_text(json.dumps(dict(status='passed',results=results,binary=descriptor(kept),expected_ground_truth=expected,scope='Actual local ARM reference and combined owner constructors plus unchanged saved GT command. Three independent held-out synthetic queries, brute-force flat L2 GT and exhaustive leaf subset cost oracle. No ANN quality/performance or realdata claim.'),indent=2)+'\n');print('passed four native commands')
