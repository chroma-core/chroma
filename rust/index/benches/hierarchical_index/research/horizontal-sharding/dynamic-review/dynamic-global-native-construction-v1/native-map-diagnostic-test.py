"""Fresh original-manifest map bridge; actual saved-GT CLI on both treatments."""
import copy,hashlib,json,os,subprocess,time,unittest,struct,itertools
from pathlib import Path
A=Path(__file__).resolve().parent;E=A/'repair-v3';OUT=E/'map-tests-attempt1';OUT.mkdir();BIN=E/'native-writer';H=lambda b:hashlib.sha256(b).hexdigest()
INPUT=A.parent/'representative-compaction-v3/runtime-attempt1/append/private-writers/canonical.f32';raw=INPUT.read_bytes();assert H(raw)=='69ce5f9fa914cd9ed6b05c860f36ef29d68600b30bdc57891d2bb2bd635838cc';binary=json.loads((E/'binary.json').read_text());assert H(BIN.read_bytes())==binary['sha256']
ENV=os.environ.copy();ENV.update(TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2');commands=[]
BASE=dict(format_version=1,source_id='retained-representative98',vectors=dict(path=str(INPUT),bytes=len(raw),sha256=H(raw)),rows=98,dimension=32,metric='l2',config=dict(branching_factor=3,split_threshold=8,merge_threshold=2,write_beam_tau=1.5,write_beam_min=128,write_beam_max=128,write_rng_epsilon=0.,write_rng_factor=1.,reassign_neighbor_count=128,write_navigation='fp',fp_npa=True),storage_chunk=7,balance_every=24,owner_slots=4,owner_vector_threshold=32,max_nodes=4096,max_owner_physical_rows=8192,max_leaf_rows=4096)
def invoke(name,args,stdin=None):
 start=time.monotonic();p=subprocess.run([str(BIN)]+args,input=stdin,env=ENV,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=90);(OUT/(name+'.stdout')).write_bytes(p.stdout);(OUT/(name+'.stderr')).write_bytes(p.stderr);commands.append(dict(name=name,command=[str(BIN)]+args,exit_code=p.returncode,elapsed_seconds=time.monotonic()-start,stdout_sha256=H(p.stdout),stderr_sha256=H(p.stderr)));return p
def artifact(p):b=p.read_bytes();return dict(path=str(p),bytes=len(b),sha256=H(b))
vectors=[struct.unpack('<32f',raw[i*128:(i+1)*128]) for i in range(98)];gt=[];queries=[]
for i,row in enumerate([0,47,97]):
 q=vectors[row];dist=sorted((sum((float(x)-float(y))**2 for x,y in zip(q,v)),j) for j,v in enumerate(vectors))[:10];gt.append(dict(query_id='q'+str(i),ids=[j for d,j in dist],distances=[d for d,j in dist]));queries.append(dict(query_id='q'+str(i),vector=q))
GT=OUT/'known-gt.jsonl';GT.write_text(''.join(json.dumps(x)+'\n' for x in gt));Q=OUT/'known-queries.jsonl';Q.write_text(''.join(json.dumps(x)+'\n' for x in queries));union=set(itertools.chain.from_iterable(x['ids'] for x in gt))
class Bridge(unittest.TestCase):
 def test_actual_original_manifests_diagnose_known_gt_in_both_arms(self):
  reranks={}
  for arm in ['reference','authoritative']:
   spec=copy.deepcopy(BASE);spec.update(treatment=arm,output_dir=str(OUT/arm));S=OUT/(arm+'.spec.json');S.write_text(json.dumps(spec));p=invoke(arm,['construct-global','--spec',str(S)]);self.assertEqual(p.returncode,0,p.stderr.decode());result=json.loads(p.stdout);snapshot=OUT/arm/'store/manifest.json';snapshot_before=H(snapshot.read_bytes());m=json.loads(snapshot.read_text());mapping=m['id_mapping'];identity=b''.join(struct.pack('<Q',i) for i in range(98));self.assertEqual(mapping,dict(path='id-map-1.u64le',count=98,bytes=784,sha256=H(identity)));self.assertEqual((snapshot.parent/mapping['path']).read_bytes(),identity);self.assertEqual(result['id_mapping']['sha256'],mapping['sha256']);self.assertEqual(result['id_mapping']['bytes'],784)
   spec=dict(format_version=1,source_id=BASE['source_id'],dimension=32,metric='l2',global_rows=98,max_map_rows=98,ground_truth=artifact(GT),query_vectors=artifact(Q),selected_queries=[dict(query_id=x['query_id'],expected=10) for x in queries],owners=[dict(owner_id='global',generation=1,snapshot=artifact(snapshot))]);D=OUT/(arm+'.diagnostic.spec.json');D.write_text(json.dumps(spec));p=invoke(arm+'-diagnostic',['diagnose-ground-truth','--spec',str(D)]);self.assertEqual(p.returncode,0,p.stderr.decode());d=json.loads(p.stdout);self.assertEqual(d['gt_union_count'],len(union));self.assertEqual(d['raw_embedding_reads'],0);owner=d['owners'][0];self.assertEqual(owner['map_rows'],98);self.assertEqual(owner['map_bytes_read'],784);self.assertEqual(owner['map_sha256'],H(identity));leaves=owner['census']['leaves'];actual=list(itertools.chain.from_iterable(x['gt_ids'] for x in leaves));self.assertEqual(len(actual),len(set(actual)));self.assertEqual(set(actual),union)
   for row in d['queries']:
    expected=set(next(x for x in gt if x['query_id']==row['query_id'])['ids']);self.assertEqual(row['covered'],10);self.assertEqual(row['expected'],10);counts=[(len(expected.intersection(x['gt_ids'])),x['live_rows']) for x in leaves];counts=[x for x in counts if x[0]];self.assertEqual(sum(x[0] for x in counts),10)
    for oracle in row['oracles']:
     target=oracle['target_hits'];best_leaves=best_rows=10**20
     # Independent exhaustive set oracle is bounded by ten GT-bearing leaves.
     for n in range(1,len(counts)+1):
      for chosen in itertools.combinations(counts,n):
       if sum(x[0] for x in chosen)>=target:best_leaves=min(best_leaves,n);best_rows=min(best_rows,sum(x[1] for x in chosen))
     self.assertEqual(oracle['minimum_leaves'],best_leaves);self.assertEqual(oracle['minimum_live_rows'],best_rows)
   payload=''.join(json.dumps(dict(op='rerank_selected',query=x['vector'],local_ids=list(range(98)),generation=1))+'\n' for x in queries).encode();p=invoke(arm+'-read',['read','--checkpoint-dir',str(snapshot.parent)],payload);self.assertEqual(p.returncode,0,p.stderr.decode());reranks[arm]=[json.loads(x)['results'] for x in p.stdout.splitlines()];self.assertEqual(H(snapshot.read_bytes()),snapshot_before)
  self.assertEqual(reranks['reference'],reranks['authoritative'])
start=time.monotonic();test=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Bridge));R=Path('/private/tmp/chroma-hierarchical-spann-sharding');pins=json.loads((E/'manifest.json').read_text())['files'];end={p:H((R/p).read_bytes()) for p in pins};assert end==pins;(OUT/'result.json').write_text(json.dumps(dict(passed=test.wasSuccessful(),tests=test.testsRun,elapsed_seconds=time.monotonic()-start,commands=commands,end_pins=end,binary=binary,input_sha256=H(raw),known_gt_provenance='independent exhaustive scalar L2 on retained98 FP32 vectors; component differences are exact modest integers in this fixture'),indent=2)+'\n');raise SystemExit(not test.wasSuccessful())
