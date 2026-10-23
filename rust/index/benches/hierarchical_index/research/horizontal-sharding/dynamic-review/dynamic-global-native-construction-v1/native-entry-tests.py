"""Actual CLI correctness checks; no cloud, runner integration, or performance claim."""
import copy,hashlib,json,os,subprocess,time,unittest,struct
from pathlib import Path
A=Path(__file__).resolve().parent;E=A/'repair-v2';OUT=E/'entry-tests-attempt1';OUT.mkdir()
BIN=E/'native-writer';H=lambda b:hashlib.sha256(b).hexdigest()
INPUT=A.parent/'representative-compaction-v3/runtime-attempt1/append/private-writers/canonical.f32';raw=INPUT.read_bytes();assert len(raw)==98*32*4
assert H(raw)=='69ce5f9fa914cd9ed6b05c860f36ef29d68600b30bdc57891d2bb2bd635838cc'
binary=json.loads((E/'binary.json').read_text());assert H(BIN.read_bytes())==binary['sha256']
ENV=os.environ.copy();ENV.update(TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2')
BASE=dict(format_version=1,treatment='reference',source_id='retained-representative98',vectors=dict(path=str(INPUT),bytes=len(raw),sha256=H(raw)),rows=98,dimension=32,metric='l2',config=dict(branching_factor=3,split_threshold=8,merge_threshold=2,write_beam_tau=1.5,write_beam_min=128,write_beam_max=128,write_rng_epsilon=0.,write_rng_factor=1.,reassign_neighbor_count=128,write_navigation='fp',fp_npa=True),storage_chunk=7,balance_every=24,owner_slots=4,owner_vector_threshold=32,max_nodes=4096,max_owner_physical_rows=8192,max_leaf_rows=4096)
commands=[]
def invoke(name,args,stdin=None):
 t=time.monotonic();p=subprocess.run([str(BIN)]+args,env=ENV,input=stdin,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=90)
 (OUT/(name+'.stdout')).write_bytes(p.stdout);(OUT/(name+'.stderr')).write_bytes(p.stderr);commands.append(dict(name=name,command=[str(BIN)]+args,exit_code=p.returncode,elapsed_seconds=time.monotonic()-t,stdout_sha256=H(p.stdout),stderr_sha256=H(p.stderr)))
 return p
def construct(name,spec):
 spec['output_dir']=str(OUT/name);path=OUT/(name+'.spec.json');path.write_text(json.dumps(spec));return invoke(name,['construct-global','--spec',str(path)])
class NativeEntry(unittest.TestCase):
 def test_paired_actual_build_save_reopen_and_exact_rerank(self):
  results={};reads={}
  for treatment in ['reference','authoritative']:
   spec=copy.deepcopy(BASE);spec['treatment']=treatment;p=construct(treatment,spec);self.assertEqual(p.returncode,0,p.stderr.decode());m=json.loads(p.stdout);results[treatment]=m
   self.assertEqual(m['config']['fp_npa'],True);self.assertEqual(m['config']['write_navigation'],'fp');self.assertEqual(m['integrity_audit']['unique_live_rows'],98);self.assertEqual(m['integrity_audit']['duplicate_current_entries'],0);self.assertEqual([x['rows'] for x in m['balance_cuts']],[24,48,72,96,98]);self.assertEqual(m['id_mapping']['sha256'],H(b''.join(struct.pack('<Q',i) for i in range(98))))
   if treatment=='authoritative':
    self.assertGreater(m['accounting']['event_counts'].get('move',0),0);self.assertGreater(len(set(m['branch_owners'].values())),1)
   requests=[dict(request_id=i,op='rerank_selected',query=list(struct.unpack('<32f',raw[q*128:(q+1)*128])),local_ids=list(range(98)),generation=1) for i,q in enumerate([0,47,97])]
   payload=''.join(json.dumps(r)+'\n' for r in requests).encode();(OUT/(treatment+'.requests.jsonl')).write_bytes(payload)
   p=invoke(treatment+'-reopen',['read','--checkpoint-dir',str(OUT/treatment/'store')],payload);self.assertEqual(p.returncode,0,p.stderr.decode());rows=[json.loads(x) for x in p.stdout.splitlines()];self.assertEqual(len(rows),3)
   for row in rows:self.assertNotIn('error',row);self.assertEqual(len(row['results']),98);self.assertEqual(set(x['id'] for x in row['results']),set(range(98)))
   reads[treatment]=[[x['results'] for x in rows]][0]
  self.assertEqual(reads['reference'],reads['authoritative']);(OUT/'paired-results.json').write_text(json.dumps(results,indent=2))
 def test_invalid_explicit_policy_and_input_reject_before_output(self):
  for name,mutate in [('quantized',lambda s:s['config'].update(fp_npa=False)),('wrong-hash',lambda s:s['vectors'].update(sha256='0'*64)),('unknown-field',lambda s:s.update(implicit_npa=True)),('zero-chunk',lambda s:s.update(storage_chunk=0))]:
   s=copy.deepcopy(BASE);mutate(s);p=construct(name,s);self.assertNotEqual(p.returncode,0);self.assertFalse((OUT/name).exists())
start=time.monotonic();result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(NativeEntry));pins=json.loads((E/'manifest.json').read_text())['files'];R=Path('/private/tmp/chroma-hierarchical-spann-sharding');current={p:H((R/p).read_bytes()) for p in pins};assert current==pins
summary=dict(passed=result.wasSuccessful(),tests=result.testsRun,elapsed_seconds=time.monotonic()-start,binary=binary,commands=commands,end_pins=current,input_sha256=H(raw));(OUT/'result.json').write_text(json.dumps(summary,indent=2)+'\n');raise SystemExit(not result.wasSuccessful())
