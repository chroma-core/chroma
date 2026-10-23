import copy,importlib.util,json,shutil,tempfile,types,unittest
from pathlib import Path
from unittest.mock import patch
P=Path(__file__).resolve().parent;W=P.parent.parent
sp=importlib.util.spec_from_file_location('glue',P/'run-cache.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m)
OLD=W/'fleet/four-bit-navigation-quality-v1/runtime-evidence/runtime-attempt1';SAVED=W/'fleet/consolidated-owner-quality-runtime-v1/runtime-evidence'
class Glue(unittest.TestCase):
 def spec(self,root):
  s=json.loads((P/'runtime-spec.template.json').read_text());binary=root/'not-executed';binary.write_text('fake');s.update(binary=m.descriptor(binary),deadline_unix=m.time.time()+40,output=str(root/'out'))
  for name,source in [('queries',SAVED/'queries.jsonl'),('ground_truth',SAVED/'runtime-attempt1/prefix-gt.jsonl')]:
   target=root/source.name;shutil.copy2(source,target);s['bindings'][name]=m.descriptor(target)
  for arm in ['reference','owner']:
   store=root/arm/'store';store.mkdir(parents=True)
   for key,name in [('manifest','manifest.json'),('map','id-map-1.u64le'),('quality',arm+'-quality.json')]:
    source=SAVED/'runtime-attempt1'/arm/'store'/name if key!='quality' else SAVED/'runtime-attempt1'/name;target=store/name;shutil.copy2(source,target);s['bindings'][arm][key]=m.descriptor(target)
  (root/'runtime-spec.template.json').write_text(json.dumps(s));shutil.copy2(P/'fixed-decisions.json',root/'fixed-decisions.json');return s
 def fake(self,calls,case):
  def run(command,stdin,stdout,stderr,timeout,env):
   self.assertLessEqual(timeout,10);self.assertEqual(command[-2],'--provider-cache-policy');policy=json.loads(command[-1]);self.assertEqual((policy['block_capacity_units'],policy['root_capacity_entries'],policy['clear_after_operation']),(256,4096,False));self.assertIn(policy['shards'],[64,1]);calls.append(command)
   if case=='exit7':return types.SimpleNamespace(returncode=7)
   nav=command[command.index('--query-navigation')+1];arm=Path(command[command.index('--checkpoint-dir')+1]).parent.name;requests=[json.loads(v) for v in stdin];raw=[json.loads(v) for v in (OLD/f'{arm}-{nav}.stdout').read_text().splitlines()];self.assertEqual([v['request_id'] for v in requests],[v['request_id'] for v in raw])
   for i,v in enumerate(raw):
    v['provider_cache_policy']=policy
    if policy['shards']==1 or case=='nonrepro':
     for block in v['vector_pin_observation']['block_access'].values():
      block['provider_hits']=block['provider_obtains']
      for key in ['provider_misses','storage_callbacks','encoded_bytes','decode_attempts','decode_successes']:block[key]=0
    if case=='decision-change' and i==0:v['query_work']['counts']['centroid_distance_evaluations']+=1
    if case=='echo-change' and i==0:v['provider_cache_policy']={**policy,'shards':2}
    stdout.write((json.dumps(v)+'\n').encode())
   return types.SimpleNamespace(returncode=0)
  return run
 def test_replay_and_inconclusive(self):
  for case in ['reproduced','nonrepro']:
   with self.subTest(case=case),tempfile.TemporaryDirectory() as td:
    root=Path(td);s=self.spec(root);calls=[]
    with patch.object(m,'HERE',root),patch.object(m.sys,'platform','linux'),patch.object(m.subprocess,'run',side_effect=self.fake(calls,case)):r=m.run(s)
    self.assertEqual(r['status'],'passed',r['error']);self.assertEqual(len(calls),4);self.assertEqual([x['retained_storage_callbacks_64_vs_1'] for x in r['attribution']],[[46,0],[27,0]] if case=='reproduced' else [[0,0],[0,0]]);expected='consistent-with-shard-fragmentation-mitigation' if case=='reproduced' else 'inconclusive-baseline-did-not-reproduce';self.assertTrue(all(x['classification']==expected and x['no_retry'] for x in r['attribution']))
 def test_failures_stop(self):
  for case in ['exit7','decision-change','echo-change']:
   with self.subTest(case=case),tempfile.TemporaryDirectory() as td:
    root=Path(td);s=self.spec(root);calls=[]
    with patch.object(m,'HERE',root),patch.object(m.sys,'platform','linux'),patch.object(m.subprocess,'run',side_effect=self.fake(calls,case)):r=m.run(s)
    self.assertEqual(r['status'],'failed');self.assertEqual(len(calls),1);self.assertEqual(r['attribution'],[])
 def test_admission_and_digest(self):
  for case in ['wrong-binary','expired']:
   with self.subTest(case=case),tempfile.TemporaryDirectory() as td:
    root=Path(td);s=self.spec(root);calls=[]
    if case=='wrong-binary':s['binary']={**s['binary'],'sha256':'0'*64}
    else:s['deadline_unix']=m.time.time()-1
    with patch.object(m,'HERE',root),patch.object(m.sys,'platform','linux'),patch.object(m.subprocess,'run',side_effect=self.fake(calls,case)):
     with self.assertRaises(ValueError):m.run(s)
    self.assertEqual(calls,[])
  v=json.loads((OLD/'reference-four-bit.stdout').read_text().splitlines()[0]);changed=copy.deepcopy(v);changed['query_stage_trace']['pretrim_candidate_ids'].reverse();changed['query_stage_trace']['retained_candidate_ids'].reverse();self.assertEqual(m.decision_digest(v),m.decision_digest(changed));changed['ids'].reverse();self.assertNotEqual(m.decision_digest(v),m.decision_digest(changed))
if __name__=='__main__':unittest.main()
