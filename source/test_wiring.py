import importlib.util,json,struct,tempfile,unittest,types
from pathlib import Path
from unittest.mock import patch
P=Path(__file__).resolve().parent
sp=importlib.util.spec_from_file_location('query_runtime',P/'run-queries.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m)
class Wiring(unittest.TestCase):
 def test_actual_native_counters_and_legacy_parity(self):
  for arm in ['reference','owner']:
   off=[json.loads(v) for v in (P/'local-native'/f'{arm}-off.stdout').read_text().splitlines()];on=[json.loads(v) for v in (P/'local-native'/f'{arm}-on.stdout').read_text().splitlines()];requests=[json.loads(v) for v in (P/'local-native'/f'{arm}-on-requests.jsonl').read_text().splitlines()]
   for a,b,q in zip(off,on,requests):
    self.assertNotIn('query_work',a);self.assertEqual((a['ids'],a['distances']),(b['ids'],b['distances']));v=m.validate(b,q,dict(query_id=q['request_id'],ids=b['ids']));self.assertEqual(v['counts']['posting_rows_scored_before_dedup'],64);self.assertEqual(v['counts']['exact_vectors_reranked'],64)
    bad=json.loads(json.dumps(b));bad['query_work']['counts']['distinct_candidates_before_trim']=0
    with self.assertRaises(ValueError):m.validate(bad,q,dict(query_id=q['request_id'],ids=b['ids']))
 def test_prespecified_bindings_budget(self):
  s=json.loads((P/'runtime-spec.template.json').read_text());self.assertEqual(s['settings'],m.SETTINGS);self.assertEqual((s['reader_seconds'],s['total_seconds']),(10,40));self.assertIsNone(s['deadline_unix']);self.assertEqual(s['bindings']['ground_truth']['sha256'],'dd27aa11120b5c9684be6f8f58b014a9862299c7314da6c2b2dc2620ea8b57f2')
 def test_fake_complete_pipeline_and_native_failure(self):
  for fail in [False,True]:
   with self.subTest(fail=fail),tempfile.TemporaryDirectory() as td:
    root=Path(td);binary=root/'never-executed';binary.write_text('fake');q=root/'queries.jsonl';g=root/'gt.jsonl';q.write_text(''.join(json.dumps(dict(query_id=f'q{i}',vector=[0]*4))+'\n' for i in range(16)));g.write_text(''.join(json.dumps(dict(query_id=f'q{i}',ids=list(range(100))))+'\n' for i in range(16)));s=json.loads((P/'runtime-spec.template.json').read_text());s.update(binary=m.descriptor(binary),deadline_unix=m.time.time()+40,output=str(root/'out'));s['bindings'].update(queries=m.descriptor(q),ground_truth=m.descriptor(g))
    for arm in ['reference','owner']:
     store=root/arm;store.mkdir();mp=store/'id-map-1.u64le';mp.write_bytes(b''.join(i.to_bytes(8,'little') for i in range(8192)));mf=store/'manifest.json';mf.write_text(json.dumps(dict(generation=1,dimension=768,metric='ip',vector_count=8192,id_mapping=dict(sha256=m.descriptor(mp)['sha256']))));s['bindings'][arm]=dict(manifest=m.descriptor(mf),map=m.descriptor(mp))
    (root/'runtime-spec.template.json').write_text(json.dumps(s));calls=[]
    def fake(command,stdin,stdout,stderr,timeout,env):
     self.assertNotIn('--generation',command);self.assertIn('--profile-query-work',command);self.assertIn('--profile-rerank-pins',command);self.assertLessEqual(timeout,10);calls.append(command)
     if fail:return types.SimpleNamespace(returncode=7)
     requests=[json.loads(line) for line in stdin];self.assertEqual(len(requests),32)
     for request in requests:
      vals=list(range(100));counts=dict(selected_leaves=request['beam_max'],nonempty_leaves_scored=request['beam_max'],posting_rows_scored_before_dedup=512,distinct_candidates_before_trim=512,candidates_after_trim=200,exact_embedding_gets=200,exact_vectors_reranked=200);work=dict(counts=counts,returned_results=100,returned_distance_bits=[struct.unpack('<I',struct.pack('<f',v))[0] for v in vals],selected_leaves=request['beam_max'],**{k:1 for k in ['navigate_ns','quantize_ns','distance_ns','sort_dedup_ns','rerank_ns']});reply=dict(request_id=request['request_id'],generation=1,shard_id='global',query_work_policy='ordinary-query-work-v1',query_work=work,ids=vals,distances=vals,actual_candidate_count=200,elapsed_ms=1,vector_pin_observation=dict(access_overflow=False,block_access={}));stdout.write((json.dumps(reply)+'\n').encode())
     return types.SimpleNamespace(returncode=0)
    with patch.object(m,'HERE',root),patch.object(m.sys,'platform','linux'),patch.object(m.subprocess,'run',side_effect=fake):v=m.run(s)
    self.assertEqual(v['status'],'failed' if fail else 'passed');self.assertEqual(len(calls),1 if fail else 4)
    if not fail:self.assertEqual(len(v['cells']),4);self.assertTrue(all(c['mean_recall_at_100']==1 for c in v['cells']))
if __name__=='__main__':unittest.main()
