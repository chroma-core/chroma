import importlib.util,json,struct,tempfile,unittest,types
from pathlib import Path
from unittest.mock import patch
P=Path(__file__).resolve().parent
sp=importlib.util.spec_from_file_location('query_runtime',P/'run-queries.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m)
class Wiring(unittest.TestCase):
 def test_actual_native_default_onebit_fp_and_saved_join(self):
  from stage_coverage import join_trace
  for arm in ['reference','owner']:
   values={label:[json.loads(v) for v in (P/'local-native'/f'{arm}-{label}.stdout').read_text().splitlines()] for label in ['default','one-bit','fp','four-bit']};requests=[json.loads(v) for v in (P/'local-native'/f'{arm}-fp-requests.jsonl').read_text().splitlines()]
   base=P.parent/'consolidated-owner-construction-v1/local-native';report=json.loads((base/(arm+'-report.json')).read_text());leaves={x['leaf_id']:x['gt_ids'] for x in report['owners'][0]['census']['leaves']};expected={x['query_id']:x['ids'] for x in (json.loads(v) for v in (base/'gt.jsonl').read_text().splitlines())}
   for i,q in enumerate(requests):
    a,b,c,d=[values[label][i] for label in ['default','one-bit','fp','four-bit']];self.assertEqual((a['ids'],a['distances']),(b['ids'],b['distances']));self.assertNotIn('query_navigation_policy',a);self.assertIsNone(b['fp_navigation_cache']);self.assertGreater(c['fp_navigation_cache']['point_gets'],0)
    self.assertEqual(d['compressed_navigation_cache']['retained_code_payload_bytes'],d['compressed_navigation_cache']['nodes']*140)
    for reply in [b,c,d]:
     m.validate(reply,q,dict(query_id=q['request_id'],ids=expected[q['request_id']]));coverage=join_trace(reply,expected[q['request_id']],leaves,list(range(64)));self.assertEqual(coverage['returned_gt_hits'],8)
 def test_prespecified_fixed_policy_and_payload(self):
  s=json.loads((P/'runtime-spec.template.json').read_text());self.assertEqual(s['settings'],[dict(name='beam32',beam=32,k=100,rerank_factor=2)]);self.assertEqual(s['navigation_policies'],['one-bit','fp','four-bit']);self.assertEqual((s['reader_seconds'],s['total_seconds']),(10,60));self.assertIsNone(s['deadline_unix'])
 def test_fake_complete_four_cells_and_failure(self):
  for fail in [False,True]:
   with self.subTest(fail=fail),tempfile.TemporaryDirectory() as td:
    root=Path(td);binary=root/'never-executed';binary.write_text('fake');q=root/'queries.jsonl';g=root/'gt.jsonl';q.write_text(''.join(json.dumps(dict(query_id=f'q{i}',vector=[0]*4))+'\n' for i in range(16)));g.write_text(''.join(json.dumps(dict(query_id=f'q{i}',ids=list(range(100))))+'\n' for i in range(16)));s=json.loads((P/'runtime-spec.template.json').read_text());s.update(binary=m.descriptor(binary),deadline_unix=m.time.time()+60,output=str(root/'out'));s['bindings'].update(queries=m.descriptor(q),ground_truth=m.descriptor(g))
    for arm in ['reference','owner']:
     store=root/arm;store.mkdir();mp=store/'id-map-1.u64le';mp.write_bytes(b''.join(i.to_bytes(8,'little') for i in range(8192)));mf=store/'manifest.json';mf.write_text(json.dumps(dict(generation=1,source_id='fake',dimension=768,metric='ip',vector_count=8192,root_ref=dict(kind='internal',id=999),id_mapping=dict(sha256=m.descriptor(mp)['sha256']))));quality=root/(arm+'-quality.json');quality.write_text(json.dumps(dict(owners=[dict(snapshot_sha256=m.descriptor(mf)['sha256'],census=dict(reachable_nodes=33,leaves=[dict(leaf_id=i,gt_ids=list(range(100)) if i==0 else []) for i in range(32)]))])));s['bindings'][arm]=dict(manifest=m.descriptor(mf),map=m.descriptor(mp),quality=m.descriptor(quality))
    (root/'runtime-spec.template.json').write_text(json.dumps(s));calls=[]
    def fake(command,stdin,stdout,stderr,timeout,env):
     self.assertLessEqual(timeout,10);self.assertIn('--trace-query-stages',command);self.assertEqual(command[-2],'--query-navigation');policy=command[-1];calls.append(command)
     if fail:return types.SimpleNamespace(returncode=7)
     requests=[json.loads(line) for line in stdin];self.assertEqual(len(requests),32)
     load=dict(root_id=999,nodes=33,dimension=768,point_gets=33,owned_f32_payload_bytes=33*768*4,logical_centroid_return_bytes=33*768*4,vector_access=dict(access_overflow=False),load_seconds=.001) if policy in ['fp','four-bit'] else None
     cache=load if policy=='fp' else None
     compressed=dict(policy='four-bit',root_id=999,nodes=33,dimension=768,code_bytes_per_node=396,retained_code_payload_bytes=33*396,temporary_owned_fp_payload_bytes=33*3072,accounted_preparation_peak_payload_bytes=33*(3072+396)+3072+396,per_distance_unpack_logical_payload_bytes=6144,fp_source_load=load) if policy=='four-bit' else None
     for request in requests:
      self.assertEqual(request['beam_max'],32);vals=list(range(100));counts=dict(expanded_internal_nodes=1,centroid_distance_evaluations=32,navigation_levels=1,peak_child_frontier=32,selected_leaves=32,nonempty_leaves_scored=32,posting_rows_scored_before_dedup=512,distinct_candidates_before_trim=512,candidates_after_trim=200,exact_embedding_gets=200,exact_vectors_reranked=200);work=dict(counts=counts,returned_results=100,returned_distance_bits=[struct.unpack('<I',struct.pack('<f',v))[0] for v in vals],selected_leaves=32,**{k:1 for k in ['navigate_ns','quantize_ns','distance_ns','sort_dedup_ns','rerank_ns']});reply=dict(query_navigation_policy=policy,fp_navigation_cache=cache,compressed_navigation_cache=compressed,query_reader_open_seconds=.001,query_navigation_snapshot=dict(generation=1,source_id='fake',dimension=768,metric='ip',root_ref=dict(kind='internal',id=999)),query_stage_trace_policy='bounded-query-stages-v1',query_stage_trace=dict(complete=True,id_cap=8192,selected_leaf_ids=list(range(32)),pretrim_candidate_ids=list(range(512)),retained_candidate_ids=list(range(200)),copy_elapsed_ns=1),request_id=request['request_id'],generation=1,shard_id='global',query_work_policy='ordinary-query-work-v1',query_work=work,ids=vals,distances=vals,actual_candidate_count=200,elapsed_ms=1,vector_pin_observation=dict(access_overflow=False,block_access={}));stdout.write((json.dumps(reply)+'\n').encode())
     return types.SimpleNamespace(returncode=0)
    with patch.object(m,'HERE',root),patch.object(m.sys,'platform','linux'),patch.object(m.subprocess,'run',side_effect=fake):v=m.run(s)
    self.assertEqual(v['status'],'failed' if fail else 'passed',v.get('error'));self.assertEqual(len(calls),1 if fail else 6)
    if not fail:self.assertEqual([c['navigation_policy'] for c in v['cells']],['one-bit','one-bit','fp','fp','four-bit','four-bit']);self.assertTrue(all(c['mean_recall_at_100']==1 for c in v['cells']))
if __name__=='__main__':unittest.main()
