import copy,importlib.util,json,os,struct,tempfile,time,types,unittest
from pathlib import Path
from unittest.mock import patch
HERE=Path(__file__).resolve().parent
sp=importlib.util.spec_from_file_location('matched',HERE/'run-matched.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m)
SOURCE=Path('/private/tmp/chroma-hierarchical-spann-sharding')

class Wiring(unittest.TestCase):
    def test_actual_fixed_cohort_and_budget(self):
        s=json.loads((HERE/'runtime-spec.template.json').read_text());self.assertEqual(len(m.check_queries(HERE/'queries.jsonl',s['query_ids'])),100);self.assertEqual(m.descriptor(HERE/'queries.jsonl')['sha256'],m.QUERY_SHA)
        self.assertEqual((s['rows'],s['arm_seconds'],s['total_seconds'],s['diagnostic_total_seconds'],s['query_total_seconds'],s['scientific_seconds']),(16384,180,540,90,90,780));self.assertEqual(s['cache_policy'],m.CACHE);self.assertEqual(s['structural_pass_queries'],80);self.assertIsNone(s['deadline_unix'])

    def test_small_exact_engine_and_overlap_rejection(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td);vectors=[[float(i),1.,-0.,float(i%3)] for i in range(8)];source=out/'source.f32';source.write_bytes(b''.join(struct.pack('<4f',*v) for v in vectors));qpath=out/'queries.jsonl';queries=[dict(query_id='q'+str(i),vector=[i/11.,.5,-0.,.3]) for i in range(3)];qpath.write_text(''.join(json.dumps(v)+'\n' for v in queries));ids=[q['query_id'] for q in queries]
            result=m.prepare(source,out,SOURCE,qpath,ids,rows=8,dimension=4,k=3,query_count=3);gt=[json.loads(v) for v in Path(result['ground_truth']['path']).read_text().splitlines()]
            for q,row in zip(queries,gt):self.assertEqual(row['ids'],sorted(range(8),key=lambda i:(1-sum(a*b for a,b in zip(vectors[i],q['vector'])),i))[:3])
            self.assertEqual(result['prefix_copy_bytes'],128);self.assertEqual((out/'evaluation-prefix.f32').read_bytes(),source.read_bytes())
            queries[0]['canonical_global_id']=7;qpath.write_text(''.join(json.dumps(v)+'\n' for v in queries))
            with self.assertRaisesRegex(ValueError,'overlaps'):m.check_queries(qpath,ids,4,3,8)
            queries[0].pop('canonical_global_id');queries[0]['vector']=vectors[0];qpath.write_text(''.join(json.dumps(v)+'\n' for v in queries));other=out/'other';other.mkdir()
            with self.assertRaisesRegex(ValueError,'duplicate'):m.prepare(source,other,SOURCE,qpath,ids,rows=8,dimension=4,k=3,query_count=3)

    def test_structural_gate_uses_internal_kind_not_leaf_rejection(self):
        counts=dict(expanded_internal_nodes=2,centroid_distance_evaluations=4,navigation_levels=2,peak_child_frontier=2,selected_leaves=1,navigation_level_trace=dict(complete=True,level_cap=128,levels=[dict(level=1,expanded_internal=1,offered_internal=2,retained_internal=1,discarded_internal=1,offered_leaf=0,retained_leaf=0,discarded_leaf=0),dict(level=2,expanded_internal=1,offered_internal=0,retained_internal=0,discarded_internal=0,offered_leaf=2,retained_leaf=1,discarded_leaf=1)]))
        self.assertTrue(m.validate_query_work_counts(counts,saved_internal_nodes=3,require_trace=True)['internal_pruning']);counts['navigation_level_trace']['complete']=False
        with self.assertRaises(ValueError):m.validate_query_work_counts(counts,saved_internal_nodes=3,require_trace=True)

    def test_child_wrapper_records_actual_small_process_resources(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td);value=m.child([__import__('sys').executable,'-c','import json;print(json.dumps({"ok":True}))'],'local',out,5,os.environ.copy())
            self.assertEqual(value,{"ok":True});receipt=json.loads((out/'local-process.json').read_text());self.assertEqual(receipt['returncode'],0);self.assertGreaterEqual(receipt['observed_cumulative_child_maxrss_kib'],0);self.assertGreaterEqual(receipt['user_cpu_seconds'],0)

    def test_fake_dispatch_success_structural_failure_and_stage_failure(self):
        for failure in [None,'structural','dynamic1']:
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as td:
                root=Path(td);tree=root/'source';tree.mkdir();(root/'compile-native.py').write_text('def verify(source, manifest): pass\n');(root/'source-manifest.json').write_text('{}');(root/'queries.jsonl').write_bytes((HERE/'queries.jsonl').read_bytes());binary=root/'fake';binary.write_text('never executed');s=json.loads((HERE/'runtime-spec.template.json').read_text());receipt=root/'restore.json';receipt.write_text(json.dumps(dict(status='complete',canonical=s['vectors'],rows=2000000,dimension=768)));s.update(binary=m.descriptor(binary),source=str(tree),restore_receipt=m.descriptor(receipt),deadline_unix=time.time()+1000,output=str(root/'out'));(root/'runtime-spec.template.json').write_text(json.dumps(s));calls=[];native_specs={};snapshots={};orig_stat=Path.stat
                def stat(path,*a,**kw):
                    if str(path)==s['vectors']['path']:return types.SimpleNamespace(st_size=6144000000)
                    return orig_stat(path,*a,**kw)
                def child(command,stem,out,remaining,env):
                    calls.append(stem);self.assertGreater(remaining,0)
                    if stem==failure:raise ValueError('injected failure')
                    if stem=='setup':
                        gt=out/'prefix-gt.jsonl';gt.write_text(''.join(json.dumps(dict(query_id=q,ids=list(range(i,i+100)),distances=list(range(100))))+'\n' for i,q in enumerate(s['query_ids'])));prep=dict(prefix_copy_bytes=m.ROWS*m.DIM*4,rows=m.ROWS,queries=m.descriptor(root/'queries.jsonl'),ground_truth=m.descriptor(gt),prefix_sha256='a'*64);m.write(out/'preparation.json',prep);return prep
                    if stem in m.ARMS:
                        n=json.loads(Path(command[-1]).read_text());native_specs[stem]=n;self.assertEqual(n['owner_slots'],1 if stem=='dynamic1' else 4);self.assertEqual(n['skip_placement'],stem=='dynamic1');self.assertEqual(n['predicate_batching'],stem!='reference');self.assertFalse(n['direct_current_reads']);self.assertEqual(n['balance_every'],4096)
                        value=dict(spec=n,spec_sha256=m.descriptor(Path(command[-1]))['sha256'],all_id_exact_fp_distance_bits=[[i,i] for i in range(m.ROWS)]);(out/stem).mkdir();m.write(out/stem/'result.json',value);return value
                    arm=stem.split('-diagnostic-')[0];ds=json.loads(Path(command[-1]).read_text());selected=[q['query_id'] for q in ds['selected_queries']];self.assertLessEqual(len(selected),16);allgt={v['query_id']:v for v in map(json.loads,Path(ds['ground_truth']['path']).read_text().splitlines())};union=set().union(*(set(allgt[q]['ids']) for q in selected))
                    return dict(spec_sha256=m.descriptor(Path(command[-1]))['sha256'],source_id=ds['source_id'],policy='saved-gt-quality-v1',declared_global_rows=m.ROWS,dimension=m.DIM,metric='ip',ground_truth=ds['ground_truth'],query_vectors=ds['query_vectors'],raw_embedding_reads=0,gt_union_count=len(union),queries=[dict(query_id=q,expected=100,covered=100,oracles=[{}, {}, {}]) for q in selected],owners=[dict(owner_id='global',generation=1,snapshot_sha256=ds['owners'][0]['snapshot']['sha256'],census=dict(reachable_nodes=10,reachable_leaves=7))])
                def checkpoint(value,n):
                    store=Path(n['output'])/'store';store.mkdir();manifest=store/'manifest.json';manifest.write_text('{}');mapping=store/'map';mapping.write_bytes(b'fake');return m.descriptor(manifest),m.descriptor(mapping)
                def query(*args):return dict(structural_gate_pass=failure!='structural')
                with patch.object(m,'HERE',root),patch.object(m.sys,'platform','linux'),patch.object(Path,'stat',stat),patch.object(m,'child',side_effect=child),patch.object(m,'shape'),patch.object(m,'checkpoint',side_effect=checkpoint),patch.object(m,'query_arm',side_effect=query):v=m.run(s)
                self.assertEqual(v['status'],'failed' if failure=='dynamic1' else 'structurally-insufficient' if failure=='structural' else 'passed',v.get('error'))
                if failure=='dynamic1':self.assertEqual(calls[-1],'dynamic1')
                else:self.assertEqual(calls[:4],['setup',*m.ARMS]);self.assertEqual(len(calls),25)

if __name__=='__main__':unittest.main()
