import copy, importlib.util, json, os, struct, tempfile, types, unittest
from pathlib import Path
from unittest.mock import patch
P=Path(__file__).resolve().parent
sp=importlib.util.spec_from_file_location('quality_runtime',P/'run-quality.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m)
SOURCE=Path('/private/tmp/chroma-hierarchical-spann-sharding')

class Wiring(unittest.TestCase):
 def test_small_real_exact_engine_and_overlap_rejection(self):
  with tempfile.TemporaryDirectory() as td:
   out=Path(td);input=out/'small.f32';rows=[[float(i),float(i%2),-0.,1.] for i in range(8)];input.write_bytes(b''.join(struct.pack('<4f',*v) for v in rows))
   qs=[dict(query_id=f'q{i}',vector=[float(i)/7,.25,-0.,1.]) for i in range(16)];(out/'queries.jsonl').write_text(''.join(json.dumps(q)+'\n' for q in qs));ids=[q['query_id'] for q in qs]
   with patch.object(m,'HERE',out):value=m.prepare(input,out,SOURCE,ids,rows=8,dimension=4,k=3)
   gt=[json.loads(v) for v in Path(value['ground_truth']['path']).read_text().splitlines()]
   for q,actual in zip(qs,gt):
    expected=sorted(range(8),key=lambda i:(1-sum(a*b for a,b in zip(rows[i],q['vector'])),i))[:3];self.assertEqual(actual['ids'],expected)
   self.assertEqual(value['prefix_copy_bytes'],128);self.assertEqual((out/'evaluation-prefix.f32').read_bytes(),input.read_bytes())
   qs[0]['canonical_global_id']=7;(out/'queries.jsonl').write_text(''.join(json.dumps(q)+'\n' for q in qs))
   with self.assertRaisesRegex(ValueError,'overlaps'):m.check_queries(out/'queries.jsonl',ids,4)
   qs[0].pop('canonical_global_id');qs[0]['global_id']=3;qs[0]['id_namespace']='canonical-global';(out/'queries.jsonl').write_text(''.join(json.dumps(q)+'\n' for q in qs))
   with self.assertRaisesRegex(ValueError,'overlaps'):m.check_queries(out/'queries.jsonl',ids,4)
 def test_frozen_cohort_and_budget(self):
  s=json.loads((P/'runtime-spec.template.json').read_text());self.assertEqual((s['rows'],s['threads'],s['owner_slots']),(8192,4,4));self.assertEqual((s['setup_seconds'],s['arm_seconds'],s['total_seconds'],s['diagnostic_arm_seconds'],s['diagnostic_total_seconds'],s['scientific_seconds']),(60,30,60,30,60,180));self.assertIsNone(s['deadline_unix']);self.assertEqual(m.descriptor(P/'queries.jsonl')['sha256'],m.QUERY_SHA);self.assertEqual(len(m.check_queries(P/'queries.jsonl',s['query_ids'])),16)
 def test_fake_full_pipeline_success_and_failures(self):
  for fail_stage in [None,'setup','owner','owner-diagnostic']:
   with self.subTest(stage=fail_stage), tempfile.TemporaryDirectory() as td:
    root=Path(td);tree=root/'source';tree.mkdir();(root/'compile-native.py').write_text('def verify(source,manifest): pass\n');(root/'source-manifest.json').write_text('{}');(root/'runtime-spec.template.json').write_bytes((P/'runtime-spec.template.json').read_bytes());(root/'queries.jsonl').write_bytes((P/'queries.jsonl').read_bytes());(root/'query-provenance.json').write_bytes((P/'query-provenance.json').read_bytes());binary=root/'fake-native';binary.write_text('FAKE never executed');restore=root/'restore.json';s=json.loads((P/'runtime-spec.template.json').read_text());restore.write_text(json.dumps(dict(status='complete',canonical=s['vectors'],rows=2000000,dimension=768)));s.update(binary=m.descriptor(binary),restore_receipt=m.descriptor(restore),source=str(tree),output=str(root/'out'),deadline_unix=m.time.time()+180)
    (root/'runtime-spec.template.json').write_text(json.dumps(s));calls=[];snapshots={};original_stat=Path.stat
    def stat(path,*a,**kw):
     if str(path)==s['vectors']['path']:return types.SimpleNamespace(st_size=6144000000)
     return original_stat(path,*a,**kw)
    def child(command,stem,out,remaining,env):
     calls.append(stem);self.assertGreater(remaining,0);self.assertLessEqual(remaining,60 if stem=='setup' else 30)
     if stem==fail_stage:raise ValueError('injected '+stem)
     if stem=='setup':
      gt=out/'prefix-gt.jsonl';gt.write_text(''.join(json.dumps(dict(query_id=q,ids=list(range(i*100,(i+1)*100)),distances=list(range(100))))+'\n' for i,q in enumerate(s['query_ids'])));prep=dict(status='passed',prefix_copy_bytes=m.PREFIX_BYTES,rows=8192,k=100,queries=m.descriptor(root/'queries.jsonl'),ground_truth=m.descriptor(gt),prefix_sha256='a'*64);m.write(out/'preparation.json',prep);return prep
     if stem in ['reference','owner']:
      native=json.loads(Path(command[-1]).read_text());self.assertEqual(native['predicate_batching'],stem=='owner');self.assertEqual(native['batched_scrub_validation'],stem=='owner');self.assertFalse(native['direct_current_reads']);self.assertTrue(native['publish_checkpoint']);self.assertEqual(native['config'],s['config'])
      # Fake transport only. Reuse accepted output shape, not a native execution.
      base=P.parent/'consolidated-owner-construction-v1/local-native'/stem/'result.json';x=json.loads(base.read_text());x['spec']=native;x['spec_sha256']=m.descriptor(Path(command[-1]))['sha256'];x['census'].update(unique_live_rows=8192);x['phases'][0]['rows']=4096;x['phases'][1]['rows']=8192;x['all_id_exact_fp_distance_bits']=[[i,i] for i in range(8192)];store=out/stem/'store';store.mkdir(parents=True);mp=store/'id-map-1.u64le';mp.write_bytes(b''.join(struct.pack('<Q',i) for i in range(8192)));mapping=dict(path=mp.name,count=8192,bytes=65536,sha256=m.descriptor(mp)['sha256']);manifest=store/'manifest.json';m.write(manifest,dict(generation=1,source_id=native['source_id'],metric='ip',dimension=768,vector_count=8192,id_mapping=mapping,block_ids=x['roots'],construction=dict(config=native['config']),construction_input=dict(prefix_sha256=native['prefix_sha256'],vectors=native['vectors'],spec_sha256=x['spec_sha256'])));d=m.descriptor(manifest);x['checkpoint_publication']=dict(generation=1,manifest_path=str(manifest),manifest_bytes=d['bytes'],manifest_sha256=d['sha256'],id_mapping=mapping,map_write_bytes=65536,map_verify_bytes=65536);m.write(out/stem/'result.json',x);m.write(out/(stem+'.stdout'),x);snapshots[stem]=d;return x
     arm=stem.split('-')[0];ds=json.loads(Path(command[-1]).read_text());self.assertEqual(ds['owners'][0]['snapshot'],snapshots[arm]);self.assertEqual(ds['ground_truth']['path'],str((out/'prefix-gt.jsonl').resolve()))
     return dict(policy='saved-gt-quality-v1',declared_global_rows=8192,dimension=768,metric='ip',ground_truth=ds['ground_truth'],query_vectors=ds['query_vectors'],raw_embedding_reads=0,gt_union_count=1600,owners=[dict(owner_id='global',generation=1,snapshot_sha256=snapshots[arm]['sha256'],map_bytes_read=65536,census=dict(reachable_leaves=100))],queries=[dict(query_id=q,expected=100,covered=100,oracles=[{}, {}, {}]) for q in s['query_ids']])
    with patch.object(m,'HERE',root),patch.object(m.sys,'platform','linux'),patch.object(Path,'stat',stat),patch.object(m,'child',side_effect=child):v=m.run(s)
    self.assertEqual(v['status'],'failed' if fail_stage else 'passed',v.get('error'));self.assertEqual(json.loads((root/'out/result.json').read_text())['status'],v['status'])
    if fail_stage:self.assertEqual(calls[-1],fail_stage)
    else:
     self.assertEqual(calls,['setup','reference','owner','reference-diagnostic','owner-diagnostic']);self.assertEqual(v['diagnostics']['owner']['map_accounting']['controller_prepost_hash_read_bytes'],131072)
 def test_shape_and_missing_gt_fail_closed(self):
  x=json.loads((P.parent/'consolidated-owner-construction-v1/local-native/owner/result.json').read_text());x['census']['unique_live_rows']=8192;x['phases'][0]['rows']=4096;x['phases'][1]['rows']=8192;m.shape(x,True)
  x['owner_report']['placement_moves']=0
  with self.assertRaisesRegex(ValueError,'movement'):m.shape(x,True)
  with self.assertRaises((KeyError,ValueError)):m.quality({}, {}, {}, [])
if __name__=='__main__':unittest.main()
