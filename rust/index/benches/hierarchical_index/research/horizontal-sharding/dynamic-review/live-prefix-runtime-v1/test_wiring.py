import copy,hashlib,importlib.util,json,os,tempfile,time,types,unittest
from pathlib import Path
from unittest.mock import patch
HERE=Path(__file__).resolve().parent;sp=importlib.util.spec_from_file_location('prefix_pair',HERE/'run-pair.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m)
BASE=HERE.parents[1]/'fleet/matched-pruning-16k-v1/runtime-evidence/runtime-attempt1/result.json'
# HERE.parents[1] is dynamic-review's parent horizontal-sharding.
BASE=HERE.parent.parent/'fleet/matched-pruning-16k-v1/runtime-evidence/runtime-attempt1/result.json'
FIXED=json.loads(BASE.read_text())['results']['dynamic4']
LIVE=json.loads((HERE.parent/'live-prefix-version-batching-v1/MECHANISM-RESULT.json').read_text())['integrated']['live_prefix']

def sample(enabled):
    value=copy.deepcopy(FIXED);work=copy.deepcopy(LIVE)
    if not enabled:
        for k in ['batches','requested_rows','consumed_rows','actions','owner_calls','owner_requested_rows']:work[k]=0
        work['requested_lengths']=[0]*65;work['consumed_lengths']=[0]*65
    work['enabled']=enabled;value['owner_report']['live_prefix_validation']=work;return value

class Wiring(unittest.TestCase):
    def test_fixed_template_and_real_small_prefix_hash(self):
        s=json.loads((HERE/'runtime-spec.template.json').read_text());self.assertEqual((s['rows'],s['threads'],s['balance_every'],s['owner_slots'],s['move_above']),(16384,4,4096,4,4096));self.assertEqual((s['arm_seconds'],s['total_seconds'],s['scientific_seconds']),(30,60,70));self.assertEqual(s['prefix_sha256'],'4b8357b1198989ca815d3775ecc0aec56f4adca5a7eb2123337a50f4631d79eb');self.assertIsNone(s['deadline_unix'])
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'source';p.write_bytes(b'12345678');v=m.prefix_hash(p,4,time.time()+5);self.assertEqual(v['sha256'],hashlib.sha256(b'1234').hexdigest());self.assertEqual(v['bytes'],4)
            with self.assertRaises(ValueError):m.prefix_hash(p,9,time.time()+5)
            with self.assertRaises(ValueError):m.prefix_hash(p,4,time.time()-1)

    def test_actual_counter_histogram_and_false_policy_gate(self):
        for enabled,arm in [(False,'baseline'),(True,'live-prefix')]:
            value=sample(enabled);summary=m.validate(value,value['spec'],arm);self.assertEqual(summary['discarded_suffix_rows'],summary['requested_rows']-summary['consumed_rows']);self.assertEqual(summary['logical_requested_id_bytes'],summary['owner_requested_rows']*4)
            bad=copy.deepcopy(value);bad['owner_report']['live_prefix_validation']['requested_lengths'][0]+=1
            with self.assertRaises(ValueError):m.validate(bad,bad['spec'],arm)
            bad=copy.deepcopy(value);bad['owner_report']['live_prefix_validation']['enabled']=not enabled
            with self.assertRaises(ValueError):m.validate(bad,bad['spec'],arm)

    def test_fake_pair_success_and_preserved_second_arm_failure(self):
        for failure in [False,True]:
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as td:
                root=Path(td);tree=root/'source';tree.mkdir();(root/'compile-native.py').write_text('def verify(source,manifest):pass\n');(root/'source-manifest.json').write_text('{}');binary=root/'fake';binary.write_text('not executed');s=json.loads((HERE/'runtime-spec.template.json').read_text());receipt=root/'receipt.json';receipt.write_text(json.dumps(dict(status='complete',canonical=s['vectors'],rows=2000000,dimension=768)));s.update(source=str(tree),output=str(root/'out'),binary=m.descriptor(binary),restore_receipt=m.descriptor(receipt),deadline_unix=time.time()+500);(root/'runtime-spec.template.json').write_text(json.dumps(s));calls=[];original_stat=Path.stat
                def stat(p,*a,**kw):
                    if str(p)==s['vectors']['path']:return types.SimpleNamespace(st_size=6144000000)
                    return original_stat(p,*a,**kw)
                def child(command,stem,out,remaining,env):
                    calls.append(stem);self.assertGreater(remaining,0);self.assertLessEqual(remaining,30);native=json.loads(Path(command[-1]).read_text());self.assertEqual(native['live_prefix_validation'],stem=='live-prefix');self.assertTrue(native['predicate_batching']);self.assertTrue(native['batched_scrub_validation']);self.assertFalse(native['direct_current_reads']);self.assertEqual(native['config'],s['config'])
                    if failure and stem=='live-prefix':raise ValueError('injected native failure')
                    value=sample(stem=='live-prefix');value.update(spec=native,spec_sha256=m.descriptor(Path(command[-1]))['sha256']);store=out/stem/'store';store.mkdir(parents=True);mp=store/'id-map-1.u64le';mp.write_bytes(b''.join(i.to_bytes(8,'little') for i in range(16384)));mapping=dict(path=mp.name,count=16384,bytes=131072,sha256=m.descriptor(mp)['sha256']);manifest=store/'manifest.json';m.write(manifest,dict(generation=1,source_id=native['source_id'],metric='ip',dimension=768,vector_count=16384,id_mapping=mapping,block_ids=value['roots'],construction=dict(config=native['config']),construction_input=dict(prefix_sha256=native['prefix_sha256'],vectors=native['vectors'],spec_sha256=value['spec_sha256'])));pin=m.descriptor(manifest);value['checkpoint_publication']=dict(generation=1,manifest_path=str(manifest),manifest_bytes=pin['bytes'],manifest_sha256=pin['sha256'],id_mapping=mapping,map_write_bytes=131072,map_verify_bytes=131072);m.write(out/stem/'result.json',value);return value
                with patch.object(m,'HERE',root),patch.object(m.sys,'platform','linux'),patch.object(Path,'stat',stat),patch.object(m,'prefix_hash',return_value=dict(bytes=50331648,sha256=s['prefix_sha256'])),patch.object(m,'child',side_effect=child):value=m.run(s)
                self.assertEqual(value['status'],'failed' if failure else 'passed',value.get('error'));self.assertEqual(calls,['baseline','live-prefix']);self.assertTrue((root/'out/baseline/result.json').exists())

if __name__=='__main__':unittest.main()
