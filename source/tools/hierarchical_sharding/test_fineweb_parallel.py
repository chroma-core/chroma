import io,json,tempfile,unittest
from pathlib import Path
import numpy as np
from .fineweb_parallel import ingest
from .fineweb_stream import prepare,_whole_prefix_model
from .test_fineweb_stream import fixture
from .tree_manifest import checked_artifact


class ParallelTests(unittest.TestCase):
    def test_parallel_canonical_ids_and_uniform_model_match_serial(self):
        x,manifest,opener=fixture()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);a=ingest(manifest,root/'parallel',21,workers=2,chunk_rows=5,batch_rows=4,open_source=opener)
            b=prepare(manifest,root/'serial',21,3,'epoch','source',chunk_rows=5,batch_rows=4,sample_size=12,open_source=opener)
            values=np.concatenate([np.fromfile(checked_artifact(c['vectors'],root/'parallel'),dtype='<f4').reshape(-1,768) for c in a['chunks']]);np.testing.assert_array_equal(values,x[:21])
            records=[json.loads(line) for c in a['chunks'] for line in checked_artifact(c['uuids'],root/'parallel').read_text().splitlines()]
            self.assertEqual([r['global_id'] for r in records],list(range(21)));self.assertEqual([r['source_uuid'] for r in records],['uuid-'+str(i) for i in range(21)])
            model=_whole_prefix_model(root/'parallel',a['chunks'],dict(count=21,seed=1729,sample_size=12,children=3,epoch='epoch',metric='ip',iterations=12))
            serial=json.loads((root/'serial/model.json').read_text());self.assertEqual(model,serial)

    def test_resume_reuses_verified_file_and_rejects_changed_source(self):
        _,manifest,opener=fixture();failed=False
        def flaky(entry):
            nonlocal failed
            if entry['path'].endswith('b.parquet') and not failed:
                failed=True;raise OSError('transient fixture failure')
            return opener(entry)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'out'
            # Probe failure happens before publication and leaves no completion.
            with self.assertRaises(OSError):ingest(manifest,root,21,workers=2,chunk_rows=5,batch_rows=4,retries=0,open_source=flaky)
            result=ingest(manifest,root,21,workers=2,chunk_rows=5,batch_rows=4,open_source=opener,resume=True)
            self.assertEqual(result['global_row_count'],21)
            with self.assertRaises(ValueError):ingest(manifest,root,21,open_source=opener,resume=True)

    def test_partial_file_failure_reuses_completed_file_and_crosschecks_rows(self):
        _,manifest,opener=fixture();calls={}
        def flaky(entry):
            path=entry['path'];calls[path]=calls.get(path,0)+1
            if path.endswith('b.parquet') and calls[path]==2:raise OSError('one ingest failure after metadata')
            return opener(entry)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'out'
            with self.assertRaises(OSError):ingest(manifest,root,21,workers=1,chunk_rows=5,batch_rows=4,retries=0,open_source=flaky)
            self.assertTrue((root/'files/000000/complete.json').exists())
            def resume_open(entry):
                self.assertTrue(entry['path'].endswith('b.parquet'))
                return opener(entry)
            result=ingest(manifest,root,21,workers=1,chunk_rows=5,batch_rows=4,open_source=resume_open,resume=True)
            self.assertEqual(result['global_row_count'],21)

    def test_larger_prefix_reuses_only_verified_complete_source_files(self):
        x,manifest,opener=fixture()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);ingest(manifest,root/'seed',11,workers=1,chunk_rows=5,batch_rows=4,open_source=opener)
            calls={}
            def counted(entry):
                calls[entry['path']]=calls.get(entry['path'],0)+1
                return opener(entry)
            result=ingest(manifest,root/'larger',21,workers=2,chunk_rows=5,batch_rows=4,open_source=counted,seed_previous=root/'seed/ingest.json')
            self.assertEqual(calls['data/a.parquet'],1)  # Metadata probe only; no vector reread.
            actual=np.concatenate([np.fromfile(checked_artifact(c['vectors'],root/'larger'),dtype='<f4').reshape(-1,768) for c in result['chunks']]);np.testing.assert_array_equal(actual,x[:21])
            receipt=root/'seed/files/000000/complete.json';data=json.loads(receipt.read_text());data['chunks'][0]['vectors']['sha256']='0'*64;receipt.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError,'bound'):ingest(manifest,root/'corrupt',21,workers=2,chunk_rows=5,batch_rows=4,open_source=opener,seed_previous=root/'seed/ingest.json')

    def test_duplicates_and_output_cap_fail_closed(self):
        _,manifest,opener=fixture(True)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            import sqlite3
            with self.assertRaises(sqlite3.IntegrityError):ingest(manifest,root/'duplicates',23,workers=2,chunk_rows=5,batch_rows=4,open_source=opener)
            self.assertFalse((root/'duplicates/complete.json').exists())
            with self.assertRaises(ValueError):ingest(manifest,root/'small',23,workers=2,chunk_rows=5,batch_rows=4,open_source=opener,max_output_bytes=100)
            self.assertFalse((root/'small/complete.json').exists())

if __name__=='__main__':unittest.main()
