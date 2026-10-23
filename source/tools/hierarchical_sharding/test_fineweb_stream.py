import io
import json
from pathlib import Path
import tempfile
import unittest
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from .fineweb_prepare import manifest_from_api,REVISION
from .fineweb_stream import prepare
from .tree_manifest import checked_artifact


def fixture(duplicate=False):
    rng=np.random.default_rng(42);x=rng.normal(size=(23,768));x=(x/np.linalg.norm(x,axis=1)[:,None]).astype(np.float16)
    ids=['uuid-'+str(i) for i in range(23)]
    if duplicate:ids[-1]=ids[0]
    blobs={}
    for name,a,b in [('data/a.parquet',0,11),('data/b.parquet',11,23)]:
        table=pa.table({'dense_embedding':pa.array(x[a:b].tolist(),type=pa.list_(pa.float16())),'id':ids[a:b]});out=pa.BufferOutputStream();pq.write_table(table,out,row_group_size=3);blobs[name]=out.getvalue().to_pybytes()
    import hashlib
    manifest=manifest_from_api(dict(sha=REVISION,siblings=[dict(rfilename=k,size=len(v),lfs=dict(sha256=hashlib.sha256(v).hexdigest())) for k,v in blobs.items()]))
    return x.astype('<f4'),manifest,lambda f:io.BytesIO(blobs[f['path']])


class StreamTests(unittest.TestCase):
    def test_chunked_and_monolithic_model_and_ownership_match(self):
        x,manifest,opener=fixture()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            a=prepare(manifest,root/'chunked',21,3,'epoch','source',chunk_rows=5,batch_rows=4,sample_size=12,open_source=opener)
            b=prepare(manifest,root/'whole',21,3,'epoch','source',chunk_rows=21,batch_rows=4,sample_size=12,open_source=opener)
            self.assertEqual(a['model']['sha256'],b['model']['sha256'])
            model=json.loads((root/'chunked/model.json').read_text());self.assertGreater(max(model['sample_row_ids']),15)
            self.assertEqual(len(model['sample_row_ids']),12)
            mapped=[]
            for sa,sb in zip(a['subtrees'],b['subtrees']):
                self.assertEqual(sa['vectors']['sha256'],sb['vectors']['sha256']);self.assertEqual(sa['local_to_global']['sha256'],sb['local_to_global']['sha256'])
                ids=np.fromfile(checked_artifact(sa['local_to_global'],root/'chunked'),dtype='<u8');mapped.extend(ids.tolist())
                vectors=np.fromfile(checked_artifact(sa['vectors'],root/'chunked'),dtype='<f4').reshape(-1,768)
                np.testing.assert_array_equal(vectors,x[ids])
            self.assertEqual(sorted(mapped),list(range(21)))
            self.assertTrue((root/'chunked/complete.json').exists())
            with self.assertRaises(ValueError):prepare(manifest,root/'chunked',21,3,'epoch','source',chunk_rows=5,batch_rows=4,sample_size=12,open_source=opener,resume=True)

    def test_verified_resume_matches_clean_run_and_rejects_corruption(self):
        _,manifest,opener=fixture()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);settings=dict(chunk_rows=5,batch_rows=4,sample_size=12,open_source=opener)
            with self.assertRaises(InterruptedError):prepare(manifest,root/'resumed',21,3,'epoch','source',stop_after_chunks=2,**settings)
            checkpoint=json.loads((root/'resumed/checkpoint.json').read_text());self.assertEqual(checkpoint['committed_rows'],10);self.assertFalse((root/'resumed/complete.json').exists())
            actual=prepare(manifest,root/'resumed',21,3,'epoch','source',resume=True,**settings)
            clean=prepare(manifest,root/'clean',21,3,'epoch','source',**settings)
            self.assertEqual(actual['model']['sha256'],clean['model']['sha256'])
            self.assertEqual([s['vectors']['sha256'] for s in actual['subtrees']],[s['vectors']['sha256'] for s in clean['subtrees']])
            with self.assertRaises(InterruptedError):prepare(manifest,root/'corrupt',21,3,'epoch','source',stop_after_chunks=1,**settings)
            file=root/'corrupt/chunks/000000.f32';data=bytearray(file.read_bytes());data[0]^=1;file.write_bytes(data)
            with self.assertRaisesRegex(ValueError,'digest'):prepare(manifest,root/'corrupt',21,3,'epoch','source',resume=True,**settings)
            with self.assertRaises(ValueError):prepare(manifest,root/'corrupt',21,2,'epoch','source',resume=True,**settings)

    def test_unpinned_source_and_invalid_limits_are_rejected(self):
        _,manifest,opener=fixture()
        with tempfile.TemporaryDirectory() as tmp:
            bad=json.loads(json.dumps(manifest));bad['files'][0]['url']='https://example.com/unpinned'
            with self.assertRaisesRegex(ValueError,'revision'):prepare(bad,Path(tmp)/'bad',21,3,'epoch','source',open_source=opener)
            bad=json.loads(json.dumps(manifest));bad['files'][0]['publisher_sha256']='invalid'
            with self.assertRaisesRegex(ValueError,'identity'):prepare(bad,Path(tmp)/'bad',21,3,'epoch','source',open_source=opener)
            with self.assertRaisesRegex(ValueError,'positive'):prepare(manifest,Path(tmp)/'bad',21,3,'epoch','source',max_wall_s=0,open_source=opener)

    def test_cross_chunk_duplicate_uuid_prevents_completion(self):
        _,manifest,opener=fixture(True)
        import sqlite3
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'bad'
            with self.assertRaises(sqlite3.IntegrityError):prepare(manifest,root,23,3,'epoch','source',chunk_rows=5,batch_rows=4,sample_size=12,open_source=opener)
            self.assertFalse((root/'complete.json').exists());self.assertFalse((root/'input.json').exists())

if __name__=='__main__':unittest.main()
