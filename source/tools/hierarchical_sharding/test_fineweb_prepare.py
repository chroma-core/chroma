import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from .fineweb_prepare import DATASET, REVISION, DIMENSION, RangeReader, export, manifest_from_api


def parquet_bytes(vectors,ids):
    table=pa.table({'dense_embedding':pa.array(vectors,type=pa.list_(pa.float16())),'id':ids,'ignored_payload':['unused']*len(ids)})
    out=pa.BufferOutputStream();pq.write_table(table,out,row_group_size=2);return out.getvalue().to_pybytes()


def fixture(blobs):
    metadata=dict(sha=REVISION,siblings=[dict(rfilename=name,size=len(data),lfs=dict(sha256=hashlib.sha256(data).hexdigest())) for name,data in blobs.items()])
    return manifest_from_api(metadata)


class FineWebTests(unittest.TestCase):
    def test_exact_half_values_order_and_uuid_mapping(self):
        x=np.zeros((5,DIMENSION),dtype=np.float16);x[:,0]=[.5,1,1.5,2,2.5];x[:,1]=np.float16(.1234)
        blobs={'data/b.parquet':parquet_bytes(x[3:].tolist(),['urn:uuid:d','urn:uuid:e']), 'data/a.parquet':parquet_bytes(x[:3].tolist(),['urn:uuid:a','urn:uuid:b','urn:uuid:c'])}
        manifest=fixture(blobs)
        self.assertEqual(manifest['files'][0]['path'],'data/a.parquet')
        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp)/'run';report=export(manifest,out,4,batch_rows=2,open_source=lambda f:io.BytesIO(blobs[f['path']]))
            actual=np.fromfile(out/'corpus.f32',dtype='<f4').reshape(-1,DIMENSION)
            np.testing.assert_array_equal(actual,x[:4].astype(np.float32))
            self.assertEqual(float(actual[-1,0]),2.) # A normalized exporter fails this assertion.
            rows=[json.loads(line) for line in (out/'row-to-uuid.jsonl').read_text().splitlines()]
            self.assertEqual([r['global_id'] for r in rows],list(range(4)))
            self.assertEqual([r['source_uuid'] for r in rows],['urn:uuid:a','urn:uuid:b','urn:uuid:c','urn:uuid:d'])
            self.assertEqual([r['source_row'] for r in rows],[0,1,2,0])
            with sqlite3.connect(out/'uuid-to-row.sqlite') as db:self.assertEqual(db.execute('SELECT global_id FROM documents WHERE source_uuid=?',('urn:uuid:d',)).fetchone(),(3,))
            self.assertIsNone(report['query_provenance']);self.assertFalse(report['sources'][0]['publisher_whole_file_hash_locally_verified'])
            with self.assertRaises(FileExistsError):export(manifest,out,4,open_source=lambda f:io.BytesIO(blobs[f['path']]))

    def test_projected_parquet_export_through_range_reader(self):
        vectors=np.zeros((2,DIMENSION),dtype=np.float16);vectors[:,0]=[1,2]
        data=parquet_bytes(vectors.tolist(),['a','b']);blobs={'data/a.parquet':data}
        class Response(io.BytesIO):
            status=206
            def __init__(self,start,end):
                super().__init__(data[start:end+1]);self.headers={'Content-Range':f'bytes {start}-{end}/{len(data)}','ETag':'fixed'}
        def opener(request,timeout):
            start,end=map(int,request.headers['Range'][6:].split('-'));return Response(start,end)
        with tempfile.TemporaryDirectory() as tmp:
            report=export(fixture(blobs),Path(tmp)/'out',1,open_source=lambda f:RangeReader(f['url'],len(data),range_bytes=128,opener=opener))
            self.assertGreater(len(report['sources'][0]['fetched_ranges']),1)
            self.assertEqual(report['sources'][0]['projected_columns'],['dense_embedding','id'])
            np.testing.assert_array_equal(np.fromfile(Path(tmp)/'out/corpus.f32',dtype='<f4'),vectors[0].astype(np.float32))

    def test_duplicates_and_bad_schema_leave_no_completed_provenance(self):
        vector=[np.float16(1)]+[np.float16(0)]*(DIMENSION-1)
        blobs={'data/a.parquet':parquet_bytes([vector,vector],['same','same'])}
        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp)/'bad'
            with self.assertRaises(sqlite3.IntegrityError):export(fixture(blobs),out,2,open_source=lambda f:io.BytesIO(blobs[f['path']]))
            self.assertFalse((out/'provenance.json').exists())
            bad=pa.table({'dense_embedding':pa.array([[1.]*DIMENSION],type=pa.list_(pa.float32())),'id':['x']});buffer=pa.BufferOutputStream();pq.write_table(bad,buffer);blobs={'data/a.parquet':buffer.getvalue().to_pybytes()}
            with self.assertRaisesRegex(ValueError,'float16'):export(fixture(blobs),Path(tmp)/'schema',1,open_source=lambda f:io.BytesIO(blobs[f['path']]))

    def test_range_reader_checks_boundaries_etag_and_memory_limit(self):
        body=b'0123456789';requests=[]
        class Response(io.BytesIO):
            status=206
            def __init__(self,start,end,etag='fixed'):
                super().__init__(body[start:end+1]);self.headers={'Content-Range':f'bytes {start}-{end}/{len(body)}','ETag':etag}
        def opener(request,timeout):
            text=request.headers['Range'];start,end=map(int,text[6:].split('-'));requests.append(request);return Response(start,end)
        reader=RangeReader('https://source',10,max_read_bytes=6,range_bytes=2,opener=opener)
        reader.seek(-4,2);self.assertEqual(reader.read(4),b'6789');self.assertEqual(len(reader.ranges),2)
        self.assertEqual(requests[1].get_header('If-match'),'fixed')
        reader.seek(0)
        with self.assertRaisesRegex(ValueError,'memory bound'):reader.read(10)
        def ignored(request,timeout):
            response=Response(0,1);response.status=200;return response
        with self.assertRaisesRegex(ValueError,'honor'):RangeReader('https://source',10,opener=ignored).read(2)
        def changed(request,timeout):return Response(0,1,'changed')
        with self.assertRaisesRegex(ValueError,'source-byte'):RangeReader('https://source',10,opener=opener,budget=dict(max_bytes=1,bytes_read=0,deadline=None)).read(2)
        with self.assertRaises(TimeoutError):RangeReader('https://source',10,opener=opener,budget=dict(max_bytes=10,bytes_read=0,deadline=time.monotonic()-1)).read(2)
        reader.opener=changed;reader.seek(0)
        with self.assertRaisesRegex(ValueError,'ETag'):reader.read(2)

    def test_revision_and_order_are_pinned(self):
        with self.assertRaises(ValueError):manifest_from_api(dict(sha='main',siblings=[]))
        blobs={'data/a.parquet':b'fixture'};manifest=fixture(blobs);manifest['files']*=2
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):export(manifest,Path(tmp)/'bad',1)

if __name__=='__main__':unittest.main()
