import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from .fineweb_queries import recover_rows,freeze_pools,materialize_pools


class QueryTests(unittest.TestCase):
    def test_join_order_and_null_delimited_checksum(self):
        ids=[dict(msmarco_config='v2.1',msmarco_split='test',msmarco_query_id=str(i)) for i in (2,1)]
        rows,h=recover_rows(ids,[dict(query_id=1,query='one'),dict(query_id=2,query='two')])
        self.assertEqual([r['query'] for r in rows],['two','one'])
        self.assertEqual(h,hashlib.sha256(b'two\0one\0').hexdigest())
        with self.assertRaises(ValueError):recover_rows(ids,[dict(query_id=1,query='one'),dict(query_id=1,query='dup')])
        with self.assertRaises(KeyError):recover_rows(ids,[dict(query_id=1,query='one')])
        with self.assertRaises(ValueError):recover_rows(ids+ids,[dict(query_id=1,query='one'),dict(query_id=2,query='two')])

    def test_frozen_disjoint_pools_and_embedding_identity_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);rows=[dict(query_id=str(i),query='text '+str(i),publisher_row=i) for i in range(6)]
            text=root/'text.jsonl';text.write_text(''.join(json.dumps(r)+'\n' for r in rows))
            one=freeze_pools(text,root/'frozen',3,3,1729);two=freeze_pools(text,root/'same',3,3,1729)
            self.assertEqual(one['calibration_ids'],two['calibration_ids'])
            self.assertFalse(set(one['calibration_ids'])&set(one['test_ids']))
            embeddings=root/'embeddings.jsonl';vectors=[dict(query_id=r['query_id'],vector=[1.]+[0.]*767) for r in rows]
            embeddings.write_text(''.join(json.dumps(r)+'\n' for r in vectors))
            result=materialize_pools(embeddings,root/'frozen/frozen-pools.json',root/'pools')
            self.assertEqual(result['pool_sizes'],dict(calibration=3,test=3))
            vectors[-1]['query_id']='foreign';embeddings.write_text(''.join(json.dumps(r)+'\n' for r in vectors))
            with self.assertRaises(ValueError):materialize_pools(embeddings,root/'frozen/frozen-pools.json',root/'bad')

if __name__=='__main__':unittest.main()
