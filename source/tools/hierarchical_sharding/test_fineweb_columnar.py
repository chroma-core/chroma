import unittest
import numpy as np
import pyarrow as pa
from .fineweb_prepare import decode_batch


class ColumnarTests(unittest.TestCase):
    def test_sliced_variable_and_fixed_arrays_preserve_exact_values_and_uuid_order(self):
        rng=np.random.default_rng(8);x=rng.normal(size=(5,768)).astype(np.float16);ids=['a','é','文','d','e'];x[1,0]=np.float16(-0.0)
        for kind in [pa.list_(pa.float16()),pa.large_list(pa.float16()),pa.list_(pa.float16(),768)]:
            values=pa.array(x.tolist(),type=kind);batch=pa.record_batch([values,pa.array(ids)],names=['dense_embedding','id']).slice(1,3)
            actual,uuids=decode_batch(batch);np.testing.assert_array_equal(actual.view('<u4'),x[1:4].astype('<f4').view('<u4'));self.assertEqual(uuids,ids[1:4])

    def test_null_parent_child_uuid_and_wrong_dimensions_fail_closed(self):
        rows=[[1.]*768,[2.]*768]
        cases=[([None,rows[1]],['a','b']),([[None]+[1.]*767,rows[1]],['a','b']),([rows[0][:-1],rows[1]],['a','b']),(rows,['a',None]),([[0.]*768,rows[1]],['a','b'])]
        for values,ids in cases:
            batch=pa.record_batch([pa.array(values,type=pa.list_(pa.float16())),pa.array(ids)],names=['dense_embedding','id'])
            with self.assertRaises(ValueError):decode_batch(batch)

    def test_wrong_primitive_type_is_rejected(self):
        batch=pa.record_batch([pa.array([[1.]*768],type=pa.list_(pa.float32())),pa.array(['a'])],names=['dense_embedding','id'])
        with self.assertRaises(ValueError):decode_batch(batch)

if __name__=='__main__':unittest.main()
