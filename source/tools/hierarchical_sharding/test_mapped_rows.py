import tempfile,unittest
from pathlib import Path
import numpy as np
from .mapped_rows import mapped_rows,copy_rows

class MappingTests(unittest.TestCase):
    def test_copy_outlives_closed_mapping(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'rows';values=np.arange(64,dtype='<f4').reshape(16,4);values.tofile(path)
            for _ in range(64):
                with mapped_rows(path,values.shape) as source:
                    mapping=source._mmap;out=np.array(source[:4],copy=True)
                self.assertTrue(mapping.closed);np.testing.assert_array_equal(out,values[:4])
                result=copy_rows(path,values.shape,np.array([15,0,8]));self.assertIsNone(result.base);np.testing.assert_array_equal(result,values[[15,0,8]])
    def test_exception_closes_mapping(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'rows';np.ones((4,4),dtype='<f4').tofile(path)
            with self.assertRaises(RuntimeError):
                with mapped_rows(path,(4,4)) as source:
                    mapping=source._mmap;raise RuntimeError('fault')
            self.assertTrue(mapping.closed)
    def test_exact_reference_closes_mapping_on_success_and_error(self):
        from unittest.mock import patch
        from .exact_blocked import exact_blocked
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'rows';np.ones((4,4),dtype='<f4').tofile(path)
            original=np.memmap;opened=[]
            def track(*args,**kwargs):
                value=original(*args,**kwargs);opened.append(value._mmap);return value
            with patch('tools.hierarchical_sharding.exact_blocked.np.memmap',side_effect=track):
                exact_blocked(path,[dict(query_id='q',vector=[1]*4)],4,1,'ip')
                with self.assertRaises(ValueError):exact_blocked(path,[dict(query_id='q',vector=[1])],4,1,'ip')
            self.assertEqual(len(opened),2);self.assertTrue(all(value.closed for value in opened))
