import copy,importlib.util,json,tempfile,unittest
from pathlib import Path
HERE=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('quality_cap_run',HERE/'run-quality.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Cap(unittest.TestCase):
    def test_exact_pretty_utf8_newline_and_self_size_boundary(self):
        value=dict(status='completed',error=None,data={'unicode':['é']*20})
        payload=m.serialized_result(copy.deepcopy(value),7);exact=7+len(payload)
        self.assertGreater(exact,7+len(json.dumps(value).encode())+1)
        for cap,expected in [(exact,'completed'),(exact-1,'failed'),(1,'failed')]:
            with self.subTest(cap=cap),tempfile.TemporaryDirectory() as td:
                out=Path(td);result=m.write_final_result(out,copy.deepcopy(value),7,cap);raw=(out/'result.json').read_bytes();self.assertEqual(result['status'],expected);self.assertEqual(result['metadata_including_result_bytes'],7+len(raw));self.assertEqual(json.loads(raw),result);self.assertEqual(raw,(json.dumps(result,indent=2)+'\n').encode('utf-8'));self.assertEqual(result['data'],value['data'])
                if expected=='completed':self.assertLessEqual(7+len(raw),cap)
                else:self.assertEqual(result['error'],'metadata cap exceeded')
if __name__=='__main__':unittest.main()
