import copy,json,tempfile,time,unittest,hashlib,shutil
from pathlib import Path
from .fineweb_scale_archive import archive_and_restore
class Store:
 def __init__(self):self.data={};self.uploads=0
 def upload(self,path,part_bytes):
  b=Path(path).read_bytes();h=hashlib.sha256(b).hexdigest();self.data[h]=b;self.uploads+=1;return dict(sha256=h,bytes=len(b),version_id='fixed')
 def restore(self,desc,path):
  Path(path).write_bytes(self.data[desc['sha256']]);return dict(sha256=desc['sha256'],bytes=desc['bytes'],version_id='fixed')
class ArchiveTests(unittest.TestCase):
 def fixture(self,root):
  data=root/'data';data.mkdir();(data/'current').write_bytes(b'canonical');(data/'failed-attempt').write_bytes(b'canonical')
  return dict(stop_epoch=time.time()+4000,archive=dict(output=str(root/'archive'),restore=str(root/'restore'),scopes=[str(data)],concurrency=2,part_bytes=64*1024**2,maximum_part_count=10000))
 def test_all_logical_attempts_have_full_version_pinned_restore(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);job=self.fixture(root);store=Store();r=archive_and_restore(job,store)
   self.assertEqual(r['status'],'passed');self.assertEqual(r['unique_objects'],1);self.assertEqual(r['logical_files'],2)
   self.assertEqual({x['relative_path'] for x in r['files']},{'current','failed-attempt'});self.assertEqual((root/'restore/0').read_bytes(),b'canonical')
 def test_expired_budget_or_unrestorable_object_cannot_pass(self):
  for failure in ('time','restore'):
   with self.subTest(failure=failure),tempfile.TemporaryDirectory() as d:
    root=Path(d);job=self.fixture(root);store=Store()
    if failure=='time':job['stop_epoch']=time.time()+1700
    else:
     def bad(*_):raise ValueError('restore mismatch')
     store.restore=bad
    with self.assertRaises((ValueError,TimeoutError)):archive_and_restore(job,store)
    self.assertEqual(json.loads((root/'archive/report.json').read_text())['status'],'failed');self.assertFalse((root/'archive/catalog-object.json').exists())
if __name__=='__main__':unittest.main()
