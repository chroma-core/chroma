import importlib.util,sys,types,unittest,tempfile,tarfile,io,json,hashlib
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,'/private/tmp/chroma-hierarchical-spann-sharding')
sys.modules.setdefault('boto3',types.SimpleNamespace())
p=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/pilot/fineweb10m-128/archive_checkpoint_epoch.py')
spec=importlib.util.spec_from_file_location('checkpoint_review',p);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Checks(unittest.TestCase):
 def test_valid_extract_and_corrupt_duplicate_traversal_reject(self):
  for kind in ['valid','corrupt','duplicate','traversal','symlink']:
   with self.subTest(kind=kind),tempfile.TemporaryDirectory() as td:
    root=Path(td);a=root/'data.tar';data=b'hello';members={'0/file':{'bytes':5,'sha256':hashlib.sha256(data).hexdigest()}}
    with tarfile.open(a,'w') as t:
     x=tarfile.TarInfo('../escape' if kind=='traversal' else '0/file');x.size=5
     if kind=='symlink':x.type=tarfile.SYMTYPE;x.linkname='/etc/passwd'
     t.addfile(x,io.BytesIO(b'other' if kind=='corrupt' else data))
     if kind=='duplicate':t.addfile(x,io.BytesIO(data))
    if kind=='valid':m.extract_verified(a,root/'out',members);self.assertEqual((root/'out/0/file').read_bytes(),data)
    else:
     with self.assertRaises((ValueError,AssertionError)):m.extract_verified(a,root/'out',members)
 def test_upload_failure_preserves_failed_phase(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);source=root/'source';source.mkdir();(source/'file').write_text('x');out=root/'out';out.mkdir()
   class Store:
    def upload(self,*args,**kwargs):raise RuntimeError('injected upload failure')
   with patch.object(m,'pinned_probe',return_value={'ids':[1]}):
    with self.assertRaises(RuntimeError):m.archive_one({'name':'fixture','source':str(source),'slots':[{'subtree_id':'0'}]}, {'binary':'unused','query':[1]},out,Store())
   r=json.loads((out/'fixture/progress.json').read_text());self.assertEqual(r['status'],'failed');self.assertEqual(r['phase'],'upload')
 def test_invalid_config_paths_and_duplicates_reject(self):
  base={'output':'/mnt/spann/review-output','stores':[{'name':'one','source':'/mnt/spann/review-input','tree_sha256':'1'*64,'slots':[{'subtree_id':'0','generation':1,'vector_count':1,'snapshot_sha256':'2'*64,'map_sha256':'3'*64}]}]}
  m.validate(base)
  import copy
  for kind in ['traversal','duplicate','overlap']:
   c=copy.deepcopy(base)
   if kind=='traversal':c['stores'][0]['name']='../bad'
   elif kind=='duplicate':c['stores']*=2
   else:c['output']=c['stores'][0]['source']+'/nested'
   with self.subTest(kind=kind),self.assertRaises(ValueError):m.validate(c)
if __name__=='__main__':unittest.main(verbosity=2)
