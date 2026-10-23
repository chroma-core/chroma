import importlib.util,sys,types,unittest,tempfile,tarfile,hashlib,json,shutil
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,'/private/tmp/chroma-hierarchical-spann-sharding')
try:import boto3
except ImportError:sys.modules['boto3']=types.ModuleType('boto3')
spec=importlib.util.spec_from_file_location('checkpoint_archive',Path(__file__).with_name('archive_checkpoint_epoch.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Store:
 def __init__(self):self.files={}
 def upload(self,p):
  data=Path(p).read_bytes();key=hashlib.sha256(data).hexdigest();self.files[key]=data;return dict(sha256=key,bytes=len(data))
 def restore(self,d,p):Path(p).write_bytes(self.files[d['sha256']])
class ArchiveTests(unittest.TestCase):
 def test_full_preservation_and_probe_roundtrip(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);source=root/'source';(source/'7').mkdir(parents=True);(source/'7/data').write_bytes(b'payload');out=root/'out';out.mkdir();case=dict(source=str(source),name='case',slots=[dict(subtree_id='7',vector_count=3)],tree_sha256='a'*64)
   with patch.object(m,'pinned_probe',return_value={'ids':[7],'distances':[.2]}):r=m.archive_one(case,dict(binary='unused',query=[1.]),out,Store())
   self.assertEqual(r['status'],'passed');self.assertEqual((out/'case/restored/7/data').read_bytes(),b'payload');self.assertEqual(r['owners'],1)
 def test_unexpected_member_and_missing_member_fail(self):
  with tempfile.TemporaryDirectory() as t:
   r=Path(t);p=r/'a';p.write_bytes(b'x');a=r/'a.tar'
   with tarfile.open(a,'w') as f:f.add(p,arcname='../escape')
   with self.assertRaises(ValueError):m.extract_verified(a,r/'restore',{})
   with tarfile.open(a,'w'):pass
   with self.assertRaises(AssertionError):m.extract_verified(a,r/'restore2',{'missing':dict(bytes=1,sha256='x')})
 def test_failed_probe_persists_phase(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);source=root/'source';source.mkdir();out=root/'out';out.mkdir();case=dict(source=str(source),name='case',slots=[dict(subtree_id='7',vector_count=3)],tree_sha256='a'*64)
   with patch.object(m,'pinned_probe',side_effect=RuntimeError('reopen failed')):
    with self.assertRaises(RuntimeError):m.archive_one(case,dict(binary='unused',query=[1.]),out,Store())
   r=json.loads((out/'case/progress.json').read_text());self.assertEqual(r['status'],'failed');self.assertEqual(r['phase'],'source-probe')
 def test_changed_complete_inventory_blocks_upload(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);source=root/'source';(source/'7').mkdir(parents=True);(source/'7/data').write_bytes(b'x');out=root/'out';out.mkdir();case=dict(source=str(source),name='case',slots=[dict(subtree_id='7',vector_count=3)],tree_sha256='a'*64);store=Store()
   with patch.object(m,'pinned_probe',return_value={}),patch.object(m,'inventory',return_value={}):
    with self.assertRaises(AssertionError):m.archive_one(case,dict(binary='unused',query=[1.]),out,store)
   self.assertEqual(store.files,{});self.assertEqual(json.loads((out/'case/progress.json').read_text())['status'],'failed')
 def test_output_overlap_and_duplicate_slots_rejected(self):
  case=dict(name='case',source='/mnt/spann/store',tree_sha256='a'*64,slots=[dict(subtree_id='7',generation=1,vector_count=3,snapshot_sha256='b'*64,map_sha256='c'*64)])
  with self.assertRaises(ValueError):m.validate(dict(output='/mnt/spann/store/child',stores=[case]))
  case['slots']*=2
  with self.assertRaises(ValueError):m.validate(dict(output='/mnt/spann/archive',stores=[case]))
if __name__=='__main__':unittest.main()
