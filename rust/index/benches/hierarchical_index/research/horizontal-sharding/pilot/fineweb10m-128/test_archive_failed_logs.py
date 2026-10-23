"""Exercise preservation and failure journals on tiny Linux-owned log fixtures."""
import hashlib,importlib.util,io,json,pathlib,signal,tempfile,unittest
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('archive_failed_logs',pathlib.Path(__file__).with_name('archive_failed_logs.py'));module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class Store:
 def __init__(self,*args):self.objects={}
 def upload(self,path):
  data=path.read_bytes();key=hashlib.sha256(data).hexdigest();self.objects[key]=data;return {'sha256':key,'bytes':len(data),'version_id':'fixture'}
 def restore(self,desc,path):path.write_bytes(self.objects[desc['sha256']])
class Opener:
 def open(self,request,timeout):
  if request.full_url.endswith('/api/token'):return io.BytesIO(b'test-token')
  return io.BytesIO(json.dumps({'accountId':'079819354890','instanceId':'fixture-instance'}).encode())
@unittest.skipUnless(pathlib.Path('/mnt/spann').is_dir(),'requires isolated Linux /mnt/spann namespace')
class Tests(unittest.TestCase):
 def fixture(self,root):
  source=root/'source';source.mkdir();(source/'owner.stdout').write_bytes(b'warning\n'*31);(source/'owner.stderr').write_bytes(b'');return {'source':str(source),'output':str(root/'output'),'expected_instance_id':'fixture-instance','bucket':'fixture','source_cap_bytes':1024**2}
 def test_full_member_roundtrip_and_signal_restore(self):
  with tempfile.TemporaryDirectory(prefix='failure-log-test-',dir='/mnt/spann') as tmp:
   config=self.fixture(pathlib.Path(tmp));previous=signal.getsignal(signal.SIGTERM)
   with patch.object(module,'ArtifactStore',Store),patch.object(module.boto3,'client',return_value=None),patch.object(module.urllib.request,'build_opener',return_value=Opener()) as opener:
    proof=module.run(config)
   self.assertEqual(proof['status'],'passed');self.assertEqual(proof['files'],2);self.assertEqual(proof['source_bytes'],248);self.assertEqual(opener.call_args.args[0].proxies,{});self.assertEqual(signal.getsignal(signal.SIGTERM),previous)
 def test_upload_failure_is_preserved(self):
  class Broken(Store):
   def upload(self,path):raise RuntimeError('injected upload failure')
  with tempfile.TemporaryDirectory(prefix='failure-log-test-',dir='/mnt/spann') as tmp:
   config=self.fixture(pathlib.Path(tmp));previous=signal.getsignal(signal.SIGTERM)
   with patch.object(module,'ArtifactStore',Broken),patch.object(module.boto3,'client',return_value=None),patch.object(module.urllib.request,'build_opener',return_value=Opener()):
    with self.assertRaisesRegex(RuntimeError,'injected'):module.run(config)
   report=json.loads((pathlib.Path(config['output'])/'report.json').read_text());self.assertEqual(report['status'],'failed');self.assertEqual(report['phase'],'upload');self.assertTrue((pathlib.Path(config['source'])/'owner.stdout').exists());self.assertEqual(signal.getsignal(signal.SIGTERM),previous)
 def test_symlink_source_is_rejected(self):
  with tempfile.TemporaryDirectory(prefix='failure-log-test-',dir='/mnt/spann') as tmp:
   root=pathlib.Path(tmp);(root/'file').write_bytes(b'x');(root/'link').symlink_to(root/'file')
   with self.assertRaisesRegex(ValueError,'symlink'):module.inventory(root)
if __name__=='__main__':unittest.main()
