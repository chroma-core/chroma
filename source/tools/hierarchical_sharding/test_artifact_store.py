import base64,hashlib,io,pathlib,tempfile,unittest,zlib,struct
from .artifact_store import ArtifactStore
class STS:
 def get_caller_identity(self):return dict(Account='079819354890',Arn='arn:aws:sts::079819354890:assumed-role/test-role/session')
class S3:
 def __init__(self):self.objects={};self.parts={};self.aborted=False
 def check(self,k):assert k['Bucket']=='test-bucket' and k['ExpectedBucketOwner']=='079819354890'
 def get_bucket_location(self,**k):self.check(k);return {}
 def put_object(self,**k):
  self.check(k);b=k['Body'].read();self.objects[k['Key']]=dict(body=b,Metadata=k['Metadata'],ContentLength=len(b),ServerSideEncryption='AES256',ChecksumSHA256=k['ChecksumSHA256']);return dict(VersionId='v1')
 def create_multipart_upload(self,**k):self.check(k);self.create=k;return dict(UploadId='upload')
 def upload_part(self,**k):
  self.check(k);self.parts[k['PartNumber']]=k['Body'];return dict(ETag='etag',ChecksumCRC32=k['ChecksumCRC32'])
 def complete_multipart_upload(self,**k):
  self.check(k);b=b''.join(self.parts[i] for i in sorted(self.parts));assert k['ChecksumCRC32']==base64.b64encode(struct.pack('>I',zlib.crc32(b)&0xffffffff)).decode();self.objects[k['Key']]=dict(body=b,Metadata=self.create['Metadata'],ContentLength=len(b),ServerSideEncryption='AES256',ChecksumCRC32=k['ChecksumCRC32']);return dict(VersionId='v1')
 def abort_multipart_upload(self,**k):self.check(k);self.aborted=True
 def head_object(self,**k):self.check(k);assert k['VersionId']=='v1';return self.objects[k['Key']]
 def get_object(self,**k):self.check(k);assert k['VersionId']=='v1';r=self.objects[k['Key']].copy();r['Body']=io.BytesIO(r.pop('body'));return r
class ArtifactTests(unittest.TestCase):
 def test_single_and_multipart_restore(self):
  with tempfile.TemporaryDirectory() as d:
   p=pathlib.Path(d);s=S3();store=ArtifactStore('test-bucket','079819354890',s,STS(),'test-role')
   for n in (123,6*1024*1024):
    f=p/str(n);f.write_bytes(b'a'*n);desc=store.upload(f,part_bytes=5*1024*1024);out=p/(str(n)+'.restored');store.restore(desc,out);self.assertEqual(f.read_bytes(),out.read_bytes());self.assertEqual(desc['sha256'],hashlib.sha256(f.read_bytes()).hexdigest())
 def test_wrong_account_and_corrupt_restore_never_publish(self):
  with self.assertRaises(ValueError):ArtifactStore('test-bucket','wrong',S3(),STS(),'test-role')
  with tempfile.TemporaryDirectory() as d:
   p=pathlib.Path(d);s=S3();store=ArtifactStore('test-bucket','079819354890',s,STS(),'test-role');f=p/'input';f.write_bytes(b'abc');desc=store.upload(f);s.objects[desc['key']]['body']=b'bad'
   with self.assertRaises(ValueError):store.restore(desc,p/'output')
   self.assertFalse((p/'output').exists())
if __name__=='__main__':unittest.main()
