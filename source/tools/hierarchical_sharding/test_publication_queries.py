import json,tempfile,unittest
from pathlib import Path
import numpy as np
from .publication_queries import produce
from .fineweb_queries import MODEL,MODEL_REVISION,CODE_REVISION

class QueriesTest(unittest.TestCase):
 def test_resume_and_pin(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);texts=root/'texts.jsonl';texts.write_text(''.join(json.dumps(dict(query_id=str(i),query=str(i)))+'\n' for i in range(5)))
   runtime=dict(model=MODEL,model_revision=MODEL_REVISION,remote_code_revision=CODE_REVISION,dtype='float32',query_prefix=None,extra_normalization=False,runtime_lock_sha256='a'*64,model_files=[dict(sha256='b'*64)],remote_code_files=[dict(sha256='c'*64)])
   calls=[]
   def encode(batch):
    calls.append(batch)
    if len(calls)==2:raise RuntimeError('injected failure')
    a=np.zeros((len(batch),768),np.float32);a[:,0]=1;return a
   with self.assertRaises(RuntimeError):produce(texts,root/'out',encode,runtime,2)
   result=produce(texts,root/'out',encode,runtime,2)
   self.assertEqual(result['resumed_rows'],2);self.assertEqual(result['query_count'],5)
   self.assertEqual(len(list((root/'out').glob('failure-*.json'))),1)
   with self.assertRaises(ValueError):produce(texts,root/'out',encode,runtime,1)
   p=root/'out/chunk-00000.f32';p.write_bytes(b'corrupt')
   with self.assertRaises(ValueError):produce(texts,root/'out',encode,runtime,2)

 def test_invalid_dtype_and_norm(self):
  from .publication_queries import validate
  with self.assertRaises(ValueError):validate(np.zeros((1,768),np.float32))
  with self.assertRaises(ValueError):validate(np.full((1,768),np.nan,np.float32))

class EncoderTest(unittest.TestCase):
 def test_length_gate_and_semantics(self):
  from .publication_queries import PinnedEncoder
  class Model:
   def tokenizer(self,texts,**kwargs):
    self.token_args=kwargs
    return {'input_ids':[[0]*len(t) for t in texts]}
   def encode(self,texts,**kwargs):
    self.args=kwargs
    a=np.zeros((len(texts),768),np.float32);a[:,0]=1;return a
  model=Model();encoder=PinnedEncoder(model)
  self.assertEqual(encoder(['hello']).shape,(1,768))
  self.assertFalse(model.token_args['truncation']);self.assertFalse(model.args['normalize_embeddings'])
  with self.assertRaises(ValueError):encoder(['a'*257])
 def test_metadata_pin_and_files(self):
  from .publication_queries import selected_files
  metadata=dict(sha=MODEL_REVISION,siblings=[dict(rfilename='model.safetensors',size=12),dict(rfilename='onnx/model.json',size=2)])
  self.assertEqual(len(selected_files(metadata)),1)
  metadata['siblings'][0]['rfilename']='/absolute/model.safetensors'
  with self.assertRaises(ValueError):selected_files(metadata)
  metadata['sha']='wrong'
  with self.assertRaises(ValueError):selected_files(metadata)

class ProbeTest(unittest.TestCase):
 def test_probe_failure(self):
  from .publication_queries import compare_producers
  def same(texts):
   a=np.zeros((len(texts),768),np.float32);a[:,0]=1;return a
  self.assertEqual(compare_producers(['a'],same,same)['status'],'passed')
  def wrong(texts):
   a=same(texts);a[:,0]=0;a[:,1]=1;return a
  with self.assertRaises(ValueError):compare_producers(['a'],same,wrong)

if __name__=='__main__':unittest.main()
