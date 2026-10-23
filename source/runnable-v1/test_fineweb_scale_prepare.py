import copy,json,tempfile,unittest
from pathlib import Path
import numpy as np
from .fineweb_stream import prepare
from .fineweb_scale_prepare import audit_membership
from .test_fineweb_stream import fixture
from .tree_manifest import artifact,checked_artifact
class FullMembershipTests(unittest.TestCase):
 def test_complete_membership_and_original_value_audit_catches_rehashed_corruption(self):
  _,manifest,opener=fixture()
  with tempfile.TemporaryDirectory() as d:
   root=Path(d)/'prepared';p=prepare(manifest,root,21,3,'epoch','source',chunk_rows=5,batch_rows=4,sample_size=12,open_source=opener);model=json.loads((root/'model.json').read_text())
   audit_membership(p['source']['chunks'],p['subtrees'],root,model,21)
   owner=next(x for x in p['subtrees'] if x['vector_count']);path=checked_artifact(owner['vectors'],root);old=path.read_bytes();x=np.frombuffer(old,dtype='<f4').copy();x[0]+=1;path.write_bytes(x.tobytes());owner['vectors']=artifact(path,root)
   with self.assertRaises(AssertionError):audit_membership(p['source']['chunks'],p['subtrees'],root,model,21)
   path.write_bytes(old);owner['vectors']=artifact(path,root)
   missing=copy.deepcopy(p['subtrees']);missing.remove(next(x for x in missing if x['vector_count']))
   with self.assertRaises(AssertionError):audit_membership(p['source']['chunks'],missing,root,model,21)
if __name__=='__main__':unittest.main()
