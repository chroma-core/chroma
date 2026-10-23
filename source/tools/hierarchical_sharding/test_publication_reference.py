import tempfile,unittest
from pathlib import Path
import numpy as np
from .publication_reference import independent_oracle,compare,integration_gate,certified_scan

class ReferenceTest(unittest.TestCase):
 def test_full_oracle_ties_and_chunks(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);x=np.array([[1,0],[1,0],[0,1],[-1,0],[1,0]],np.float32)
   chunks=[]
   for start,end in [(0,2),(2,5)]:
    p=root/str(start);x[start:end].tofile(p);chunks.append((p,start,end-start))
   queries=[dict(query_id='q',vector=[1,0])]
   rows=independent_oracle(chunks,queries,2,4,block_rows=1)
   self.assertEqual(rows[0]['ids'],[0,1,4,2]);compare(rows,rows,4)
   with self.assertRaises(ValueError):compare(rows,[dict(query_id='q',ids=[1,0,4,2],distances=[0,0,0,1])],4)
 def test_fail_closed_report(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);p=root/'x';np.ones((3,2),np.float32).tofile(p)
   with self.assertRaises(ValueError):certified_scan([(p,0,3)],[dict(query_id='x',vector=[1,0])],2,1,None,root/'out',[],[])
   self.assertTrue((root/'out/report.json').exists());self.assertFalse((root/'out/complete.json').exists())
 def test_broadcast_rejected(self):
  row=dict(query_id='q',ids=list(range(100)),distances=[0])
  expected=dict(query_id='q',ids=list(range(100)),distances=[0]*100)
  with self.assertRaises(ValueError):compare([row],[expected],100)
 def test_missing_integration(self):
  with self.assertRaises(ValueError):integration_gate([],[],{},[])

if __name__=='__main__':unittest.main()
