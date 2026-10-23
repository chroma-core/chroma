import unittest
from .publication_gpu_job import admit_work
class BudgetTest(unittest.TestCase):
 def test_budget(self):
  self.assertEqual(admit_work(.1,100,400)['admitted_projection_s'],15)
  with self.assertRaises(TimeoutError):admit_work(1,100,400)
  for args in [(float('nan'),1,1000),(.1,True,1000),(-1,1,1000)]:
   with self.assertRaises(ValueError):admit_work(*args)
 def test_legacy_nonfinite_rejected(self):
  import numpy as np
  from .publication_gpu_job import legacy_probe
  a=np.zeros((16,768),np.float32);a[:,0]=1
  self.assertEqual(legacy_probe(a,a),0)
  for bad in [np.full((16,768),np.nan,np.float32),np.full((16,768),np.inf,np.float32),a[:1]]:
   with self.assertRaises(ValueError):legacy_probe(a,bad)
if __name__=='__main__':unittest.main()
