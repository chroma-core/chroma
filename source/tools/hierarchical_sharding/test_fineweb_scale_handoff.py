import json,tempfile,unittest
from pathlib import Path
from .fineweb_scale_handoff import handoff
class HandoffTests(unittest.TestCase):
 def test_unrestored_or_unreconciled_results_cannot_publish(self):
  for mode in ('archive','process'):
   with tempfile.TemporaryDirectory() as d:
    root=Path(d);(root/'control/guard-output').mkdir(parents=True);(root/'pipeline.json').write_text(json.dumps(dict(status='passed',archive=dict(status='failed' if mode=='archive' else 'passed'))));(root/'control/guard-output/report.json').write_text(json.dumps(dict(status='passed',outcomes=[dict(status='ok',returncode=0,unreconciled_children=[1] if mode=='process' else [])])))
    class Store:
     def upload(self,*_):raise AssertionError('upload must not run before terminal gates')
    with self.assertRaises(ValueError):handoff(root,Store())
    self.assertFalse((root/'handoff.json').exists())
if __name__=='__main__':unittest.main()
