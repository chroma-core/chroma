import json,tempfile,unittest
from pathlib import Path
from .preparation_attempt import new_attempt,save_attempt
from .tree_manifest import checked_artifact

class AttemptTests(unittest.TestCase):
    def test_retry_preserves_prior_report(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);first=new_attempt(root);save_attempt(first,dict(status='failed',error='injected'));original=(first/'run.json').read_bytes()
            second=new_attempt(root);save_attempt(second,dict(status='passed'))
            self.assertEqual((first/'run.json').read_bytes(),original)
            pointer=json.loads((root/'latest.json').read_text());self.assertEqual(pointer['attempt'],second.name)
            self.assertEqual(json.loads(checked_artifact(pointer['report'],root).read_text())['status'],'passed')
