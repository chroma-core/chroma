import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import run_reference as m
class ReferenceTests(unittest.TestCase):
 def test_source_null_blocks(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d)/'contract';p.write_text('{}')
   with patch.object(m,'CONTRACT',p):
    with self.assertRaisesRegex(ValueError,'unfilled'):m.readiness()
 def test_command_preserves_reference_and_validation(self):
  command=m.native_command(Path('/binary'),Path('/fresh'))
  for flag in ['--validate-postings','--verify-valid-postings','--save','--fp-npa']:self.assertIn(flag,command)
  self.assertNotIn('--no-commit',command);self.assertNotIn('--checkpoint-snapshots-dir',command)
  for key,value in [('--threads','32'),('--balance-threads','32'),('--checkpoint-size','100000'),('--num-queries','0'),('--gc-blockfiles','false'),('--write-rng-factor','1'),('--write-beam-tau','1.5')]:self.assertEqual(command[command.index(key)+1],value)
 def valid_text(self):
  return '\n'.join(['RETAINED_POSTING_VALIDATION '+json.dumps({'phase':phase,'unique_current_ids':100000,'current_postings':100000,'duplicate_current_postings':0,'seconds':1.0})for phase in ['before commit','after reopen']]+['[posting validation] before commit: 100000 root-reachable valid ids; all child links resolve','[posting validation] after reopen: 100000 root-reachable valid ids; all child links resolve','Pre-commit reachable valid IDs: 100000 | indexed embeddings without one: 0 | missing child nodes: 0','Post-reopen reachable valid IDs: 100000 | missing child nodes: 0','RETAINED_SAVED_BITS '+json.dumps({'rows':100000,'all_current_vector_bits_sha256':m.SAVED_DIGEST,'seconds':1.0})])
 def test_membership_required_beyond_embedding_digest(self):
  text=self.valid_text();self.assertEqual(m.validate_summary(text)['saved_bits']['rows'],100000)
  for changed in [text.replace('[posting validation] after reopen','missing-after-reopen'),text.replace('indexed embeddings without one: 0','indexed embeddings without one: 1'),text.replace('\"duplicate_current_postings\": 0','\"duplicate_current_postings\": 1'),text+'\n'+text.splitlines()[0]]:
   with self.assertRaises(ValueError):m.validate_summary(changed)
 def test_saved_digest_exact_and_one_checkpoint(self):
  text=self.valid_text()
  for changed in [text.replace(m.SAVED_DIGEST,'0'*64),text+'\n'+text.splitlines()[-1]]:
   with self.assertRaises(ValueError):m.validate_summary(changed)
 def test_inventory_rejects_unlisted(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d);(root/'source').mkdir();(root/'source/extra').write_text('unlisted')
   manifest=root/'source-manifest.json';manifest.write_text(json.dumps({'files':{},'symlinks':{}}))
   with patch.object(m,'readiness',return_value={'source_manifest_sha256':m.digest(manifest)}):
    with self.assertRaisesRegex(ValueError,'inventory'):m.verify_source(root, m.time.time()+400)
 def test_path_isolation(self):
  m.isolate_paths(Path('/control'),Path('/target'),Path('/output'))
  with self.assertRaises(ValueError):m.isolate_paths(Path('/control'),Path('/control/target'),Path('/output'))
if __name__=='__main__':unittest.main()
