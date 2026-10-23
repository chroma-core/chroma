import tempfile,unittest
from pathlib import Path
from .tree_manifest import artifact
from .publication_launch import KEYS,PRODUCERS,validate_job,commands
from unittest.mock import patch
class LaunchTest(unittest.TestCase):
 def test_hashes_deadline_and_commands(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);p=root/'file';p.write_bytes(b'test')
   desc=artifact(p,root);job=dict(started_epoch=100,stop_epoch=3700,inputs={key:desc for key in KEYS},producer_sources=[artifact(Path(__file__).with_name(name)) for name in sorted(PRODUCERS)],source_chunks=[desc])
   with patch('tools.hierarchical_sharding.publication_launch.snapshot_chunks',return_value=[(str(p.resolve()),desc['sha256'],desc['bytes'])]):
    paths=validate_job(job,root,now=101)
   steps=commands(paths,root/'out',3700,python='/p')
   self.assertEqual([x[0] for x in steps],['integration1m','integration10m','embedding11000','reference11000'])
   self.assertIn('--stop-epoch',steps[-1][1])
   job['stop_epoch']=3701
   with self.assertRaises(ValueError):validate_job(job,root,now=101)
   job['stop_epoch']=3700;p.write_bytes(b'changed')
   with self.assertRaises(ValueError):validate_job(job,root,now=101)
if __name__=='__main__':unittest.main()
