"""Stage counters preserve canonical artifact bytes and exact UUID rejection."""
import json,tempfile,unittest,sqlite3
from pathlib import Path
from .fineweb_parallel import ingest
from .fineweb_stream import _audit_uuids
from .test_fineweb_stream import fixture
from .tree_manifest import artifact
class ProfileTests(unittest.TestCase):
 def test_profile_does_not_change_canonical_chunk_bytes(self):
  _,manifest,opener=fixture()
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);results=[ingest(manifest,root/str(i),21,workers=2,chunk_rows=5,batch_rows=4,open_source=opener) for i in range(2)]
   self.assertEqual(results[0]['chunks'],results[1]['chunks'])
   for result in results:
    profile=result['uuid_audit_profile'];self.assertEqual(profile['rows'],21);self.assertEqual(profile['status'],'passed')
    for name in ('input_hash_s','json_parse_s','sqlite_insert_s','sqlite_commit_s','database_hash_s','process_cpu_s','wall_s'):self.assertGreaterEqual(profile[name],0)
   receipt=json.loads((root/'0/files/000000/complete.json').read_text());profile=receipt['stage_profile'];self.assertGreaterEqual(profile['thread_cpu_s'],0);self.assertTrue(profile['source'])
   for name in ('uuid_serialization_s','vector_serialization_s','artifact_write_s','chunk_hash_s'):self.assertGreaterEqual(profile['emit'][name],0)
 def test_cross_chunk_duplicate_failure_keeps_profile_and_immutable_inputs(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);chunks=[]
   for i in range(2):
    path=root/(str(i)+'.jsonl');path.write_text(json.dumps(dict(global_id=i,source_uuid='same'))+'\n');chunks.append(dict(start_id=i,row_count=1,uuids=artifact(path,root)))
   before=[(root/c['uuids']['path']).read_bytes() for c in chunks];profile={}
   with self.assertRaises(sqlite3.IntegrityError):_audit_uuids(root,chunks,timings=profile)
   self.assertEqual(profile['status'],'failed');self.assertEqual(profile['rows'],2)
   self.assertFalse((root/'uuid-to-row.sqlite').exists());self.assertEqual(before,[(root/c['uuids']['path']).read_bytes() for c in chunks])
if __name__=='__main__':unittest.main()
