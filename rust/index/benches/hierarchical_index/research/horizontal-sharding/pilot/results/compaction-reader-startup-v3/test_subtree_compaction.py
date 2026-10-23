"""Exercise terminal append failures and immutable reader observations."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import threading
import re
import time
from unittest.mock import patch
from .subtree_compaction import preflight_owner, monitored_publication, journal, validate_preparation, assert_mutation_overlap, RemoteBackend


class CompactionTests(unittest.TestCase):
    def fixture(self, directory):
        root=Path(directory); old_map=root/'id-map-1.u64le'; old_map.write_bytes((0).to_bytes(8,'little'))
        digest=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
        manifest=dict(vector_count=1,routing_boundary_hash='boundary',id_mapping=dict(path=old_map.name,count=1,sha256=digest(old_map)))
        payload=json.dumps(manifest).encode(); (root/'manifest.json').write_bytes(payload); (root/'manifest-1.json').write_bytes(payload)
        target=root/'target.u64le'; target.write_bytes(old_map.read_bytes()+(1).to_bytes(8,'little'))
        vectors=root/'delta.f32'; vectors.write_bytes(b'\0'*4)
        entry=dict(index=str(root),id_map=str(target),vectors=str(vectors))
        old=dict(generation=1,snapshot=dict(sha256=digest(root/'manifest.json')),local_to_global=dict(sha256=digest(old_map)))
        slot=dict(subtree_id='a',local_start=1,vector_count=2,append_row_count=1,routing_boundary_hash='boundary',local_to_global=dict(sha256=digest(target)),vectors=dict(sha256=digest(vectors),bytes=4))
        return entry,slot,old

    def test_exact_parent_and_prefix_accept_but_orphan_generation_rejects(self):
        with tempfile.TemporaryDirectory() as directory:
            entry,slot,old=self.fixture(directory)
            preflight_owner(entry,slot,old,2)
            for name in ('id-map-2.u64le','manifest-2.json','build.lock','manifest.json.tmp'):
                path=Path(directory)/name; path.write_text('partial')
                with self.assertRaises(ValueError):preflight_owner(entry,slot,old,2)
                self.assertEqual(path.read_text(),'partial'); path.unlink()
            Path(directory,'manifest.json').write_text('another parent')
            with self.assertRaisesRegex(ValueError,'parent'):preflight_owner(entry,slot,old,2)

    def test_changed_old_map_prefix_rejects_even_with_matching_target_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            entry,slot,old=self.fixture(directory)
            path=Path(entry['id_map']);path.write_bytes((9).to_bytes(8,'little')+(1).to_bytes(8,'little'))
            slot['local_to_global']['sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError,'prefix'):preflight_owner(entry,slot,old,2)

    def test_failed_publication_leaves_monitor_and_failure_journal_reviewable(self):
        class Coordinator:
            def query(self,**kwargs):return dict(tree_sha256='old',generation=1,ids=[0],distances=[.25],levels=[],selected_candidates=[])
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'journal.jsonl'; state=dict(head='old')
            def publication(_):raise ValueError('CAS conflict')
            with self.assertRaisesRegex(ValueError,'CAS'):
                monitored_publication(Coordinator(),[dict(query=[1])],lambda:dict(done=True),publication,path)
            journal(path,'failed',retry_policy='fresh clone')
            rows=[json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(state['head'],'old');self.assertTrue(any(r['phase']=='old-reader' for r in rows));self.assertEqual(rows[-1]['phase'],'failed')

    def test_distance_change_after_cas_fails_even_if_ids_unchanged(self):
        class Coordinator:
            distance=.25
            def query(self,**kwargs):return dict(tree_sha256='old',generation=1,ids=[0],distances=[self.distance])
        coordinator=Coordinator()
        with tempfile.TemporaryDirectory() as directory:
            def publication(_):coordinator.distance=.5;return dict(published=True)
            with self.assertRaisesRegex(AssertionError,'trace changed'):
                monitored_publication(coordinator,[dict(query=[1])],lambda:None,publication,Path(directory)/'journal.jsonl')

    def test_preparation_rejects_duplicate_ids_and_wrong_target_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); model=root/'model.json';model.write_text('{}')
            mapping=root/'ids';mapping.write_bytes((0).to_bytes(8,'little')+(1).to_bytes(8,'little'))
            vectors=root/'vectors';vectors.write_bytes(b'\0'*4)
            def artifact(path):return dict(path=str(path),bytes=path.stat().st_size,sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            old=dict(construction_epoch='e',dimension=1,metric='ip',upper_root={},source=dict(source_id='s',row_count=1),model=artifact(model),subtrees=[dict(subtree_id='a',vector_count=1,centroid_index=0,routing_boundary_hash='b')])
            prepared=dict(construction_epoch='e',dimension=1,metric='ip',upper_root={},source_id='s',start_id=1,input_row_count=1,global_row_count=2,model=artifact(model),subtrees=[dict(subtree_id='a',local_start=1,append_row_count=1,vector_count=2,centroid_index=0,routing_boundary_hash='b',local_to_global=artifact(mapping),vectors=artifact(vectors))])
            validate_preparation(old,prepared,root)
            mapping.write_bytes((0).to_bytes(8,'little')*2);prepared['subtrees'][0]['local_to_global']=artifact(mapping)
            with self.assertRaises(ValueError):validate_preparation(old,prepared,root)
            prepared['subtrees'][0]['vector_count']=3
            with self.assertRaisesRegex(ValueError,'count'):validate_preparation(old,prepared,root)

    def test_persistent_reader_ssh_channels_do_not_multiplex(self):
        captured=[]
        class FakeReader:
            def __init__(self,command,timeout):captured.append(command)
            def query(self,payload):return dict(request_id=payload['request_id'],generation=1,shard_id='a',root=dict(node_id=1,is_leaf=True),vector_count=1,id_mapping=dict(sha256='map'))
            def close(self):pass
        tree=dict(subtrees=[dict(subtree_id='a',generation=1,root_ref=dict(id=1,kind='leaf'),vector_count=1,local_to_global=dict(sha256='map'))])
        config=dict(cpu_affinity='0',binary='/writer',entries=dict(a=dict(host='worker',index='/clone')))
        with patch('tools.hierarchical_sharding.subtree_compaction.Reader',FakeReader):
            backend=RemoteBackend(tree,config);backend.close()
        self.assertIn('ControlMaster=no',captured[0]);self.assertIn('ControlPath=none',captured[0])

    def test_startup_slots_cover_handshake_and_all_readers_close_after_failure(self):
        lock=threading.Lock();active=[0];peak=[0];created=[]
        class FakeReader:
            def __init__(self,command,timeout):
                self.closed=False;self.sid=re.search(r'/clone/(\d+)',command[-1]).group(1);created.append(self)
            def query(self,payload):
                with lock:active[0]+=1;peak[0]=max(peak[0],active[0])
                time.sleep(.03 if self.sid=='0' else .01)
                with lock:active[0]-=1
                if self.sid=='0':raise ValueError('simulated SSH authentication failure')
                return dict(request_id=payload['request_id'],generation=1,shard_id=self.sid,root=dict(node_id=1,is_leaf=True),vector_count=1,id_mapping=dict(sha256='map'))
            def close(self):self.closed=True
        tree=dict(subtrees=[dict(subtree_id=str(i),generation=1,root_ref=dict(id=1,kind='leaf'),vector_count=1,local_to_global=dict(sha256='map')) for i in range(128)])
        config=dict(cpu_affinity='0',binary='/writer',entries={str(i):dict(host='worker',index='/clone/'+str(i)) for i in range(128)})
        with patch('tools.hierarchical_sharding.subtree_compaction.Reader',FakeReader):
            with self.assertRaisesRegex(ValueError,'authentication'):RemoteBackend(tree,config)
        self.assertLessEqual(peak[0],8);self.assertTrue(all(r.closed for r in created))

    def test_reader_observations_must_cover_actual_writer_interval(self):
        with self.assertRaisesRegex(AssertionError,'during writer mutation'):
            assert_mutation_overlap([(1,2),(8,9)],3,7)
        assert_mutation_overlap([(2,4),(5,8)],3,7)

    def test_success_checks_old_reader_after_publication(self):
        class Coordinator:
            def query(self,**kwargs):return dict(tree_sha256='old',generation=1,ids=[0],distances=[.25])
        with tempfile.TemporaryDirectory() as directory:
            _,pointer,count=monitored_publication(Coordinator(),[dict(query=[1])],lambda:42,lambda result:dict(value=result),Path(directory)/'journal.jsonl')
            self.assertEqual(pointer,dict(value=42));self.assertGreaterEqual(count,2)

if __name__=='__main__':unittest.main()
