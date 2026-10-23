import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from .subtree_orchestrate import validate_binding,publish_checked,checkpoint_inventory,transfer_checkpoint

class PublicationGateTest(unittest.TestCase):
    def fixture(self):
        manifest=dict(generation=2,dimension=4,metric='ip',source_id='source',root_ref=dict(kind='leaf',id=7),block_ids={'leaf':'block'},vector_count=4,id_mapping={'sha256':'map'},integrity_audit=dict(generation=2,block_ids={'leaf':'block'},id_map_sha256='map',coverage=dict(expected_rows=4,unique_live_rows=4)))
        tree=dict(dimension=4,metric='ip',source=dict(source_id='source'),subtrees=[dict(subtree_id='0',generation=2,vector_count=4,root_ref=dict(kind='leaf',id=7),snapshot={'sha256':'snapshot'},local_to_global={'sha256':'map'})])
        rows=[dict(subtree_id='0',generation=2,snapshot_sha256='snapshot',mapping_sha256='map',manifest=manifest,root_response=dict(generation=2,shard_id='0',root=dict(node_id=7,is_leaf=True),id_mapping=dict(sha256='map')))]
        return tree,rows

    def test_rejects_stale_generation_blocks_map_and_incomplete_coverage(self):
        _,rows=self.fixture();manifest=rows[0]['manifest']
        self.assertEqual(validate_binding(manifest,'map')['generation'],2)
        for field,value in [('generation',1),('block_ids',{'leaf':'stale'}),('id_map_sha256','stale'),('coverage',dict(expected_rows=4,unique_live_rows=3))]:
            bad=copy.deepcopy(manifest);bad['integrity_audit'][field]=value
            with self.assertRaises(ValueError):validate_binding(bad,'map')

    def test_failed_remote_verification_preserves_old_global_pointer(self):
        tree,rows=self.fixture()
        with tempfile.TemporaryDirectory() as folder:
            pointer=Path(folder)/'current.json';pointer.write_text('old tree')
            for field,value in [('snapshot_sha256','stale'),('generation',1),('mapping_sha256','stale')]:
                bad=copy.deepcopy(rows);bad[0][field]=value
                with patch('tools.hierarchical_sharding.subtree_orchestrate.publish') as publisher:
                    with self.assertRaises(ValueError):publish_checked(pointer,tree,bad,folder,expected_old_hash='old')
                    publisher.assert_not_called()
                self.assertEqual(pointer.read_text(),'old tree')

    def test_missing_or_duplicate_owner_verification_blocks_publication(self):
        tree,rows=self.fixture()
        for bad in ([],rows+rows):
            with patch('tools.hierarchical_sharding.subtree_orchestrate.publish') as publisher:
                with self.assertRaises(ValueError):publish_checked('pointer',tree,bad,'.')
                publisher.assert_not_called()

    def test_verified_remote_proof_keeps_atomic_compare_and_swap(self):
        tree,rows=self.fixture()
        with patch('tools.hierarchical_sharding.subtree_orchestrate.publish',return_value={'published':True}) as publisher:
            self.assertTrue(publish_checked('pointer',tree,rows,'.',expected_old_hash='old')['published'])
            publisher.assert_called_once_with('pointer',tree,expected_old_hash='old',base='.')

class TransferGateTest(unittest.TestCase):
    def test_inventory_pins_snapshot_and_every_transferred_file(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);manifest=root/'manifest-2.json';manifest.write_bytes(b'committed');(root/'block').write_bytes(b'block contents')
            expected=hashlib.sha256(b'committed').hexdigest()
            rows=checkpoint_inventory(root,2,expected)
            self.assertEqual(sum(row['bytes'] for row in rows),len(b'committedblock contents'))
            with self.assertRaises(ValueError):checkpoint_inventory(root,2,'another snapshot')
            (root/'alias').symlink_to(root/'block')
            with self.assertRaises(ValueError):checkpoint_inventory(root,2,expected)

    def test_existing_destination_is_not_overwritten(self):
        inventory=[dict(path='manifest-1.json',bytes=1,sha256='x')]
        with patch('tools.hierarchical_sharding.subtree_orchestrate.ssh',side_effect=[json.dumps(inventory),ValueError('destination exists')]),patch('tools.hierarchical_sharding.subtree_orchestrate.subprocess.run') as copy:
            with self.assertRaisesRegex(ValueError,'destination exists'):
                transfer_checkpoint('worker0','/source','worker1','/destination',1,'snapshot')
            copy.assert_not_called()

if __name__=='__main__':unittest.main()

class ServingPolicyTest(unittest.TestCase):
    def test_four_adapters_partition_owners_and_physical_cpu_pairs(self):
        from unittest.mock import patch
        from .subtree_orchestrate import serve_case
        class Coordinator:
            def verify_roots(self,**kwargs):return {'status':'ok'}
            def close(self):pass
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);tree=root/'tree.json';slots=[dict(subtree_id=str(i),vector_count=100-i,generation=1,snapshot=dict(sha256='a'*64)) for i in range(8)];tree.write_text(json.dumps(dict(subtrees=slots)))
            (root/'case.json').write_text(json.dumps(dict(tree=str(tree),tree_sha256='b'*64,host_order=['ubuntu@host'],binary='/binary',entries={str(i):dict(host='ubuntu@host',index='/index/'+str(i)) for i in range(8)})))
            with patch('tools.hierarchical_sharding.subtree_orchestrate.ssh',return_value='123'),patch('tools.hierarchical_sharding.subtree_transport.TreeCoordinator.from_files',return_value=Coordinator()):
                path=serve_case(root,9000,adapters_per_host=4,read_binary='/v4',profile_memory=True)
            servers=json.loads((root/'servers-two-hosts.json').read_text());self.assertEqual(len(servers),4)
            masks=[set(map(int,s['cpu_affinity'].split(','))) for s in servers];self.assertEqual(len(set.union(*masks)),16)
            self.assertEqual({s['binary'] for s in servers},{'/v4'});placement=json.loads(path.read_text());self.assertEqual(len({o['endpoint'] for o in placement['owners'].values()}),4)
