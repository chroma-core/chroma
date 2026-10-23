"""Deterministic fsync order and namespace-failure admission checks."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from .dynamic_runtime import durable_directory,copy_checkpoint

class DurabilityTests(unittest.TestCase):
    def test_files_and_child_directory_entries_are_synced_before_parents(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'worker'/'snapshot';leaf=root/'data'/'objects';leaf.mkdir(parents=True);(leaf/'block').write_bytes(b'block')
            paths={};synced=[];real_open=os.open;real_fsync=os.fsync
            def opened(path,*args,**kwargs):
                fd=real_open(path,*args,**kwargs);paths[fd]=str(path);return fd
            def synced_fd(fd):
                synced.append(paths.get(fd,'file'));real_fsync(fd)
            with patch('os.open',side_effect=opened),patch('os.fsync',side_effect=synced_fd):durable_directory(root)
            self.assertEqual(synced[0],'file');self.assertLess(synced.index(str(leaf)),synced.index(str(root)));self.assertLess(synced.index(str(root)),synced.index(str(root.parent)))
    def test_directory_sync_failure_prevents_transfer_readiness(self):
        with tempfile.TemporaryDirectory() as temp:
            source=Path(temp)/'source';source.mkdir();(source/'block').write_bytes(b'block');destination=Path(temp)/'new'/'snapshot'
            with patch('tools.hierarchical_sharding.dynamic_runtime.durable_directory',side_effect=OSError('injected directory fsync failure')):
                with self.assertRaisesRegex(OSError,'fsync'):copy_checkpoint(source,destination,100)
            self.assertTrue(destination.exists());self.assertFalse((destination.parent/'snapshot.transfer.json').exists())

    def test_reconciliation_requires_successful_pointer_namespace_sync(self):
        from .dynamic_deployment import deployment_contains
        with tempfile.TemporaryDirectory() as temp:
            pointer=Path(temp)/'head.json'
            with patch('tools.hierarchical_sharding.dynamic_runtime.durable_namespace',side_effect=OSError('pointer namespace sync failed')),patch('tools.hierarchical_sharding.dynamic_deployment.load_deployment') as loaded:
                with self.assertRaisesRegex(OSError,'namespace sync'):deployment_contains(pointer,'0'*64)
                loaded.assert_not_called()

    def test_visible_replacement_with_failed_sync_stays_unknown_in_journal(self):
        import json
        from .dynamic_deployment import run_transaction
        from .tree_manifest import content_hash
        parent=dict(fence=1);candidate=dict(parent_sha256=content_hash(parent),fence=2)
        with tempfile.TemporaryDirectory() as temp:
            journal=Path(temp)/'journal.json';pointer=Path(temp)/'head.json'
            def failed_publication(*args,**kwargs):
                pointer.write_text('visible replacement')
                raise OSError('injected post-replace directory fsync failure')
            with patch('tools.hierarchical_sharding.dynamic_deployment.publish_deployment',side_effect=failed_publication),patch('tools.hierarchical_sharding.dynamic_runtime.durable_namespace',side_effect=[None,OSError('reconciliation sync also fails')]):
                with self.assertRaisesRegex(OSError,'post-replace'):run_transaction(pointer,parent,lambda _:candidate,lambda *args:None,journal)
            self.assertEqual(json.loads(journal.read_text())['status'],'publication_state_unknown')

    def test_injected_backend_must_bind_the_complete_deployment(self):
        from .dynamic_runtime import DeploymentReader
        from unittest.mock import Mock
        parent=dict(tree_sha256='same-tree',owners={'s0':{'worker_id':'old'}})
        backend=Mock();backend.deployment_sha256='obsolete-deployment'
        with patch('tools.hierarchical_sharding.dynamic_deployment.load_deployment',return_value=parent),patch('tools.hierarchical_sharding.dynamic_deployment.validate_record'):
            with self.assertRaisesRegex(ValueError,'complete deployment'):DeploymentReader('/unused','/unused',backend=backend)
        backend.close.assert_not_called()

    def test_observed_peer_failure_stops_before_validation_and_publication(self):
        from .dynamic_deployment import publish_deployment
        with tempfile.TemporaryDirectory() as temp:
            pointer=Path(temp)/'head.json';pointer.write_bytes(b'old durable pointer')
            def failed_health():raise ValueError('observed peer failure')
            with patch('tools.hierarchical_sharding.dynamic_deployment.validate_record') as validated:
                with self.assertRaisesRegex(ValueError,'peer failure'):publish_deployment(pointer,{},None,lambda *args:None,health=failed_health)
                validated.assert_not_called()
            self.assertEqual(pointer.read_bytes(),b'old durable pointer')
