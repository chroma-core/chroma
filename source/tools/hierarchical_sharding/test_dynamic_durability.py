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
