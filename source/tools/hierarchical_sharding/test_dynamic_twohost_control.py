"""Control admission and failure paths stay inside the sandbox fixture scope."""
import copy
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
from .dynamic_twohost_control import admission,control,stage_from_driver
from .dynamic_ssh import authenticated_key_map
from .tree_manifest import file_hash


class ControlTests(unittest.TestCase):
    def test_control_never_serializes_private_agent_token(self):
        entry=dict(source_root='/fresh/source',token='secret',run_id='run')
        with patch('tools.hierarchical_sharding.dynamic_twohost_control.remote_python',return_value={}) as call:
            control('poll',entry,5)
        self.assertNotIn('secret',call.call_args.args[1]);self.assertNotIn('token',call.call_args.args[1])
    def test_source_wire_mismatch_stops_before_ssh(self):
        with tempfile.TemporaryDirectory() as temp:
            archive=Path(temp)/'source.tar';archive.write_bytes(b'bad')
            with patch('subprocess.run') as launched:
                with self.assertRaisesRegex(ValueError,'archive differs'):stage_from_driver(dict(source_archive=str(archive),source_archive_sha256='0'*64))
                launched.assert_not_called()
    def test_original_key_receipt_mismatch_stops_before_worker_access(self):
        # Identity and key receipt checks precede any mutating operation.
        with patch('tools.hierarchical_sharding.dynamic_ssh.authenticated_key_map',return_value={}),patch('tools.hierarchical_sharding.dynamic_twohost_control.file_hash',return_value='bad'):
            with self.assertRaisesRegex(ValueError,'provenance'):admission(dict(root='/mnt/spann/dynamic-twohost-v1/fresh/driver/execution',host_key_evidence_directory='/evidence',original_host_key_receipt='/receipt'))
    def test_mutated_control_plane_receipt_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            (Path(temp)/'attestation-receipt-v1.json').write_text('{}')
            with self.assertRaisesRegex(ValueError,'receipt differs'):authenticated_key_map(temp)

    def test_early_supervisor_failure_aborts_all_peers_before_scenario(self):
        from .dynamic_twohost_control import execute
        with tempfile.TemporaryDirectory() as temp:
            config=dict(root=str(Path(temp)/'execution'),run_id='run',deadline_unix=time.time()+10,workers={worker:dict(host=worker,run_id=worker) for worker in ('worker-a','worker-b')})
            with patch('tools.hierarchical_sharding.dynamic_twohost_control.admission',side_effect=lambda value:value),patch('tools.hierarchical_sharding.dynamic_twohost_control.stage_from_driver',side_effect=lambda value:value),patch('tools.hierarchical_sharding.dynamic_twohost_control.run_batch',return_value=dict(complete=False,missing_terminal_hosts=[])),patch('tools.hierarchical_sharding.dynamic_twohost_control.control',return_value={}) as abort,patch('tools.hierarchical_sharding.dynamic_twohost_control.remote_python',return_value=None),patch('tools.hierarchical_sharding.dynamic_twohost_scenario.run') as scenario:
                with self.assertRaisesRegex(ValueError,'before agent readiness'):execute(config)
                scenario.assert_not_called()
                self.assertEqual({call.args[1]['host'] for call in abort.call_args_list},{'worker-a','worker-b'})
                self.assertTrue(all(call.args[0]=='abort' for call in abort.call_args_list))
            report=json.loads((Path(config['root'])/'controller.json').read_text())
            self.assertEqual(report['status'],'failed');self.assertFalse(report['batch']['complete'])


if __name__=='__main__':unittest.main()
