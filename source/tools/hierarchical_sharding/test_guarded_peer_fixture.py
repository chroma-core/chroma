"""Keep intentional failure fixtures separate from benchmark publication."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from .guarded_peer_fixture import run,stage


class FixtureTests(unittest.TestCase):
    def test_duplicate_ssh_host_rejects_before_remote_work(self):
        with patch('tools.hierarchical_sharding.guarded_peer_fixture.remote') as remote:
            with self.assertRaises(ValueError):run(['same','same'],'unused')
            remote.assert_not_called()

    def test_aliases_of_one_physical_host_never_launch(self):
        def remote(host,function,args):
            self.assertEqual(function,'stage');return dict(actual_host='same-physical-host',host=host)
        with tempfile.TemporaryDirectory() as directory,patch('tools.hierarchical_sharding.guarded_peer_fixture.remote',remote),patch('tools.hierarchical_sharding.guarded_peer_fixture.run_batch') as batch:
            with self.assertRaisesRegex(ValueError,'same physical'):run(['alias-a','alias-b'],Path(directory)/'fixture')
            batch.assert_not_called();self.assertEqual(json.loads((Path(directory)/'fixture/fixture.json').read_text())['status'],'failed')

    def test_staging_records_local_commands_and_fresh_owned_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            descriptor=stage(directory,1,'0','fresh-run');config=json.loads(Path(descriptor['config_path']).read_text())
            self.assertEqual(config['run_id'],'fresh-run');self.assertEqual(config['guard']['concurrency'],1)
            self.assertNotIn('ssh',config['guard']['commands'][0]);self.assertEqual(config['guard']['abort_plan'],'verified-process-groups-no-detach')
            with self.assertRaises(FileExistsError):stage(directory,1,'0','fresh-run')

if __name__=='__main__':unittest.main()
