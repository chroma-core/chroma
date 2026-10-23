import json
from pathlib import Path
import shlex
import struct
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from .build import command, partition, run_jobs


class BuildTests(unittest.TestCase):
    def test_partition_append_preserves_modulo_ids(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / 'data'
            source.write_bytes(struct.pack('<8f', *range(8)))
            result = partition(source, root / 'shards', 2, 3, 'dataset-revision', start_id=5)
            self.assertEqual([s['global_id_start'] for s in result['shards']], [6, 7, 5])
            self.assertEqual([s['row_count'] for s in result['shards']], [1, 1, 2])
            self.assertEqual((root / 'shards/shard-2.f32').read_bytes(), struct.pack('<4f', 0, 1, 6, 7))
            self.assertEqual(json.loads((root / 'shards/input.json').read_text())['source_id'], 'dataset-revision')

    def job(self):
        return dict(binary='/tmp/worker with spaces', vectors='/tmp/$(no).f32', checkpoint_dir='/tmp/index',
                    shard_id='0', source_id='dataset', dimension=2, row_count=2, id_stride=1,
                    generation=1, global_id_start=0)

    def test_ssh_quoting_preserves_literal_paths(self):
        job = dict(self.job(), host='worker-1')
        args = command(job)
        self.assertEqual(shlex.split(args[-1]), command(self.job()))
        with self.assertRaises(ValueError):
            command(dict(job, host='-oProxyCommand=bad'))

    def test_failed_build_does_not_claim_complete(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch('tools.hierarchical_sharding.build.subprocess.run') as run:
                run.return_value.returncode = 1
                result = run_jobs([self.job()], Path(folder) / 'logs')
            self.assertFalse(result['complete'])
            self.assertEqual(result['jobs'][0]['status'], 'error')

    def test_duplicate_writer_rejected_before_launch(self):
        with self.assertRaises(ValueError):
            run_jobs([self.job(), self.job()], '/unused')

    def test_remote_timeout_blocks_remaining_host_jobs(self):
        first = dict(self.job(), host='worker-1')
        second = dict(first, shard_id='1', checkpoint_dir='/tmp/other-index')
        with tempfile.TemporaryDirectory() as folder:
            with patch('tools.hierarchical_sharding.build.subprocess.run', side_effect=subprocess.TimeoutExpired('ssh', 1)) as run:
                result = run_jobs([first, second], Path(folder) / 'logs', timeout=1)
            self.assertEqual(run.call_count, 1)
            self.assertEqual([r['status'] for r in result['jobs']], ['timeout', 'blocked'])

    def test_local_path_alias_rejected(self):
        with self.assertRaises(ValueError):
            run_jobs([self.job(), dict(self.job(), shard_id='1', checkpoint_dir='/tmp/./index')], '/unused')


if __name__ == '__main__':
    unittest.main()
