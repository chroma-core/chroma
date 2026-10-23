import json
from pathlib import Path
import sys
import tempfile
import time
import unittest

import supervise


class HostPolicyTests(unittest.TestCase):
    def test_null_and_unverified_host_reject(self):
        with self.assertRaises(ValueError):
            supervise.host_policy({}, {'vectors': {'path': None}})
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'host.json'
            host = {'account': '079819354890', 'physical_cores': 32,
                    'instance_id': 'test-task-only', 'input_path': '/retained/input',
                    'fresh_identity_verified': False, 'capacity_verified': False,
                    'region': 'us-east-1', 'data_mount': str(Path(folder).resolve()), 'deadline_unix': 12345}
            path.write_text(json.dumps(host))
            proof = Path(folder) / 'shutdown.json'
            proof.write_text(json.dumps({'instance_id': 'test-task-only', 'deadline_unix': 12345}))
            plan = {'host_admission': str(path), 'host_admission_sha256': supervise.digest(path),
                    'selected_instance_id': 'test-task-only', 'region': 'us-east-1',
                    'data_mount': str(Path(folder).resolve()), 'deadline_unix': 12345, 'shutdown_proof': str(proof)}
            with self.assertRaises(AssertionError):
                supervise.host_policy(plan, {'vectors': {'path': '/retained/input'}})
            host.update(fresh_identity_verified=True, capacity_verified=True)
            path.write_text(json.dumps(host))
            plan['host_admission_sha256'] = supervise.digest(path)
            supervise.host_policy(plan, {'vectors': {'path': '/retained/input'}})
            for key, value in [('selected_instance_id', 'other-task'), ('region', 'us-west-2'), ('deadline_unix', 12346), ('data_mount', '/other/mount')]:
                changed = dict(plan, **{key: value})
                with self.subTest(key=key), self.assertRaises(AssertionError):
                    supervise.host_policy(changed, {'vectors': {'path': '/retained/input'}})
            with self.assertRaises(AssertionError):
                supervise.host_policy(plan, {'vectors': {'path': '/different/input'}})

    def test_actual_child_environment_32(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            result = supervise.run([sys.executable, '-c',
                "import os;assert os.environ['TOKIO_WORKER_THREADS']=='32';assert os.environ['RAYON_NUM_THREADS']=='32'"],
                root / 'receipt', root / 'output', time.time() + 5,
                512 * 1024 * 1024, 1024 * 1024, 1024 * 1024, root)
            self.assertEqual(result['exit_code'], 0)
            self.assertEqual(result['thread_environment'], {'TOKIO_WORKER_THREADS': '32', 'RAYON_NUM_THREADS': '32'})
