import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from remote_stages import prepare_output_root

PACKAGE = Path(__file__).resolve().parents[3] / 'dynamic-review/full-record-million-runtime-v3'
module_spec = importlib.util.spec_from_file_location('real_supervisor', PACKAGE / 'supervise.py')
supervisor = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(supervisor)


class OutputParentTests(unittest.TestCase):
    def fixture(self, base):
        control = base / 'control'
        source = control / 'source'
        source.mkdir(parents=True)
        mount = base / 'data'
        mount.mkdir()
        cohort = mount / 'experiment'
        cohort.mkdir()
        inputs = mount / 'retained'
        inputs.mkdir()
        (inputs / 'canonical').write_bytes(b'preserved')
        plan = json.loads((PACKAGE / 'PLAN-TEMPLATE.json').read_text())
        plan.update(source_root=str(source), data_mount=str(mount),
                    data_output_root=str(cohort / 'epoch'), control_root=str(control),
                    receipt_dir=str(control / 'measurement'))
        spec = {'vectors': {'path': str(inputs / 'canonical')},
                'output': str(cohort / 'epoch/index')}
        return plan, spec

    def test_fresh_parent_admitted_before_native_creates_index(self):
        with tempfile.TemporaryDirectory() as folder:
            plan, spec = self.fixture(Path(folder).resolve())
            prepare_output_root(plan, spec, supervisor.policy_and_paths)
            self.assertTrue(Path(plan['data_output_root']).is_dir())
            self.assertFalse(Path(spec['output']).exists())
            Path(spec['output']).mkdir()  # Actual native nonrecursive create seam.
            self.assertTrue(Path(spec['output']).is_dir())

    def test_existing_root_preserved_and_symlink_rejected(self):
        for symlink in [False, True]:
            with self.subTest(symlink=symlink), tempfile.TemporaryDirectory() as folder:
                plan, spec = self.fixture(Path(folder).resolve())
                root = Path(plan['data_output_root'])
                retained = Path(folder) / 'keep'
                retained.mkdir()
                marker = retained / 'marker'
                marker.write_bytes(b'keep')
                if symlink:
                    root.symlink_to(retained, target_is_directory=True)
                else:
                    root.mkdir()
                    (root / 'unexpected').write_bytes(b'old')
                with self.assertRaises(ValueError):
                    prepare_output_root(plan, spec, supervisor.policy_and_paths)
                self.assertEqual(marker.read_bytes(), b'keep')
                if not symlink:
                    self.assertEqual((root / 'unexpected').read_bytes(), b'old')

    def test_failed_admission_creates_nothing(self):
        with tempfile.TemporaryDirectory() as folder:
            plan, spec = self.fixture(Path(folder).resolve())
            spec['output'] = plan['source_root'] + '/index'
            with self.assertRaises(ValueError):
                prepare_output_root(plan, spec, supervisor.policy_and_paths)
            self.assertFalse(Path(plan['data_output_root']).exists())


if __name__ == '__main__':
    unittest.main()
