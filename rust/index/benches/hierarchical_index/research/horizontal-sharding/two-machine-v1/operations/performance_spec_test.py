"""Validate the frozen job-spec matrix locally; no process or AWS execution."""
import argparse
import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("make_performance_spec", Path(__file__).with_name("make-performance-spec.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def arguments(arm, mode="run"):
    return argparse.Namespace(window_id="epoch2-test", window_deadline=2000, job_deadline=1600, arm=arm, mode=mode, worker_ip="172.31.1.2", peer_ip="172.31.1.3", base="/home/ubuntu/spann-two-machine-v1/epoch2", corpus_descriptor="/home/ubuntu/spann-two-machine-v1/epoch2/input/3m.json" if arm.startswith("3m-") else "/home/ubuntu/spann-two-machine-v1/epoch2/input/100k.json", input_file="/home/ubuntu/retained/input.f32")


class PerformanceSpecTests(unittest.TestCase):
    def test_all_six_arms_and_distributed_peer_specs_validate_with_mock_time(self):
        plan = m.json.loads((m.HERE / "workload-plan.json").read_text())
        count = 0
        for arm in plan["order"][1:]:
            for mode in (["run"] if arm.endswith("-reference") else ["run", "serve"]):
                with self.subTest(arm=arm, mode=mode):
                    args = arguments(arm, mode)
                    job = m.generate(args, 1000)
                    command, output = m.remote_job.validate(job, 1000)
                    self.assertEqual(job["instance_id"], m.WORKER if mode == "run" else m.PEER)
                    self.assertEqual(job["workload_timeout_seconds"], 600)
                    self.assertEqual(job["minimum_mem_available_bytes"], 32 * 1024**3)
                    self.assertEqual(job["minimum_free_bytes"], (12 if arm == "3m-reference" else 2) * 1024**3)
                    self.assertEqual(job["args"]["--workers"], 32)
                    self.assertEqual(command[0], job["binary"])
                    self.assertTrue(str(output).startswith(args.base + "/"))
                    count += 1
        self.assertEqual(count, 9)

    def test_peers_share_run_and_match_cadence_and_byte_bindings(self):
        one = m.generate(arguments("100k-two50k-distributed"), 1000)
        two = m.generate(arguments("100k-two50k-distributed", "serve"), 1000)
        self.assertEqual(one["args"]["--run"], two["args"]["--run"])
        self.assertEqual(one["args"]["--initial-batch"], 50_000)
        self.assertEqual(one["args"]["--first-owner-budget"], 40_000)
        self.assertEqual(one["args"]["--peer-data"], two["args"]["--data-bind"])
        self.assertEqual(one["binary_sha256"], m.PERFORMANCE_SHA256)
        reference = m.generate(arguments("3m-reference"), 1000)
        self.assertEqual(reference["binary_sha256"], m.REFERENCE_SHA256)
        self.assertEqual(reference["args"]["--rows"], 3_000_000)
        self.assertEqual(reference["args"]["--initial-batch-rows"], 100_000)
        self.assertEqual(reference["args"]["--later-batch-rows"], 1_000_000)

    def test_bad_role_deadline_address_and_namespace_fail_closed(self):
        changes = [{"mode": "serve", "arm": "100k-baseline-reference"}, {"arm": "unapproved-sweep"}, {"job_deadline": 2000}, {"job_deadline": 999}, {"worker_ip": "8.8.8.8"}, {"peer_ip": "172.31.1.2"}, {"base": "/home/ubuntu/spann-two-machine-v1/epoch1"}, {"input_file": None}]
        for change in changes:
            args = arguments("100k-baseline-reference")
            for key, value in change.items():
                setattr(args, key, value)
            with self.subTest(change=change), self.assertRaises(ValueError):
                m.generate(args, 1000)


if __name__ == "__main__":
    unittest.main()
