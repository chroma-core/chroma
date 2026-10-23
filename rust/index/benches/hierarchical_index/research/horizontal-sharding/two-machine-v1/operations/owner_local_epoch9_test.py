"""Pure spec/runner checks. No cloud, subprocess workload or benchmark starts."""
import argparse
import copy
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
location = importlib.util.spec_from_file_location("owner_local_spec", HERE / "make-owner-local-spec-epoch9.py")
g = importlib.util.module_from_spec(location)
location.loader.exec_module(g)


def args(arm="100k-before", mode="run"):
    return argparse.Namespace(arm=arm, mode=mode, window_id="epoch9-admission-test",
        window_deadline=1900, job_deadline=1590, worker_ip="10.0.0.1", peer_ip="10.0.0.2",
        base=g.BASE, corpus_descriptor=g.DESCRIPTOR, input_file=g.INPUT)


class SpecTests(unittest.TestCase):
    def test_actual_epoch7_baseline_binding_and_fixed_workload(self):
        value = g.generate(args(), 1000)
        self.assertEqual(value["binary"], "/home/ubuntu/spann-two-machine-v1/epoch7/build/two_machine_spann")
        self.assertEqual(value["binary_sha256"], "2fd0e4a2aecb9ab0718cd7a72a6105e809dc3273d12cc7fbaa0b22e76217f864")
        self.assertEqual(value["reader_sha256"], "9787bad5d8f5ce1fbf2c4aee63f3a651d2a9069cd67b9ab8bc3505873552c356")
        self.assertEqual(value["workload_timeout_seconds"], 600)
        self.assertEqual(value["deadline_unix"], 1590)
        for key, expected in {"--rows":100000,"--workers":32,"--initial-batch":100000,
                              "--owner-budget":80000,"--first-owner-budget":0,
                              "--split-threshold":2048,"--merge-threshold":512,
                              "--branching-factor":100}.items():
            self.assertEqual(value["args"][key], expected)
        self.assertEqual(value["args"]["--input-descriptor"], g.DESCRIPTOR)
        self.assertEqual(value["provenance"]["input_bytes"], 307200000)
        self.assertEqual(value["provenance"]["input_sha256"], "5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c")
        self.assertEqual(value["args"]["--output"], g.BASE+"/100k-before-owner1")

    def test_after_has_explicit_new_hash_binding_but_identical_workload(self):
        before = g.generate(args(), 1000)
        hashes = {"two_machine_spann":"a"*64,"two_machine_reader":"b"*64}
        with patch.object(g, "artifact_bindings", return_value=("/home/ubuntu/spann-two-machine-v1/epoch9/build",hashes,"c"*64)):
            after = g.generate(args("100k-after"), 1000)
            peer = g.generate(args("100k-after","serve"), 1000)
        excluded = {"--run","--output","--reader-binary"}
        self.assertEqual({k:v for k,v in before["args"].items() if k not in excluded},
                         {k:v for k,v in after["args"].items() if k not in excluded})
        self.assertNotEqual(before["args"]["--run"], after["args"]["--run"])
        self.assertEqual(after["binary_sha256"], "a"*64)
        self.assertEqual(after["provenance"]["artifact_hash_file_sha256"], "c"*64)
        self.assertEqual(peer["args"]["--run"], after["args"]["--run"])
        self.assertEqual(peer["instance_id"], g.PEER)
        self.assertEqual(peer["args"]["--output"], g.BASE+"/100k-after-owner2")
        with patch.object(Path,"read_text",side_effect=FileNotFoundError("after build receipt absent")):
            with self.assertRaises(FileNotFoundError):
                g.artifact_bindings("100k-after")
        with patch.object(Path,"read_text",return_value='{"two_machine_spann":"unknown","two_machine_reader":"unknown"}'):
            with self.assertRaises(ValueError):
                g.artifact_bindings("100k-after")

    def test_other_workloads_changed_input_deadline_and_closed_names_reject(self):
        original = args()
        for mutation in (
            lambda a:setattr(a,"arm","3m-distributed"),
            lambda a:setattr(a,"arm","100k-two50k-distributed"),
            lambda a:setattr(a,"window_id","epoch8-closed"),
            lambda a:setattr(a,"base","/home/ubuntu/spann-two-machine-v1/epoch7"),
            lambda a:setattr(a,"corpus_descriptor","/other/100k.json"),
            lambda a:setattr(a,"input_file","/other/file.f32"),
            lambda a:setattr(a,"job_deadline",1591),
            lambda a:setattr(a,"window_deadline",1595),
            lambda a:setattr(a,"peer_ip","10.0.0.1"),
        ):
            changed=copy.copy(original);mutation(changed)
            with self.assertRaises(ValueError):g.generate(changed,1000)
        with self.assertRaises(ValueError):g.generate(original,1590)
        later=copy.copy(original);later.window_id="epoch9-another-fresh-window"
        self.assertNotEqual(g.generate(later,1000)["args"]["--run"],g.generate(original,1000)["args"]["--run"])

    def test_runner_is_one_explicit_arm_without_retry_or_profile_switch(self):
        location=importlib.util.spec_from_file_location("owner_local_runner",HERE/"run-one-arm-epoch9.py")
        runner=importlib.util.module_from_spec(location);location.loader.exec_module(runner)
        self.assertEqual(runner.execution_bounds("100k-before"),(590,660))
        self.assertEqual(runner.execution_bounds("100k-after"),(590,660))
        for command in (["runner","3m-distributed"],["runner","100k-before","--attempt","2"],["runner","100k-before","--profile"]):
            with patch.object(sys,"argv",command),patch("sys.stderr"):
                with self.assertRaises(SystemExit):runner.main()


if __name__ == "__main__":unittest.main()
