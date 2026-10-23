"""Pure admission and mocked process-group tests; starts no remote job."""
import copy
import importlib.util
from pathlib import Path
import signal
import unittest
from unittest.mock import patch, Mock

specification = importlib.util.spec_from_file_location("remote_job", Path(__file__).with_name("remote-job-epoch8.py"))
m = importlib.util.module_from_spec(specification)
specification.loader.exec_module(m)


def spec():
    return {"combined_review_passed": True, "job_id": "correctness-one", "window_id": "window-one", "instance_id": "i-011ff180f03584f40", "kind": "correctness", "mode": "run", "binary": "/home/ubuntu/build/two_machine_correctness", "binary_sha256": "a" * 64, "reader": "/home/ubuntu/build/two_machine_reader", "reader_sha256": "b" * 64, "window_deadline_unix": 2000, "deadline_unix": 1900, "workload_timeout_seconds": 600, "minimum_mem_available_bytes": 1024**3, "minimum_free_bytes": 1024**3, "args": {"--mode": "run", "--owner": 1, "--run": 123, "--output": "/home/ubuntu/fresh-output", "--data-bind": "0.0.0.0:9100", "--control-bind": "0.0.0.0:9101", "--peer-data": "10.0.0.2:9100", "--peer-control": "10.0.0.2:9101", "--token-file": "/home/ubuntu/token", "--shutdown-at-unix": 1900, "--reader-binary": "/home/ubuntu/build/two_machine_reader", "--input-file": "/home/ubuntu/fixture.f32"}}


class JobTests(unittest.TestCase):
    def test_reviewed_args_and_bounds(self):
        original = spec()
        command, output = m.validate(original, 1000)
        self.assertEqual(command[0], original["binary"])
        self.assertEqual(str(output), original["args"]["--output"])
        for mutation in (lambda s: s.update(instance_id="i-0f0b5dcb6888ad43d"), lambda s: s.update(combined_review_passed=False), lambda s: s.update(workload_timeout_seconds=601), lambda s: s.update(deadline_unix=2000), lambda s: s["args"].update({"--shell-command": "anything"}), lambda s: s["args"].update({"--owner": 2}), lambda s: s["args"].update({"--reader-binary": "/other-reader"})):
            changed = copy.deepcopy(original)
            mutation(changed)
            with self.assertRaises(ValueError):
                m.validate(changed, 1000)
        with self.assertRaises(ValueError):
            m.validate(original, 1901)

    def test_duplicate_fields_and_exact_owned_group_only(self):
        with self.assertRaises(ValueError):
            m.unique_object([("binary", "one"), ("binary", "two")])
        victims = [{"pid": 410, "start_ticks": 123}]
        with patch.object(m, "owned_group", side_effect=[victims, []]), patch.object(m.os, "killpg") as kill:
            self.assertEqual(m.terminate(410, "job:hash"), victims)
            kill.assert_called_once_with(410, signal.SIGTERM)
        with patch.object(m, "owned_group", side_effect=ValueError("unowned group")), patch.object(m.os, "killpg") as kill:
            with self.assertRaises(ValueError):
                m.terminate(410, "wrong-marker")
            kill.assert_not_called()

    def test_failed_cleanup_retains_ownership_until_fixed_host_deadline(self):
        clock=[100.0]
        child=Mock(pid=410)
        child.poll.return_value=None
        def sleep(seconds):
            clock[0]+=seconds
        victims=[{"pid":410,"start_ticks":123}]
        with patch.object(m,"terminate",side_effect=RuntimeError("survives SIGKILL")), patch.object(m,"owned_group",return_value=victims), patch.object(m.time,"time",side_effect=lambda:clock[0]), patch.object(m.time,"sleep",side_effect=sleep), patch.object(m,"write") as write, patch.object(m.signal,"getsignal",return_value="original"), patch.object(m.signal,"signal") as handlers:
            self.assertFalse(m.cleanup_owned(child,"job:hash",Path("/unused"),103))
            self.assertEqual(clock[0],103)
            self.assertTrue(any(call.args[1].get("lock_retained") for call in write.call_args_list))
            self.assertFalse(write.call_args_list[-1].args[1]["confirmed"])
            self.assertEqual(handlers.call_args_list[0].args,(signal.SIGTERM,signal.SIG_IGN))
            self.assertEqual(handlers.call_args_list[2].args,(signal.SIGHUP,signal.SIG_IGN))
            self.assertEqual(handlers.call_args_list[-1].args,(signal.SIGHUP,"original"))
        child.wait.assert_not_called()

    def test_failed_cleanup_can_release_only_after_clean_readback(self):
        clock=[100.0]
        child=Mock(pid=410)
        child.poll.return_value=0
        with patch.object(m,"terminate",side_effect=RuntimeError("readback initially fails")), patch.object(m,"owned_group",side_effect=[[{"pid":410}],[]]), patch.object(m.time,"time",side_effect=lambda:clock[0]), patch.object(m.time,"sleep",side_effect=lambda seconds:clock.__setitem__(0,clock[0]+seconds)), patch.object(m,"write"), patch.object(m.signal,"getsignal",return_value="original"), patch.object(m.signal,"signal"):
            self.assertTrue(m.cleanup_owned(child,"job:hash",Path("/unused"),103))
            self.assertLess(clock[0],103)
            child.wait.assert_called_once_with(timeout=1)


class EpochEightBoundTests(unittest.TestCase):
    def extended_spec(self, mode="run"):
        value = spec()
        owner = 1 if mode == "run" else 2
        value.update(kind="performance", mode=mode, window_id="epoch8-review-test",
                     job_id=f"3m-distributed-owner{owner}",
                     deadline_unix=2790, window_deadline_unix=6400,
                     workload_timeout_seconds=1800,
                     binary=m.EXTENDED_BINARY, binary_sha256=m.EXTENDED_BINARY_SHA256,
                     reader=m.EXTENDED_READER, reader_sha256=m.EXTENDED_READER_SHA256,
                     provenance={"arm": "3m-distributed",
                                 "workload_plan_sha256": "39f31ba042f79d29e678278a5147343f218088a6675fd9cb31746f606ccb46b4",
                                 "input_sha256": "18fb3c71c45bb70d28730f8edfdbd2ae90a3361b150a4871f09d870cb6684aba",
                                 "input_bytes": 9216000000})
        value["args"].pop("--input-file")
        value["args"].update(m.EXTENDED_SETTINGS)
        value["args"].update({"--input-descriptor": "/home/ubuntu/spann-two-machine-v1/epoch2/input/3m.json",
                             "--output": f"/home/ubuntu/spann-two-machine-v1/epoch8/3m-distributed-owner{owner}",
                             "--reader-binary": m.EXTENDED_READER,
                             "--shutdown-at-unix": 2790, "--mode": mode, "--owner": owner})
        return value

    def test_fixed_3m_run_and_peer_have_only_extended_exception(self):
        for mode in ("run", "serve"):
            value = self.extended_spec(mode)
            m.validate(value, 1000)
            self.assertEqual(value["workload_timeout_seconds"], 1800)
            for mutation in (
                lambda s: s.update(workload_timeout_seconds=1801),
                lambda s: s.update(kind="reference"),
                lambda s: s.update(kind="correctness"),
                lambda s: s["args"].update({"--rows": 100000}),
                lambda s: s["args"].update({"--rows": 3000001}),
                lambda s: s["args"].update({"--workers": 4}),
                lambda s: s.update(binary_sha256="c" * 64),
                lambda s: s.update(reader_sha256="d" * 64),
                lambda s: s.update(window_id="epoch7-closed"),
                lambda s: s["provenance"].update(arm="100k-baseline-distributed"),
                lambda s: s["provenance"].update(workload_plan_sha256="e" * 64),
                lambda s: s["provenance"].pop("workload_plan_sha256"),
                lambda s: s["provenance"].update(input_sha256="f" * 64),
                lambda s: s["provenance"].pop("input_sha256"),
                lambda s: s["provenance"].update(input_bytes=9216000001),
                lambda s: s["provenance"].pop("input_bytes"),
                lambda s: s["args"].update({"--input-descriptor": "/other/3m.json"}),
                lambda s: s["args"].pop("--input-descriptor"),
                lambda s: s["args"].update({"--output": "/home/ubuntu/spann-two-machine-v1/epoch7/retained"}),
            ):
                changed = copy.deepcopy(value)
                mutation(changed)
                with self.assertRaises(ValueError):
                    m.validate(changed, 1000)

    def test_absolute_deadlines_remain_fixed(self):
        value = self.extended_spec()
        for mutation in (
            lambda s: s.update(deadline_unix=2791),
            lambda s: s.update(window_deadline_unix=6401),
            lambda s: s.update(window_deadline_unix=2795),
            lambda s: s["args"].update({"--shutdown-at-unix": 2791}),
        ):
            changed = copy.deepcopy(value)
            mutation(changed)
            with self.assertRaises(ValueError):
                m.validate(changed, 1000)
        with self.assertRaises(ValueError):
            m.validate(value, 2790)
        m.validate(value, 1100)
        self.assertEqual(value["deadline_unix"], 2790)

    def test_generator_binds_epoch7_binaries_and_fresh_epoch8_identity(self):
        import argparse
        location = importlib.util.spec_from_file_location("epoch8_generator", Path(__file__).with_name("make-performance-spec-epoch8.py"))
        generator = importlib.util.module_from_spec(location)
        location.loader.exec_module(generator)
        def arguments(arm, window="epoch8-review-test"):
            return argparse.Namespace(arm=arm, mode="run", window_id=window,
                window_deadline=6400, job_deadline=2790 if arm == "3m-distributed" else 1590,
                worker_ip="10.0.0.1", peer_ip="10.0.0.2",
                base="/home/ubuntu/spann-two-machine-v1/epoch8",
                corpus_descriptor="/home/ubuntu/spann-two-machine-v1/epoch2/input/3m.json",
                input_file="/home/ubuntu/spann-two-machine-v1/epoch2/input/fineweb3m.f32")
        first = generator.generate(arguments("3m-distributed"), 1000)
        self.assertEqual(first["binary"], m.EXTENDED_BINARY)
        self.assertEqual(first["binary_sha256"], m.EXTENDED_BINARY_SHA256)
        self.assertEqual(first["deadline_unix"], 2790)
        self.assertEqual(first["workload_timeout_seconds"], 1800)
        second = generator.generate(arguments("3m-distributed", "epoch8-another-window"), 1000)
        self.assertNotEqual(first["args"]["--run"], second["args"]["--run"])
        for arm in ("3m-reference", "100k-baseline-reference", "100k-baseline-distributed", "100k-two50k-distributed"):
            self.assertEqual(generator.generate(arguments(arm), 1000)["workload_timeout_seconds"], 600)
        with self.assertRaises(ValueError):
            generator.generate(arguments("3m-distributed", "epoch7-closed"), 1000)


class EpochEightRunnerTests(unittest.TestCase):
    def test_runner_requires_remaining_host_budget_and_disallows_attempt_flag(self):
        import sys
        location = importlib.util.spec_from_file_location("epoch8_runner", Path(__file__).with_name("run-one-arm-epoch8.py"))
        runner = importlib.util.module_from_spec(location)
        location.loader.exec_module(runner)
        self.assertEqual(runner.execution_bounds("3m-distributed"), (1790, 1860))
        self.assertEqual(runner.execution_bounds("3m-reference"), (590, 660))
        self.assertEqual(runner.execution_bounds("100k-baseline-distributed"), (590, 660))
        with patch.object(sys, "argv", ["runner", "3m-distributed", "--attempt", "2"]), patch("sys.stderr"):
            with self.assertRaises(SystemExit):
                runner.main()


if __name__ == "__main__":
    unittest.main()
