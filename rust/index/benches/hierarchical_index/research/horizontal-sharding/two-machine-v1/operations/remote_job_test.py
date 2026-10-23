"""Pure admission and mocked process-group tests; starts no remote job."""
import copy
import importlib.util
from pathlib import Path
import signal
import unittest
from unittest.mock import patch, Mock

specification = importlib.util.spec_from_file_location("remote_job", Path(__file__).with_name("remote-job.py"))
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


if __name__ == "__main__":
    unittest.main()
