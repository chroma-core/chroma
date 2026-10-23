"""Mock-only reporting regression. No process is launched or signaled."""
import json
from pathlib import Path
import runpy
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

class Reporting(unittest.TestCase):
    def test_denied_group_signal_retains_unreconciled_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            script=Path(temp)/"watch_coordinator.py"
            script.write_bytes(Path(__file__).with_name("watch_coordinator.py").read_bytes())
            child=Mock(pid=123456);child.wait.return_value=0
            with patch("subprocess.Popen",return_value=child),patch("subprocess.run",return_value=SimpleNamespace(stdout="Z")),patch("os.killpg",side_effect=PermissionError("denied")):
                with self.assertRaises(SystemExit) as result:runpy.run_path(str(script),run_name="__main__")
            self.assertEqual(result.exception.code,1)
            receipt=json.loads((Path(temp)/"protocol-watchdog/receipt.json").read_text())
            self.assertEqual(receipt["status"],"unreconciled");self.assertIn("signal_error",receipt)
            self.assertIn("absence_error",receipt);self.assertEqual(receipt["exit_code"],0)
            child.wait.assert_called_once_with(timeout=5)

if __name__=="__main__":unittest.main(verbosity=2)
