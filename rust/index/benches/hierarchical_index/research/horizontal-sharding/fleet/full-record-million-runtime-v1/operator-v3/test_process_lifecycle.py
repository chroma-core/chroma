import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from process_lifecycle import run_owned


class LifecycleTests(unittest.TestCase):
    def test_timeout_stops_descendant_delayed_write(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            marker = root / 'late'
            code = "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',\"import time,pathlib; time.sleep(2); pathlib.Path(sys.argv[1]).touch()\".replace('import time,pathlib','import time,pathlib,sys'),sys.argv[1]]); time.sleep(10)"
            with self.assertRaises((TimeoutError, RuntimeError)):
                run_owned([sys.executable, '-c', code, str(marker)], root / 'receipt', .15, graceful_seconds=.2)
            time.sleep(2.1)
            self.assertFalse(marker.exists())
            self.assertEqual(json.loads((root / 'receipt').read_text())['status'], 'timeout')

    def test_interrupted_supervisor_cleans_separate_native_group(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            supervisor = root / 'supervisor.py'
            supervisor.write_text('''import os,signal,subprocess,sys,time
child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'],start_new_session=True)
def stop(sig,frame):
 os.killpg(child.pid,signal.SIGKILL)
 child.wait()
 open(sys.argv[1],'w').write('clean')
 raise SystemExit(0)
signal.signal(signal.SIGTERM,stop)
open(sys.argv[2],'w').write(str(os.getpid()))
time.sleep(30)
''')
            launcher = root / 'launcher.py'
            module = str(Path(__file__).resolve().parent)
            launcher.write_text("import sys\nsys.path.insert(0," + repr(module) + ")\nfrom process_lifecycle import run_owned\nrun_owned(sys.argv[2:],sys.argv[1],20,graceful_seconds=2)\n")
            process = subprocess.Popen([sys.executable, str(launcher), str(root / 'receipt'), sys.executable, str(supervisor), str(root / 'clean'), str(root / 'ready')])
            until = time.monotonic() + 3
            while not (root / 'ready').exists() and time.monotonic() < until:
                time.sleep(.02)
            self.assertTrue((root / 'ready').exists())
            process.terminate()
            process.wait(timeout=5)
            self.assertEqual((root / 'clean').read_text(), 'clean')
            result = json.loads((root / 'receipt').read_text())
            self.assertEqual(result['status'], 'interrupted')
            self.assertTrue(result['group_absent'])


if __name__ == '__main__':
    unittest.main()

class CleanupFailureTests(unittest.TestCase):
    def test_injected_cleanup_failure_preserves_receipt(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as folder:
            receipt = Path(folder) / 'receipt'
            with patch('process_lifecycle.fallback_groups', side_effect=OSError('injected cleanup failure')):
                with self.assertRaises(RuntimeError):
                    run_owned([sys.executable, '-c', 'import time;time.sleep(10)'], receipt, .05, graceful_seconds=.1)
            result = json.loads(receipt.read_text())
            self.assertEqual(result['status'], 'timeout')
            self.assertIn('injected cleanup failure', result['cleanup_error'])
            self.assertFalse(result['cleanup_proven'])

    @unittest.skipUnless(sys.platform == 'linux', 'Requires Linux /proc identity fallback')
    def test_linux_term_ignoring_supervisor_separate_session(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            code = "import subprocess,signal,sys,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); c=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'],start_new_session=True); open(sys.argv[1],'w').write(str(c.pid)); time.sleep(30)"
            with self.assertRaises((TimeoutError, RuntimeError)):
                run_owned([sys.executable, '-c', code, str(root / 'native-pid')], root / 'receipt', .3, graceful_seconds=.1)
            result = json.loads((root / 'receipt').read_text())
            native = int((root / 'native-pid').read_text())
            self.assertTrue(any(item['pid'] == native and item['state'] == 'verified-session-killed' for item in result['fallback_outcomes']))
            until = time.monotonic() + 2
            while time.monotonic() < until:
                path = Path(f'/proc/{native}/stat')
                if not path.exists() or path.read_text().rsplit(')', 1)[1].split()[0] == 'Z':
                    break
                time.sleep(.02)
            else:
                self.fail('Separate native session still runs')
