import tempfile
import unittest
from pathlib import Path
from .host_metrics import snapshot


class HostMetricsTests(unittest.TestCase):
    def test_named_boundaries_and_process_field_offsets(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            (p/'net').mkdir()
            (p/'42').mkdir()
            (p/'stat').write_text('cpu 10 20 30 40\n')
            (p/'diskstats').write_text('259 0 nvme0n1 2 0 3 0 5 0 7 0 1 9 0\n')
            (p/'net/dev').write_text('header\nheader\n eth0: 100 2 0 0 0 0 0 0 200 3 0 0 0 0 0 0\n')
            (p/'42/status').write_text('VmRSS: 123 kB\nVmHWM: 456 kB\n')
            (p/'42/io').write_text('rchar: 999\nread_bytes: 512\n')
            # Tail starts at field3; offsets7/9/11/12 map to minflt/majflt/utime/stime.
            tail = ['S'] + ['0']*20
            for index, value in [(7, '4'), (9, '5'), (11, '100'), (12, '200')]:
                tail[index] = value
            (p/'42/stat').write_text('42 (name with (parens)) '+' '.join(tail))
            row = snapshot(42, tmp)
            self.assertEqual(row['disk_devices']['nvme0n1']['read_bytes'], 1536)
            self.assertEqual(row['network_interfaces']['eth0']['transmit_bytes'], 200)
            self.assertEqual(row['process_memory_kib']['VmHWM'], 456)
            self.assertEqual(row['process_page_faults'], dict(minor=4, major=5))
            self.assertEqual(row['process_cpu_seconds']['system'], 200/row['clock_ticks_per_second'])

if __name__ == '__main__':
    unittest.main()
