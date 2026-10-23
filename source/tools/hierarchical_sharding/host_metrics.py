"""Linux /proc time series. OS boundaries are explicit; logical app I/O is separate."""
import argparse
import json
import os
import socket
import time
from pathlib import Path


def snapshot(pid=None, proc='/proc'):
    root = Path(proc)
    row = dict(unix_time_s=time.time(), monotonic_s=time.monotonic(), host=socket.gethostname(),
               boundaries=dict(cpu='host /proc/stat jiffies', disk='host /proc/diskstats sectors * 512; devices overlap, never sum all',
               network='host interface /proc/net/dev bytes; includes unrelated processes',
               process='selected PID /proc io physical read_bytes/write_bytes and logical rchar/wchar; RSS KiB'))
    row['host_cpu_jiffies'] = [int(v) for v in (root/'stat').read_text().splitlines()[0].split()[1:]]
    row['clock_ticks_per_second'] = os.sysconf('SC_CLK_TCK')
    row['disk_devices'] = {}
    for line in (root/'diskstats').read_text().splitlines():
        v = line.split()
        if len(v) >= 14:
            row['disk_devices'][v[2]] = dict(read_operations=int(v[3]), read_bytes=int(v[5])*512,
                write_operations=int(v[7]), write_bytes=int(v[9])*512, io_in_progress=int(v[11]), io_ms=int(v[12]))
    row['network_interfaces'] = {}
    for line in (root/'net/dev').read_text().splitlines()[2:]:
        name, values = line.split(':', 1)
        v = values.split()
        row['network_interfaces'][name.strip()] = dict(receive_bytes=int(v[0]), receive_packets=int(v[1]),
            transmit_bytes=int(v[8]), transmit_packets=int(v[9]))
    if pid:
        row['pid'] = pid
        try:
            status = dict(line.split(':', 1) for line in (root/str(pid)/'status').read_text().splitlines() if ':' in line)
            row['process_memory_kib'] = {k: int(status[k].split()[0]) for k in ('VmRSS', 'VmHWM') if k in status}
            row['process_io'] = {k: int(v) for k, v in (line.split(':', 1) for line in (root/str(pid)/'io').read_text().splitlines())}
            # comm can contain spaces and parentheses; split after the final closing parenthesis.
            stat = (root/str(pid)/'stat').read_text().rsplit(')', 1)[1].split()
            row['process_cpu_seconds'] = dict(user=int(stat[11])/row['clock_ticks_per_second'], system=int(stat[12])/row['clock_ticks_per_second'])
            row['process_page_faults'] = dict(minor=int(stat[7]), major=int(stat[9]))
        except (FileNotFoundError, ProcessLookupError):
            row['process_status'] = 'exited'
        except PermissionError:
            row['process_status'] = 'permission_denied'
    return row


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--pid', type=int)
    p.add_argument('--interval', type=float, default=1)
    p.add_argument('--duration', type=float, required=True)
    p.add_argument('--output', required=True)
    a = p.parse_args()
    if min(a.interval, a.duration) <= 0:
        p.error('positive interval and duration required')
    stop = time.monotonic() + a.duration
    with open(a.output, 'w') as out:
        while True:
            out.write(json.dumps(snapshot(a.pid))+'\n')
            out.flush()
            remaining = stop-time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(a.interval, remaining))

if __name__ == '__main__':
    main()
