"""Supervise one Linux task; external absolute machine shutdown is independent."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time


def run(args, cwd, receipt_dir, deadline_unix, min_free_bytes, max_rss_bytes, max_log_bytes, env=None):
    root = Path(receipt_dir); root.mkdir(parents=True, exist_ok=False)
    if time.time() >= deadline_unix or min(min_free_bytes, max_rss_bytes, max_log_bytes) <= 0:
        raise ValueError('expired deadline or invalid limits')
    if shutil.disk_usage(root).free < min_free_bytes:
        raise RuntimeError('insufficient initial disk')
    start = time.time(); peak = 0; reason = None; error = None
    with (root/'stdout').open('xb') as out, (root/'stderr').open('xb') as err:
        proc = subprocess.Popen(args, cwd=cwd, env=env, stdout=out, stderr=err, start_new_session=True)
        try:
            while proc.poll() is None:
                rss = 0
                try:
                    lines = Path(f'/proc/{proc.pid}/status').read_text().splitlines()
                    rss = next((int(x.split()[1])*1024 for x in lines if x.startswith('VmRSS:')), 0)
                except FileNotFoundError:
                    if proc.poll() is None and not Path('/proc').exists():
                        raise RuntimeError('Linux process accounting required')
                peak = max(peak, rss)
                if time.time() >= deadline_unix: reason = 'absolute task deadline'
                if rss > max_rss_bytes: reason = 'sampled process RSS'
                if shutil.disk_usage(root).free < min_free_bytes: reason = 'disk floor'
                if sum((root/n).stat().st_size for n in ['stdout','stderr']) > max_log_bytes: reason = 'log budget'
                if reason: break
                time.sleep(.1)
        except BaseException as exc:
            error = repr(exc)
            raise
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
                try: proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL); proc.wait()
            result = dict(command=args, cwd=str(cwd), started_unix=start, elapsed_s=time.time()-start,
                          deadline_unix=deadline_unix, returncode=proc.returncode,
                          stop_reason=reason, supervisor_error=error, sampled_peak_process_rss_bytes=peak,
                          memory_scope='Native task process only; child-process aggregate excluded. No hard RSS quota.')
            (root/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    if reason or proc.returncode != 0: raise RuntimeError('task failed or stopped; evidence retained')
    return result
