"""Bounded process-group cleanup for operator build and supervisor stages."""
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def group_exists(group):
    try:
        os.killpg(group, 0)
        return True
    except ProcessLookupError:
        return False


def send(group, sig):
    try:
        os.killpg(group, sig)
    except ProcessLookupError:
        pass


def descendant_groups(leader):
    """Linux fallback records PID start ticks and session before signaling."""
    if not Path('/proc').is_dir():
        return []
    rows = {}
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / 'stat').read_text().rsplit(')', 1)[1].split()
            rows[int(entry.name)] = (int(fields[1]), int(fields[2]), int(fields[3]), fields[19])
        except (OSError, ValueError, IndexError):
            continue
    owned = {leader}
    while True:
        added = {pid for pid, row in rows.items() if row[0] in owned} - owned
        if not added:
            break
        owned.update(added)
    return [(pid, rows[pid]) for pid in owned if pid in rows and rows[pid][1] == pid and rows[pid][2] == pid]


def fallback_groups(records):
    outcomes = []
    for pid, original in records:
        try:
            fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
            current = (int(fields[1]), int(fields[2]), int(fields[3]), fields[19])
        except FileNotFoundError:
            outcomes.append({'pid': pid, 'state': 'leader-absent-unproven-group'})
            continue
        if current[1:] != original[1:]:
            raise RuntimeError('Fallback process identity changed')
        send(pid, signal.SIGKILL)
        outcomes.append({'pid': pid, 'state': 'verified-session-killed'})
    return outcomes


def run_owned(command, receipt, timeout, *, cwd=None, env=None, stdout=None,
              stderr=None, graceful_seconds=5):
    """TERM first lets a supervisor clean its independently grouped native child."""
    started = time.monotonic()
    child = None
    reason = None
    previous = {}
    result = {'command': list(command), 'status': 'starting'}

    def interrupted(signum, frame):
        raise InterruptedError('Operator received signal ' + str(signum))

    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, interrupted)
        child = subprocess.Popen(command, cwd=cwd, env=env, stdout=stdout,
                                 stderr=stderr, start_new_session=True)
        result.update(pid=child.pid, process_group=child.pid)
        try:
            child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            reason = 'timeout'
        result['status'] = 'completed' if reason is None else reason
    except BaseException as error:
        reason = 'interrupted' if isinstance(error, InterruptedError) else 'exception'
        result.update(status=reason, error=repr(error))
        raise
    finally:
        # Ignore further interrupts during bounded cleanup; restore handlers afterward.
        for sig in previous:
            signal.signal(sig, signal.SIG_IGN)
        cleanup_error = None
        try:
            if child is not None:
                if reason is not None or group_exists(child.pid):
                    separate = descendant_groups(child.pid)
                    result['fallback_sessions'] = separate
                    send(child.pid, signal.SIGTERM)
                    until = time.monotonic() + graceful_seconds
                    while time.monotonic() < until:
                        child.poll()
                        if not group_exists(child.pid):
                            break
                        time.sleep(.02)
                    if group_exists(child.pid):
                        send(child.pid, signal.SIGKILL)
                    result['fallback_outcomes'] = fallback_groups(separate)
                    child.wait(timeout=3)
                result['returncode'] = child.poll()
                result['group_absent'] = not group_exists(child.pid)
                result['cleanup_scope'] = 'Owned group only; a supervisor owns cleanup of its separate native group. Group absence is checked, not inferred from leader exit.'
        except BaseException as error:
            cleanup_error = error
            result['cleanup_error'] = repr(error)
            result['cleanup_proven'] = False
        finally:
            result['elapsed_seconds'] = time.monotonic() - started
            path = Path(receipt)
            with path.open('x') as stream:
                json.dump(result, stream, indent=2)
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
            for sig, handler in previous.items():
                signal.signal(sig, handler)
        if cleanup_error is not None and 'error' not in result:
            raise RuntimeError('Cleanup failed; receipt preserved') from cleanup_error
    if reason is not None:
        raise TimeoutError('Owned command timed out')
    if not result.get('group_absent', False):
        raise RuntimeError('Owned process group remains; cleanup is unproven')
    return subprocess.CompletedProcess(command, child.returncode)
