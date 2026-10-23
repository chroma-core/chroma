"""Build only the requested boundaries, reducing batch size under memory pressure."""
import json
import os
from pathlib import Path
import signal
import subprocess
import time

ROOT = Path('/mnt/msmarco')
OUT = ROOT / 'results'
BINARY = OUT / 'binary'
LIVE = ROOT / 'build-live'
BASE = ['--dataset', 'ms-marco', '--metric', 'l2', '--branching-factor', '100', '--split-threshold', '2048', '--merge-threshold', '512', '--threads', '64', '--balance-threads', '64', '--write-navigation', 'fp', '--fp-npa', '--write-beam-tau', '1.5', '--write-beam-min', '10', '--write-beam-max', '16', '--max-replicas', '1', '--write-rng-epsilon', '0', '--write-rng-factor', '4', '--max-cache-bytes', '1073741824', '--compute-gt-clusters', 'false', '--num-queries', '0', '--clear-reader-block-pins', 'true', '--save']


def count():
    p = LIVE / 'checkpoint.json'
    return json.loads(p.read_text())['total_vectors'] if p.exists() else 0


def run(target, batch, label):
    before = count()
    assert target % batch == 0 and before % batch == 0
    if before:
        subprocess.run(['python3', str(OUT / 'checkpoints.py'), 'normalize', str(LIVE), str(batch)], check=True)
    args = [str(BINARY), *BASE, '--checkpoint', str(target // batch), '--checkpoint-size', str(batch), '--save-dir', str(LIVE)]
    if before:
        args.append('--resume')
    (OUT / f'{label}.command.json').write_text(json.dumps(args, indent=2) + '\n')
    (OUT / 'phase.txt').write_text(f'{label}: {before:,} -> {target:,}, batch {batch:,}\n')
    print(f'Start {label}: {before:,} -> {target:,}; batch {batch:,}', flush=True)
    pressured = False
    with (OUT / f'{label}.log').open('w') as log:
        p = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        (OUT / 'benchmark.pid').write_text(str(p.pid))
        while p.poll() is None:
            try:
                available = int(next(x.split()[1] for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:'))) * 1024
                if available < 24 * 1024**3:
                    pressured = True
                    print(f'Memory guard: {available / 1024**3:.1f} GiB available; stop and resume from the last saved checkpoint with smaller batches.', flush=True)
                    os.killpg(p.pid, signal.SIGTERM)
                    try:
                        p.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(p.pid, signal.SIGKILL)
                    break
            except FileNotFoundError:
                pass
            time.sleep(2)
        code = p.wait()
    (OUT / f'{label}.status').write_text(str(code) + '\n')
    if code and not pressured:
        raise RuntimeError(f'{label} failed with {code}; inspect log before resuming')
    return not pressured


for target in (39_000_000, 99_000_000):
    for attempt, batch in enumerate((3_000_000, 1_000_000), 1):
        if count() >= target:
            break
        if run(target, batch, f'build-{target // 1_000_000}m-attempt-{attempt}'):
            break
    if count() != target:
        raise RuntimeError(f'Memory guard prevented reaching {target:,}; last saved count {count():,}')
    dest = ROOT / f'checkpoint-{target // 1_000_000}m'
    subprocess.run(['python3', str(OUT / 'checkpoints.py'), 'snapshot', str(LIVE), str(dest)], check=True)
    with (OUT / f'checkpoint-{target // 1_000_000}m-manifest.json').open('w') as f:
        subprocess.run(['python3', str(OUT / 'checkpoints.py'), 'manifest', str(dest)], stdout=f, check=True)
    print(f'Saved resumable {target:,} snapshot: {dest}', flush=True)
(OUT / 'phase.txt').write_text('39M and 99M snapshots saved; preparing ground truth and final 1M verification runs.\n')
