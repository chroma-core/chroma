"""Verify the exact final increments without changing either saved source."""
import json
from pathlib import Path
import subprocess
import sys

from checkpoints import manifest

ROOT = Path('/mnt/msmarco')
OUT = ROOT / 'results'
BASE = ['--dataset', 'ms-marco', '--metric', 'l2', '--branching-factor', '100', '--split-threshold', '2048', '--merge-threshold', '512', '--threads', '64', '--balance-threads', '64', '--write-navigation', 'fp', '--fp-npa', '--write-beam-tau', '1.5', '--write-beam-min', '10', '--write-beam-max', '16', '--max-replicas', '1', '--write-rng-epsilon', '0', '--write-rng-factor', '4', '--max-cache-bytes', '1073741824', '--compute-gt-clusters', 'false', '--num-queries', '100', '--recall-tau-values', '1.5,2', '--recall-rerank-vectors', '1,8', '--clear-reader-block-pins', 'true', '--save', '--resume', '--checkpoint-size', '1000000']

for size in (39, 99):
    source = ROOT / f'checkpoint-{size}m'
    before = json.loads((OUT / f'checkpoint-{size}m-manifest.json').read_text())
    assert manifest(source) == before
    dest = ROOT / f'verified-{size + 1}m'
    if dest.exists():
        raise FileExistsError(dest)
    subprocess.run(['cp', '-a', '--reflink=auto', str(source), str(dest)], check=True)
    a = next((source / 'data/block').iterdir())
    b = dest / 'data/block' / a.name
    assert (a.stat().st_dev, a.stat().st_ino) != (b.stat().st_dev, b.stat().st_ino)
    args = [str(OUT / 'binary'), *BASE, '--checkpoint', str(size + 1), '--save-dir', str(dest)]
    (OUT / f'verify-{size + 1}m.command.json').write_text(json.dumps(args, indent=2) + '\n')
    print(f'Testing {size}M -> {size + 1}M, source {source}', flush=True)
    with (OUT / f'verify-{size + 1}m.log').open('w') as log:
        subprocess.run(args, stdout=log, stderr=subprocess.STDOUT, check=True)
    meta = json.loads((dest / 'checkpoint.json').read_text())
    assert meta['total_vectors'] == (size + 1) * 1_000_000
    assert meta['checkpoint_idx'] == size
    assert manifest(source) == before
    (OUT / f'verify-{size + 1}m.status').write_text('0\n')
    print(f'Verified saved count {meta["total_vectors"]:,}; source manifest unchanged.', flush=True)
