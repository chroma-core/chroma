#!/usr/bin/env python3
"""Run one fixed native transfer test under an absolute 30-second process bound."""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument('--attempt', required=True)
parser.add_argument('--build-receipt', required=True)
args = parser.parse_args()
root = Path(__file__).resolve().parent
prefix = root / ('fixed-transfer-run-' + args.attempt)
log = prefix.with_suffix('.log')
result = prefix.with_suffix('.json')
run = root / ('fixed-transfer-run-' + args.attempt + '-supervisor.json')
for target in (log, result, run):
    if target.exists():
        raise SystemExit('preserve prior artifact: ' + str(target))
artifacts = [json.loads(line) for line in Path(args.build_receipt).read_text().splitlines() if line.strip().startswith('{')]
executable = next(item['executable'] for item in artifacts if item.get('reason') == 'compiler-artifact' and item.get('target', {}).get('name') == 'hierarchical_distributed' and item.get('executable'))
command = [executable, 'hierarchical_index::distributed::local_transfer::tests::fixed_transfer_eight_remote_groups_isolates_protocol_from_local_bulk', '--exact', '--nocapture', '--test-threads=1']
env = os.environ.copy()
env['SPANN_FIXED_TRANSFER_DIAGNOSTIC_OUTPUT'] = str(result)
started = time.monotonic()
receipt = {'command': command, 'absolute_timeout_seconds': 30, 'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
try:
    completed = subprocess.run(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
    log.write_bytes(completed.stdout)
    receipt.update(exit_code=completed.returncode, reason='exited')
except subprocess.TimeoutExpired as error:
    log.write_bytes(error.stdout or b'')
    receipt.update(exit_code=None, reason='absolute30s timeout; subprocess killed and waited')
receipt['elapsed_seconds'] = time.monotonic() - started
run.write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
raise SystemExit(0 if receipt['exit_code'] == 0 else 1)
