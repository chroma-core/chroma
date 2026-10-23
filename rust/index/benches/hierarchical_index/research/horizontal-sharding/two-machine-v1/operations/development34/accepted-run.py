"""Run a named final-million arm with the recorded accepted configuration."""
import json
import os
from pathlib import Path
import subprocess
import sys

p = Path(__file__).resolve().parent
profile = json.loads((p / 'accepted-profile.json').read_text())
if len(sys.argv) < 2:
    raise SystemExit('Usage: accepted-run.py UNIQUE-NAME [save] [detailed] [profile]')
env = dict(os.environ, **profile['env'])
raise SystemExit(subprocess.run(
    [sys.executable, str(p / 'run.py'), sys.argv[1], profile['writer_kind'], *sys.argv[2:]],
    env=env,
).returncode)
