#!/usr/bin/env bash
set -euo pipefail
exec flock -n /home/ubuntu/spann-benchmark.lock python3 "$(dirname "$0")/remote-run.py" "$@"
