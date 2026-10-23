#!/usr/bin/env bash
set -euo pipefail
# No script installs dependencies or deletes retained files.
# Use one lock across build, validation and both reversed timing pairs.
exec flock -n /home/ubuntu/spann-benchmark.lock python3 "$(dirname "$0")/remote-run.py" "$@"
