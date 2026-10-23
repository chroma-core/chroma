#!/usr/bin/env bash
set -euo pipefail
# No generated Python body. These exact files must be installed and hash-verified.
readonly python=/mnt/spann/pilot-venv/bin/python
readonly scripts="$1"
shift
exec "$python" "$scripts/remote_stages.py" "$@"
