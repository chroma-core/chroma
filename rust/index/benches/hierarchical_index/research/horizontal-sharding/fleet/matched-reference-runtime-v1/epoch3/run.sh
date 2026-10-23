#!/usr/bin/env bash
set -euo pipefail
base=/home/ubuntu/spann-matched-reference-v1/epoch1
control=$base/control
sudo systemctl is-active --quiet spann-reference100k-run3-stop.timer
args=(--control "$control" --target /home/ubuntu/spann-matched-reference-v1/target --output /home/ubuntu/spann-matched-reference-science-v1/epoch1/index --host "$base/host-admission.json" --proof "$base/shutdown-proof.json" --deadline 1792296155.6913762)
/usr/bin/python3 "$control/run_reference.py" build "${args[@]}"
mkdir -p /home/ubuntu/spann-matched-reference-science-v1/epoch1
exec /usr/bin/python3 "$control/run_reference.py" science "${args[@]}"
