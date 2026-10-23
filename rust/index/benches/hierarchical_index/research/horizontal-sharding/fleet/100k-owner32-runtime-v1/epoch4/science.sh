#!/usr/bin/env bash
set -euo pipefail
readonly control=/home/ubuntu/spann-owner32-build-v6/epoch4
sudo systemctl is-active --quiet spann-100k-owner32-run4-stop.timer
sudo systemctl show spann-100k-owner32-run4-stop.timer -p NextElapseUSecRealtime
cat "$control/build/result.json"
printf '%s\n' '{"armed":true,"deadline_unix":1792283379.263718,"instance_id":"i-011ff180f03584f40","timer":"spann-100k-owner32-run4-stop.timer","local_watchdog_pid":47102}' > "$control/shutdown-proof.json"
sync "$control/shutdown-proof.json"
TMPDIR="$control/tmp" exec /usr/bin/python3 "$control/operator/remote_stages.py" science --control "$control" --deadline 1792283379.263718 --output-root /mnt/spann-owner32-scratch/epoch4 --shutdown-proof "$control/shutdown-proof.json" --host-admission "$control/host-admission.json"
