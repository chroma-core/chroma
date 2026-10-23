#!/usr/bin/env bash
set -euo pipefail
readonly control=/mnt/spann/full-record-million-runtime-v3/epoch7
sudo systemctl is-active --quiet spann-fullrecord1m-v3-epoch7-stop.timer
sudo systemctl show spann-fullrecord1m-v3-epoch7-stop.timer -p NextElapseUSecRealtime
cat "$control/build/result.json"
printf '%s\n' '{"armed":true,"deadline_unix":1792278001.800991,"instance_id":"i-00268d1e1dfea0357","timer":"spann-fullrecord1m-v3-epoch7-stop.timer","local_watchdog_pid":98447}' > "$control/shutdown-proof.json"
sync "$control/shutdown-proof.json"
exec /mnt/spann/pilot-venv/bin/python "$control/operator/remote_stages.py" science --control "$control" --deadline 1792278001.800991 --output-root /mnt/spann-matched-2m-dynamic-v1/full-record-million-runtime-v1/epoch7 --shutdown-proof "$control/shutdown-proof.json"
