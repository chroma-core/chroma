#!/usr/bin/env bash
set -euo pipefail
readonly base=/home/ubuntu/spann-owner32-build-v6
sudo systemctl is-active --quiet spann-100k-owner32-run8-stop.timer
printf '%s\n' 'cf1a05c1b624241c50da032eca4f20c64d0adcd72f46efb84abb933bbf7a48bb  /home/ubuntu/spann-owner32-build-v6/run_queue.py' | sha256sum -c -
mkdir /home/ubuntu/spann-owner32-queue-cache-science-v1
exec /usr/bin/python3 "$base/run_queue.py" --control "$base/queue-cache4-epoch1" --host "$base/host-admission.json" --proof "$base/shutdown-proof.json" --output-root /home/ubuntu/spann-owner32-queue-cache-science-v1/epoch1 --deadline 1792285153.178632
