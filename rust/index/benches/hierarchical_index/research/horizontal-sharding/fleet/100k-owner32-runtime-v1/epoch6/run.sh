#!/usr/bin/env bash
set -euo pipefail
readonly root=/home/ubuntu/spann-owner32-build-v6/epoch6
sudo systemctl is-active --quiet spann-100k-owner32-run6-stop.timer
printf '%s\n' 'e8517c0a5dea6cda1b56fc4b03fdff4af742770f01a666b78bb15ec07b36cd96  /home/ubuntu/spann-owner32-build-v6/epoch6/run_pair.py' | sha256sum -c -
exec /usr/bin/python3 "$root/run_pair.py" --batched "$root/batched" --baseline "$root/baseline" --science-parent /home/ubuntu/spann-owner32-paired-science-v1/epoch6 --proof "$root/shutdown-proof.json" --host "$root/host-admission.json" --deadline 1792285385.176517
