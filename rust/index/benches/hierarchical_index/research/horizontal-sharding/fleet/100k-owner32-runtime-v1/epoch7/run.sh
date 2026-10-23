#!/usr/bin/env bash
set -euo pipefail
readonly root=/home/ubuntu/spann-owner32-build-v6/epoch7
sudo systemctl is-active --quiet spann-100k-owner32-run7-stop.timer
printf '%s\n' 'c2f2560b8d09507d93240bce429f3d39feb840f082291aa5fea4dfb48d4173a3  /home/ubuntu/spann-owner32-build-v6/epoch7/batched/operator/run_pair.py' | sha256sum -c -
exec /usr/bin/python3 "$root/batched/operator/run_pair.py" --batched "$root/batched" --baseline "$root/baseline" --science-parent /home/ubuntu/spann-owner32-paired-science-v1/epoch7 --proof "$root/shutdown-proof.json" --host "$root/host-admission.json" --deadline 1792285949.8351161
