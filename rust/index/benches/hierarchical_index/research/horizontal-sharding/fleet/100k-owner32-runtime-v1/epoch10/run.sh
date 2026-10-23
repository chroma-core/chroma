#!/usr/bin/env bash
set -euo pipefail
readonly root=/home/ubuntu/spann-owner32-build-v6/local-scrub-epoch1
sudo systemctl is-active --quiet spann-100k-owner32-run10-stop.timer
printf '%s\n' '862a0bcef26a44293f60ca5f1b75bfc7c85df7eb83d25d7e32b7eefdc91b1186  /home/ubuntu/spann-owner32-build-v6/local-scrub-epoch1/build-control/operator/run_pair.py' | sha256sum -c -
exec /usr/bin/python3 "$root/build-control/operator/run_pair.py" --build-control "$root/build-control" --off-control "$root/off" --on-control "$root/on" --science-parent /home/ubuntu/spann-owner32-local-scrub-science-v1/epoch1 --proof "$root/shutdown-proof.json" --host "$root/host-admission.json" --deadline 1792289599.306695
