#!/usr/bin/env bash
set -euo pipefail
readonly root=/home/ubuntu/spann-owner32-build-v6/local-reassignment-epoch1
sudo systemctl is-active --quiet spann-100k-owner32-run12-stop.timer
printf '%s\n' 'feb8b844f97d3cba3634e448c2b1111e28ff195f22a215b89b3953be789ad247  /home/ubuntu/spann-owner32-build-v6/local-reassignment-epoch1/build-control/operator/run_pair.py' | sha256sum -c -
exec /usr/bin/python3 "$root/build-control/operator/run_pair.py" --build-control "$root/build-control" --off-control "$root/off" --on-control "$root/on" --science-parent /home/ubuntu/spann-owner32-local-reassignment-science-v1/epoch1 --proof "$root/shutdown-proof.json" --host "$root/host-admission.json" --deadline 1792292136.032224
