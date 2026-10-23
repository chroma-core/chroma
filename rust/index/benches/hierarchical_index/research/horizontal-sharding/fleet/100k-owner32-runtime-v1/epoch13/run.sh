#!/usr/bin/env bash
set -euo pipefail
readonly root=/home/ubuntu/spann-owner32-build-v6/cadence-epoch1
sudo systemctl is-active --quiet spann-100k-owner32-run13-stop.timer
printf '%s\n' '7d0dd543915701c466ded28b479cecda1fb2b0264b0aaff5d840e22b6263f3e7  /home/ubuntu/spann-owner32-build-v6/cadence-epoch1/build-control/operator/run_pair.py' | sha256sum -c -
exec /usr/bin/python3 "$root/build-control/operator/run_pair.py" --build-control "$root/build-control" --short-control "$root/short" --long-control "$root/long" --science-parent /home/ubuntu/spann-owner32-cadence-science-v1/epoch1 --proof "$root/shutdown-proof.json" --host "$root/host-admission.json" --deadline 1792293486.539584
