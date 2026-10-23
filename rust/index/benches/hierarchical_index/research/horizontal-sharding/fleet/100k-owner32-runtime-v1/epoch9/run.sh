#!/usr/bin/env bash
set -euo pipefail
readonly root=/home/ubuntu/spann-owner32-build-v6/eligibility-prefix-epoch1
sudo systemctl is-active --quiet spann-100k-owner32-run9-stop.timer
printf '%s\n' '2696b19003337d27976758c01f2f0814822b09ee874393c2e693754af031381d  /home/ubuntu/spann-owner32-build-v6/eligibility-prefix-epoch1/build-control/operator/run_pair.py' | sha256sum -c -
exec /usr/bin/python3 "$root/build-control/operator/run_pair.py" --build-control "$root/build-control" --off-control "$root/off" --on-control "$root/on" --science-parent /home/ubuntu/spann-owner32-eligibility-science-v1/epoch1 --proof "$root/shutdown-proof.json" --host "$root/host-admission.json" --deadline 1792287469.743366
