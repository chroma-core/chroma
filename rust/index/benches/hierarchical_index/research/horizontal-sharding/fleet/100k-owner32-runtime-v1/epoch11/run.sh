#!/usr/bin/env bash
set -euo pipefail
readonly root=/home/ubuntu/spann-owner32-build-v6/decoded-cache-epoch1
sudo systemctl is-active --quiet spann-100k-owner32-run11-stop.timer
printf '%s\n' 'fff1e170e332162468221cb3b17a7355d4ef8299db2b36a1068a88c477960870  /home/ubuntu/spann-owner32-build-v6/decoded-cache-epoch1/build-control/operator/run_pair.py' | sha256sum -c -
exec /usr/bin/python3 "$root/build-control/operator/run_pair.py" --build-control "$root/build-control" --default-control "$root/default" --byte-control "$root/byte" --science-parent /home/ubuntu/spann-owner32-decoded-cache-science-v1/epoch1 --proof "$root/shutdown-proof.json" --host "$root/host-admission.json" --deadline 1792290941.897804
