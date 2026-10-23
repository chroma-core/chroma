#!/usr/bin/env bash
set -euo pipefail
readonly package=/mnt/spann/full-record-million-runtime-v3/runtime-package.tar.gz
readonly control=/mnt/spann/full-record-million-runtime-v3/epoch7
test "$(stat -c %s "$package")" = 2414526
echo 'd9ae8aecd57e7319077575fa078dade17714b1ae2f967a8503ce0b9768df2374  /mnt/spann/full-record-million-runtime-v3/runtime-package.tar.gz' | sha256sum -c -
readonly bootstrap=/mnt/spann/full-record-million-runtime-v3/bootstrap-epoch7
mkdir "$bootstrap"
tar -xzf "$package" -C "$bootstrap" operator/extract_and_test.py
/mnt/spann/pilot-venv/bin/python "$bootstrap/operator/extract_and_test.py" --package "$package" --package-sha256 d9ae8aecd57e7319077575fa078dade17714b1ae2f967a8503ce0b9768df2374 --package-bytes 2414526 --control "$control" --deadline 1792278001.800991

echo '3a17348a9779a2d62f8279c730adbabee67833995aef5b3a0c8d5c09e939fc22  /mnt/spann/full-record-million-runtime-v3/operator-delta-v4.tar.gz' | sha256sum -c -
tar -xzf /mnt/spann/full-record-million-runtime-v3/operator-delta-v4.tar.gz -C "$control/operator"
echo 'cc6a1c1a733869bf945a50656fbfebef7a7d00932927f71779cc202593cf2f60  /mnt/spann/full-record-million-runtime-v3/epoch7/operator/remote_stages.py' | sha256sum -c -
cd "$control/operator"
/mnt/spann/pilot-venv/bin/python -m unittest -v test_build_profile
/mnt/spann/pilot-venv/bin/python remote_stages.py reuse-build --control "$control" --prior-control /mnt/spann/full-record-million-runtime-v3/epoch6 --deadline 1792278001.800991
cat "$control/build/reuse.json"
