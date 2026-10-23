#!/usr/bin/env bash
set -euo pipefail
readonly package=/mnt/spann/full-record-million-runtime-v3/runtime-package.tar.gz
readonly control=/mnt/spann/full-record-million-runtime-v3/epoch6
test "$(stat -c %s "$package")" = 2414526
echo 'd9ae8aecd57e7319077575fa078dade17714b1ae2f967a8503ce0b9768df2374  /mnt/spann/full-record-million-runtime-v3/runtime-package.tar.gz' | sha256sum -c -
readonly bootstrap=/mnt/spann/full-record-million-runtime-v3/bootstrap-epoch6
mkdir "$bootstrap"
tar -xzf "$package" -C "$bootstrap" operator/extract_and_test.py
/mnt/spann/pilot-venv/bin/python "$bootstrap/operator/extract_and_test.py" --package "$package" --package-sha256 d9ae8aecd57e7319077575fa078dade17714b1ae2f967a8503ce0b9768df2374 --package-bytes 2414526 --control "$control" --deadline 1792277391.55881

echo '1401f440a32013f4b62ba14609a1a98905d5dc598b4277ae2c361bb16e71ba67  /mnt/spann/full-record-million-runtime-v3/remote-stages-profile-v3.py' | sha256sum -c -
cp /mnt/spann/full-record-million-runtime-v3/remote-stages-profile-v3.py "$control/operator/remote_stages.py"
cp /mnt/spann/full-record-million-runtime-v3/test-build-profile-v3.py "$control/operator/test_build_profile.py"
cd "$control/operator"
/mnt/spann/pilot-venv/bin/python -m unittest -v test_build_profile
