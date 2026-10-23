#!/usr/bin/env bash
set -euo pipefail
readonly package=/mnt/spann/full-record-million-runtime-v3/runtime-package.tar.gz
readonly control=/mnt/spann/full-record-million-runtime-v3/epoch5
test "$(stat -c %s "$package")" = 2414526
echo 'd9ae8aecd57e7319077575fa078dade17714b1ae2f967a8503ce0b9768df2374  /mnt/spann/full-record-million-runtime-v3/runtime-package.tar.gz' | sha256sum -c -
readonly bootstrap=/mnt/spann/full-record-million-runtime-v3/bootstrap-epoch5
mkdir "$bootstrap"
tar -xzf "$package" -C "$bootstrap" operator/extract_and_test.py
exec /mnt/spann/pilot-venv/bin/python "$bootstrap/operator/extract_and_test.py" --package "$package" --package-sha256 d9ae8aecd57e7319077575fa078dade17714b1ae2f967a8503ce0b9768df2374 --package-bytes 2414526 --control "$control" --deadline 1792276486.33988
