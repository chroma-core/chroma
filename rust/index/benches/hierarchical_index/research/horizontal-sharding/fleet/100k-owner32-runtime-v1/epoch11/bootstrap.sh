#!/usr/bin/env bash
set -euo pipefail
readonly base=/home/ubuntu/spann-owner32-build-v6
readonly control=$base/decoded-cache-epoch1/build-control
readonly package=$base/decoded-cache-epoch1/runtime-package.tar.gz
echo '83ff4de5a6d95eb04bb4266027158b2a85e65dc77b5d1bf10a3c0cb50642f4ae  /home/ubuntu/spann-owner32-build-v6/decoded-cache-epoch1/runtime-package.tar.gz' | sha256sum -c -
mkdir "$base/bootstrap-epoch11"
tar -xzf "$package" -C "$base/bootstrap-epoch11" operator/extract_and_test.py
/usr/bin/python3 "$base/bootstrap-epoch11/operator/extract_and_test.py" --package "$package" --package-sha256 83ff4de5a6d95eb04bb4266027158b2a85e65dc77b5d1bf10a3c0cb50642f4ae --package-bytes 2505715 --control "$control" --deadline 1792290941.897804
mkdir "$control/tmp"
cd "$control/operator"
/usr/bin/python3 -m unittest -v test_build_profile test_prefix_binding
sha256sum /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
