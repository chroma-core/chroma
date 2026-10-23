#!/usr/bin/env bash
set -euo pipefail
readonly base=/home/ubuntu/spann-owner32-build-v6
readonly control=$base/local-reassignment-epoch1/build-control
readonly package=$base/local-reassignment-epoch1/runtime-package.tar.gz
echo '9a06ff3c64b377a0f751526a4aa601473bbaf9026f639ca71e9721b2f09cae8c  /home/ubuntu/spann-owner32-build-v6/local-reassignment-epoch1/runtime-package.tar.gz' | sha256sum -c -
mkdir "$base/bootstrap-epoch12"
tar -xzf "$package" -C "$base/bootstrap-epoch12" operator/extract_and_test.py
/usr/bin/python3 "$base/bootstrap-epoch12/operator/extract_and_test.py" --package "$package" --package-sha256 9a06ff3c64b377a0f751526a4aa601473bbaf9026f639ca71e9721b2f09cae8c --package-bytes 2508509 --control "$control" --deadline 1792292136.032224
mkdir "$control/tmp"
cd "$control/operator"
/usr/bin/python3 -m unittest -v test_build_profile test_prefix_binding
sha256sum /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
