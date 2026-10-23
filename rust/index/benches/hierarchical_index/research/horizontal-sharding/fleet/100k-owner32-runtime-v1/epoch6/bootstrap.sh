#!/usr/bin/env bash
set -euo pipefail
readonly base=/home/ubuntu/spann-owner32-build-v6
readonly control=$base/epoch6/batched
readonly package=$base/epoch6/runtime-package.tar.gz
echo '94553c61f09c69326a839b8795fe0708e829b877f2b0f5cff3879e2ee3762109  /home/ubuntu/spann-owner32-build-v6/epoch6/runtime-package.tar.gz' | sha256sum -c -
mkdir "$base/bootstrap-epoch6"
tar -xzf "$package" -C "$base/bootstrap-epoch6" operator/extract_and_test.py
/usr/bin/python3 "$base/bootstrap-epoch6/operator/extract_and_test.py" --package "$package" --package-sha256 94553c61f09c69326a839b8795fe0708e829b877f2b0f5cff3879e2ee3762109 --package-bytes 2440193 --control "$control" --deadline 1792285385.176517
mkdir "$control/tmp"
cd "$control/operator"
/usr/bin/python3 -m unittest -v test_build_profile test_prefix_binding
sha256sum /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
