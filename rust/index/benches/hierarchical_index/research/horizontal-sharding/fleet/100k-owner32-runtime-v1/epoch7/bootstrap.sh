#!/usr/bin/env bash
set -euo pipefail
readonly base=/home/ubuntu/spann-owner32-build-v6
readonly control=$base/epoch7/batched
readonly package=$base/epoch7/runtime-package.tar.gz
echo 'fb61ef00d76be70f85ca6cd1f0628572dd4736b056a24e27e168f159fa8d562b  /home/ubuntu/spann-owner32-build-v6/epoch7/runtime-package.tar.gz' | sha256sum -c -
mkdir "$base/bootstrap-epoch7"
tar -xzf "$package" -C "$base/bootstrap-epoch7" operator/extract_and_test.py
/usr/bin/python3 "$base/bootstrap-epoch7/operator/extract_and_test.py" --package "$package" --package-sha256 fb61ef00d76be70f85ca6cd1f0628572dd4736b056a24e27e168f159fa8d562b --package-bytes 2497383 --control "$control" --deadline 1792285949.8351161
mkdir "$control/tmp"
cd "$control/operator"
/usr/bin/python3 -m unittest -v test_build_profile test_prefix_binding
sha256sum /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
