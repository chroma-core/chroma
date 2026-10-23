#!/usr/bin/env bash
set -euo pipefail
readonly base=/home/ubuntu/spann-owner32-build-v6
readonly control=$base/epoch4
readonly package=/home/ubuntu/spann-owner32-runtime-v6-package.tar.gz
echo '2b41d2e427505b2cca50e6c3fdf6c5de76cc8aa8781bcf3a110a552c82022421  /home/ubuntu/spann-owner32-runtime-v6-package.tar.gz' | sha256sum -c -
mkdir "$base/bootstrap-epoch4"
tar -xzf "$package" -C "$base/bootstrap-epoch4" operator/extract_and_test.py
/usr/bin/python3 "$base/bootstrap-epoch4/operator/extract_and_test.py" --package "$package" --package-sha256 2b41d2e427505b2cca50e6c3fdf6c5de76cc8aa8781bcf3a110a552c82022421 --package-bytes 2438368 --control "$control" --deadline 1792283379.263718
mkdir "$control/tmp"
cd "$control/operator"
/usr/bin/python3 -m unittest -v test_build_profile test_prefix_binding
sha256sum /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
