#!/usr/bin/env bash
set -euo pipefail
readonly base=/home/ubuntu/spann-owner32-build-v6
readonly control=$base/local-scrub-epoch1/build-control
readonly package=$base/local-scrub-epoch1/runtime-package.tar.gz
echo 'd9af5416eea3614b4edcc2995f7c0d82adcdc138b86fb7051e976c8e0e0b94aa  /home/ubuntu/spann-owner32-build-v6/local-scrub-epoch1/runtime-package.tar.gz' | sha256sum -c -
mkdir "$base/bootstrap-epoch10"
tar -xzf "$package" -C "$base/bootstrap-epoch10" operator/extract_and_test.py
/usr/bin/python3 "$base/bootstrap-epoch10/operator/extract_and_test.py" --package "$package" --package-sha256 d9af5416eea3614b4edcc2995f7c0d82adcdc138b86fb7051e976c8e0e0b94aa --package-bytes 2502887 --control "$control" --deadline 1792289599.306695
mkdir "$control/tmp"
cd "$control/operator"
/usr/bin/python3 -m unittest -v test_build_profile test_prefix_binding
sha256sum /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
