#!/usr/bin/env bash
set -euo pipefail
readonly base=/home/ubuntu/spann-owner32-build-v6
readonly control=$base/cadence-epoch1/build-control
readonly package=$base/cadence-epoch1/runtime-package.tar.gz
echo '50e2da7709b0033ea77329d19c960b7a20c8cc4e40a10848d406e916f70eb5be  /home/ubuntu/spann-owner32-build-v6/cadence-epoch1/runtime-package.tar.gz' | sha256sum -c -
mkdir "$base/bootstrap-epoch13"
tar -xzf "$package" -C "$base/bootstrap-epoch13" operator/extract_and_test.py
/usr/bin/python3 "$base/bootstrap-epoch13/operator/extract_and_test.py" --package "$package" --package-sha256 50e2da7709b0033ea77329d19c960b7a20c8cc4e40a10848d406e916f70eb5be --package-bytes 2514401 --control "$control" --deadline 1792293486.539584
mkdir "$control/tmp"
cd "$control/operator"
/usr/bin/python3 -m unittest -v test_build_profile test_prefix_binding
sha256sum /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
