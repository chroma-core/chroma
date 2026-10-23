#!/usr/bin/env bash
set -euo pipefail
readonly scratch=/mnt/spann-owner32-scratch
readonly control=$scratch/runtime
cd "$scratch"
echo '681dd5055e5995b9fce83c0002f8d39d3b01d19324b103b5ed3a0cbb909d02f4  runtime-package.tar.gz' | sha256sum -c -
echo 'a3b22d96ce6300d51edccbbe552d15f19eb91fbd277b5b772c884dfd3b59533b  binding-delta.tar.gz' | sha256sum -c -
mkdir bootstrap
tar -xzf binding-delta.tar.gz -C bootstrap operator/extract_and_test.py
/usr/bin/python3 bootstrap/operator/extract_and_test.py --package "$scratch/runtime-package.tar.gz" --package-sha256 681dd5055e5995b9fce83c0002f8d39d3b01d19324b103b5ed3a0cbb909d02f4 --package-bytes 2436724 --control "$control" --deadline 1792282112.817069
tar -xzf binding-delta.tar.gz -C "$control"
mkdir "$scratch/tmp"
cd "$control"
/usr/bin/python3 -m unittest -v test_supervisor test_monitor test_host_policy
cd operator
/usr/bin/python3 -m unittest -v test_build_profile test_prefix_binding
sha256sum /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
