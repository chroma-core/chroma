#!/usr/bin/env bash
set -euo pipefail
readonly scratch=/mnt/spann-owner32-scratch
readonly control=$scratch/runtime
cd "$scratch"
echo '0268503aa26d73dc554a4247c1a14263fcca5c1c9019a54d92a5a36aac3734ef  runtime-package.tar.gz' | sha256sum -c -
mkdir bootstrap
tar -xzf runtime-package.tar.gz -C bootstrap operator/extract_and_test.py
/usr/bin/python3 bootstrap/operator/extract_and_test.py --package "$scratch/runtime-package.tar.gz" --package-sha256 0268503aa26d73dc554a4247c1a14263fcca5c1c9019a54d92a5a36aac3734ef --package-bytes 2438195 --control "$control" --deadline 1792282393.855556
mkdir "$scratch/tmp"
cd "$control/operator"
/usr/bin/python3 -m unittest -v test_build_profile test_prefix_binding
sha256sum /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
