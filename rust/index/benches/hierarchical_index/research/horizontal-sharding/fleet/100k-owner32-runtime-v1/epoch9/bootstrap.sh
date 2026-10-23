#!/usr/bin/env bash
set -euo pipefail
readonly base=/home/ubuntu/spann-owner32-build-v6
readonly control=$base/eligibility-prefix-epoch1/build-control
readonly package=$base/eligibility-prefix-epoch1/runtime-package.tar.gz
echo '6726f9d2775dac03a9ce48dc65cc1b6268af90c664efa13d46608fb398e6e1a8  /home/ubuntu/spann-owner32-build-v6/eligibility-prefix-epoch1/runtime-package.tar.gz' | sha256sum -c -
mkdir "$base/bootstrap-epoch9"
tar -xzf "$package" -C "$base/bootstrap-epoch9" operator/extract_and_test.py
/usr/bin/python3 "$base/bootstrap-epoch9/operator/extract_and_test.py" --package "$package" --package-sha256 6726f9d2775dac03a9ce48dc65cc1b6268af90c664efa13d46608fb398e6e1a8 --package-bytes 2501331 --control "$control" --deadline 1792287469.743366
mkdir "$control/tmp"
cd "$control/operator"
/usr/bin/python3 -m unittest -v test_build_profile test_prefix_binding
sha256sum /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
