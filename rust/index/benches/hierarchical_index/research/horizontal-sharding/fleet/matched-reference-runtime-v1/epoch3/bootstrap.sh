#!/usr/bin/env bash
set -euo pipefail
base=/home/ubuntu/spann-matched-reference-v1/epoch1
sudo systemctl is-active --quiet spann-reference100k-run3-stop.timer
printf '%s\n' 'c368a1e489e6540418353985309cef73d2732aced79f7b3d35b8e3bebb1216f0  /home/ubuntu/spann-matched-reference-v1/epoch1/runtime-package.tar.gz' | sha256sum -c -
test "$(stat -c %s "$base/runtime-package.tar.gz")" = 3367696
mkdir "$base/bootstrap"
tar -xzf "$base/runtime-package.tar.gz" -C "$base/bootstrap"
/usr/bin/python3 "$base/bootstrap/extract_package.py" "$base/runtime-package.tar.gz" "$base/control"
mkdir "$base/control/tmp"
cd "$base/control"
/usr/bin/python3 -m unittest -v test_reference
sha256sum /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
du -sb /home/ubuntu/spann-owner32-build-v6/target
df -B1 /
test ! -e /home/ubuntu/spann-matched-reference-v1/target
cp -a /home/ubuntu/spann-owner32-build-v6/target /home/ubuntu/spann-matched-reference-v1/target
df -B1 /
