#!/usr/bin/env bash
set -euo pipefail
readonly keep=/home/ubuntu/spann-fineweb100k-retained-v1
mkdir "$keep"
cp /mnt/spann-owner32-scratch/input/fineweb100k.f32 "$keep/fineweb100k.f32"
cp /mnt/spann-owner32-scratch/input/fineweb100k.receipt.json "$keep/source-receipt.json"
echo '5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c  /home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32' | sha256sum -c -
test "$(stat -c %s "$keep/fineweb100k.f32")" = 307200000
readonly free=$(df -B1 --output=avail "$keep" | tail -1 | xargs)
test "$free" -ge 21474836480
cat "$keep/source-receipt.json"
printf '\nretained_root_free_bytes=%s\n' "$free"
sha256sum /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc
