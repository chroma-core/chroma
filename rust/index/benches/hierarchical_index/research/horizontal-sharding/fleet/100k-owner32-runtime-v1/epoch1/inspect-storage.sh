#!/usr/bin/env bash
set -euo pipefail
for device in /dev/nvme1n1 /dev/nvme2n1; do
    echo "$device"
    sudo wipefs --no-act "$device"
    sudo blkid "$device" || true
    lsblk -no NAME,SERIAL,MODEL,SIZE,FSTYPE,MOUNTPOINTS "$device"
done
find /home/ubuntu -maxdepth 4 -type d \( -name target -o -name 'pilot-venv' -o -name '.venv' \) -print
for path in /home/ubuntu/owner3m/target /home/ubuntu/reference3m/target /home/ubuntu/.cargo /home/ubuntu/.rustup; do
    if test -d "$path"; then du -s -B1 "$path"; fi
done
/home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc --version
command -v aws || true
command -v python3
