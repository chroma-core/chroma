#!/usr/bin/env bash
set -euo pipefail
readonly serial=AWS2245560FC5D239A27
readonly device=$(lsblk -dnpo NAME,SERIAL | awk -v serial="$serial" '$2==serial {print $1}')
test -n "$device"
test "$(lsblk -dnro SERIAL "$device" | xargs)" = "$serial"
test "$(lsblk -dno MODEL "$device" | xargs)" = 'Amazon EC2 NVMe Instance Storage'
test "$(lsblk -npo NAME "$device" | wc -l | xargs)" = 1
test -z "$(lsblk -nro MOUNTPOINTS "$device" | xargs)"
test -z "$(sudo wipefs --no-act --noheadings --output TYPE "$device")"
if sudo blkid "$device"; then echo 'Unexpected signature; abort' >&2; exit 1; fi
sudo mkfs.ext4 -q -m 0 "$device"
sudo mkdir -p /mnt/spann-owner32-scratch
test -z "$(ls -A /mnt/spann-owner32-scratch)"
sudo mount "$device" /mnt/spann-owner32-scratch
sudo chown ubuntu:ubuntu /mnt/spann-owner32-scratch
test ! -e /home/ubuntu/spann-owner32-build-v6
mkdir /home/ubuntu/spann-owner32-build-v6
cp -a /home/ubuntu/reference3m/source/target /home/ubuntu/spann-owner32-build-v6/target
du -s -B1 /home/ubuntu/spann-owner32-build-v6/target
df -B1 /home/ubuntu/spann-owner32-build-v6
lsblk -o NAME,SERIAL,SIZE,FSTYPE,MOUNTPOINTS "$device"
df -B1 /mnt/spann-owner32-scratch
