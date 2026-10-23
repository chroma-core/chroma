#!/usr/bin/env bash
set -euo pipefail
readonly serial=AWS22E0594BBE22808D1
readonly device=$(lsblk -dnpo NAME,SERIAL | awk -v serial="$serial" '$2==serial {print $1}')
test -n "$device"
test "$(lsblk -dnro SERIAL "$device")" = "$serial"
test "$(lsblk -dnro MODEL "$device" | xargs)" = 'Amazon EC2 NVMe Instance Storage'
test "$(lsblk -npo NAME "$device" | wc -l | xargs)" = 1
test -z "$(lsblk -nro MOUNTPOINTS "$device" | xargs)"
test -z "$(sudo wipefs --no-act --noheadings --output TYPE "$device")"
if sudo blkid "$device"; then echo 'Unexpected signature; abort' >&2; exit 1; fi
sudo mkfs.ext4 -q -m 0 "$device"
sudo mkdir /mnt/spann-owner32-scratch
sudo mount "$device" /mnt/spann-owner32-scratch
sudo chown ubuntu:ubuntu /mnt/spann-owner32-scratch
mkdir /mnt/spann-owner32-scratch/retained-target-copy
cp -a /home/ubuntu/reference3m/source/target/. /mnt/spann-owner32-scratch/retained-target-copy/
lsblk -o NAME,SERIAL,SIZE,FSTYPE,MOUNTPOINTS "$device"
df -B1 /mnt/spann-owner32-scratch
