#!/usr/bin/env bash
set -euo pipefail
readonly deadline="$1"
readonly unit=spann-fullrecord1m-v3-epoch6-stop
sudo systemd-run --unit="$unit" --on-calendar="@$deadline" --timer-property=AccuracySec=1s /sbin/shutdown -h now
sudo systemctl is-active "$unit.timer"
sudo systemctl show "$unit.timer" -p NextElapseUSecRealtime
# New guard is active before retiring the previous stopped epoch's retained timer.
sudo systemctl stop spann-fullrecord1m-v3-epoch5-stop.timer || true
readonly token=$(curl --noproxy '*' -fsS --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
curl --noproxy '*' -fsS --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document
lsblk -o NAME,SERIAL,SIZE,FSTYPE,MOUNTPOINTS
if ! mountpoint -q /mnt/spann-matched-2m-dynamic-v1; then
    readonly data_device=$(readlink -f /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol06c673c76a50340f8)
    test "$(sudo blkid -s TYPE -o value "$data_device")" = ext4
    sudo mount "$data_device" /mnt/spann-matched-2m-dynamic-v1
fi
df -B1 /mnt/spann /mnt/spann-matched-2m-dynamic-v1
free -b
pgrep -af 'hierarchical_spann_shard|supervise.py|cargo build|rustc' || true
mkdir -p /mnt/spann/full-record-million-runtime-v3
