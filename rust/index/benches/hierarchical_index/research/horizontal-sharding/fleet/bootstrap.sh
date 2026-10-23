#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y build-essential clang cmake pkg-config libssl-dev protobuf-compiler libprotobuf-dev git curl unzip jq python3-venv python3-pip nvme-cli mdadm
mkdir -p /mnt/spann /var/lib/spann-results
# Only fresh instance-store disks are eligible; never select EBS or root.
mapfile -t disks < <(lsblk -dnpo NAME,MODEL | awk '/Amazon EC2 NVMe Instance Storage/ {print $1}')
if ((${#disks[@]})); then
 for d in "${disks[@]}"; do test -z "$(lsblk -nro MOUNTPOINT "$d" | tr -d '[:space:]')"; test -z "$(wipefs -n "$d")"; done
 if ((${#disks[@]}>1)); then mdadm --create /dev/md0 --level=0 --raid-devices="${#disks[@]}" "${disks[@]}" --run; scratch=/dev/md0; else scratch=${disks[0]}; fi
 mkfs.ext4 -F "$scratch"
 mount "$scratch" /mnt/spann
fi
chown ubuntu:ubuntu /mnt/spann /var/lib/spann-results
sudo -u ubuntu bash -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal'
cat >/usr/local/sbin/spann-preserve-stop <<'GUARD'
#!/bin/bash
set -euo pipefail
# Stop destroys local NVMe. Only stop after result preservation succeeds.
mkdir -p /var/lib/spann-results
if mountpoint -q /mnt/spann && test -d /mnt/spann/results; then
 available=$(df -B1 --output=avail /var/lib/spann-results | tail -1)
 needed=$(du -sb /mnt/spann/results | cut -f1)
 if ((available < needed + 1073741824)); then logger 'SPANN guard: insufficient EBS space to preserve results; shutdown cancelled'; exit 1; fi
 cp -a /mnt/spann/results /var/lib/spann-results/
fi
sync
shutdown -h now
GUARD
chmod +x /usr/local/sbin/spann-preserve-stop
systemd-run --unit=spann-preserve-stop --on-active=12h /usr/local/sbin/spann-preserve-stop
date -u +%FT%TZ > /var/lib/spann-results/bootstrap-ready
